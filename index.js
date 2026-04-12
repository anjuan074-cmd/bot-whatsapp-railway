/**
 * BACKEND WHATSAPP VÍA QR (Baileys) — Railway Deploy
 * ====================================================
 * Corre como servicio persistente en Railway.
 * La sesión de WhatsApp se guarda en Firestore (wa_sessions/).
 * Solo necesitas escanear el QR una vez en /qr
 *
 * VARIABLES DE ENTORNO en Railway:
 *   FIREBASE_SERVICE_ACCOUNT_JSON  — contenido completo del serviceAccountKey.json
 *   FIREBASE_STORAGE_BUCKET        — ej: userpruebb.firebasestorage.app
 *   GEMINI_API_KEY
 *   GEMINI_MODEL                   — ej: gemini-2.5-flash
 *   NOMBRE_EMPRESA
 *   CUENTA_NEQUI
 *   ADMIN_PHONES
 *   TECHNICAL_PHONE
 *   PHONE_ID
 *   SESSION_ID                     — default "default"
 *   ADMIN_SECRET                   — opcional, protege endpoints
 *   PORT                           — Railway lo inyecta automáticamente
 */

"use strict";
require("dotenv").config();

const express   = require("express");
const axios     = require("axios");
const crypto    = require("crypto");
const QRCode    = require("qrcode");
const admin     = require("firebase-admin");
const { GoogleGenerativeAI } = require("@google/generative-ai");

const {
    default: makeWASocket,
    initAuthCreds,
    BufferJSON,
    DisconnectReason,
    downloadMediaMessage,
    fetchLatestBaileysVersion,
    proto,
} = require("@whiskeysockets/baileys");

// ── Firebase Admin ─────────────────────────────────────────────────────────────
// En Railway las credenciales llegan como variable de entorno (JSON string)
// para no subir el archivo serviceAccountKey.json al repo.
const fbConfig = {};
if (process.env.FIREBASE_STORAGE_BUCKET) {
    fbConfig.storageBucket = process.env.FIREBASE_STORAGE_BUCKET;
}

if (process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount), ...fbConfig });
} else if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    // Fallback local: ruta al archivo JSON
    const path = require("path");
    const serviceAccount = require(path.resolve(process.env.FIREBASE_SERVICE_ACCOUNT));
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount), ...fbConfig });
} else {
    // Dentro de GCP usa credenciales automáticas
    admin.initializeApp(fbConfig);
}

const db = admin.firestore();

// ── Gemini ─────────────────────────────────────────────────────────────────────
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// ── Config ─────────────────────────────────────────────────────────────────────
const CONFIG = {
    PORT:                   process.env.PORT || 3001,
    SESSION_ID:             process.env.SESSION_ID || "default",
    ADMIN_SECRET:           process.env.ADMIN_SECRET || null,
    GEMINI_MODEL:           process.env.GEMINI_MODEL || "gemini-2.5-flash",
    WOMPI_INTEGRITY_SECRET: process.env.WOMPI_INTEGRITY_SECRET,
    WOMPI_PRIVATE_KEY:      process.env.WOMPI_PRIVATE_KEY,
    UMBRAL_CONFIANZA:       55,
};

// ==========================================
// SESIÓN WHATSAPP EN FIRESTORE
// ==========================================
async function useFirestoreAuthState(sessionId) {
    const sessionRef = db.collection("wa_sessions").doc(sessionId);

    const readKey = async (docId) => {
        const snap = await sessionRef.collection("keys").doc(docId).get();
        return snap.exists ? JSON.parse(snap.data().value, BufferJSON.reviver) : null;
    };
    const writeKey = async (docId, data) => {
        await sessionRef.collection("keys").doc(docId).set(
            { value: JSON.stringify(data, BufferJSON.replacer) }
        );
    };
    const deleteKey = async (docId) => {
        await sessionRef.collection("keys").doc(docId).delete();
    };

    const credsSnap = await sessionRef.get();
    const creds = credsSnap.exists
        ? JSON.parse(credsSnap.data().creds, BufferJSON.reviver)
        : initAuthCreds();

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const result = {};
                    await Promise.all(ids.map(async (id) => {
                        let val = await readKey(`${type}-${id}`);
                        if (type === "app-state-sync-key" && val) {
                            val = proto.Message.AppStateSyncKeyData.fromObject(val);
                        }
                        result[id] = val;
                    }));
                    return result;
                },
                set: async (data) => {
                    const tasks = [];
                    for (const [category, items] of Object.entries(data)) {
                        for (const [id, value] of Object.entries(items)) {
                            const docId = `${category}-${id}`;
                            tasks.push(value ? writeKey(docId, value) : deleteKey(docId));
                        }
                    }
                    await Promise.all(tasks);
                },
            },
        },
        saveCreds: async () => {
            await sessionRef.set(
                { creds: JSON.stringify(creds, BufferJSON.replacer) },
                { merge: true }
            );
        },
    };
}

async function limpiarSesionFirestore(sessionId) {
    const sessionRef = db.collection("wa_sessions").doc(sessionId);
    const keysSnap   = await sessionRef.collection("keys").get();
    const batch      = db.batch();
    keysSnap.forEach((doc) => batch.delete(doc.ref));
    batch.delete(sessionRef);
    await batch.commit();
    console.log("🗑️  Sesión Firestore eliminada:", sessionId);
}

// ==========================================
// ESTADO GLOBAL DEL BOT
// ==========================================
let sock         = null;
let currentQR    = null;
let isConnected  = false;

function toJID(phone) {
    let clean = String(phone).replace(/\D/g, "");
    if (clean.length === 10) clean = "57" + clean;
    return `${clean}@s.whatsapp.net`;
}

// Siempre devuelve 12 dígitos con código de país (ej: 573001234567).
// Usado como ID de documento en chats/ para evitar duplicados por formato.
function normalizePhone(phone) {
    const clean = String(phone).replace(/\D/g, "");
    if (clean.length === 10 && clean.startsWith("3")) return "57" + clean;
    if (clean.length === 13 && clean.startsWith("057"))  return clean.slice(1);
    return clean; // ya está en formato correcto (12 dígitos) o desconocido
}

// ==========================================
// INICIALIZACIÓN Y RECONEXIÓN DE WHATSAPP
// ==========================================
async function iniciarWhatsApp() {
    const { state, saveCreds } = await useFirestoreAuthState(CONFIG.SESSION_ID);
    const { version }          = await fetchLatestBaileysVersion();

    sock = makeWASocket({
        version,
        auth:              state,
        browser:           ["ISP Bot", "Chrome", "1.0.0"],
        syncFullHistory:   false,
        markOnlineOnConnect: false,
        generateHighQualityLinkPreview: false,
    });

    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            currentQR   = await QRCode.toDataURL(qr);
            isConnected = false;
            console.log("📱 Nuevo QR generado — visita /qr para escanearlo");
        }

        if (connection === "open") {
            currentQR   = null;
            isConnected = true;
            console.log("✅ WhatsApp conectado:", sock.user?.id);
        }

        if (connection === "close") {
            isConnected = false;
            const statusCode      = lastDisconnect?.error?.output?.statusCode;
            const isLoggedOut     = statusCode === DisconnectReason.loggedOut;
            // 440 = connectionReplaced: otro container tomó la sesión (redeploy Railway).
            // Esperamos más tiempo para que el nuevo container termine de iniciar.
            const isReplaced      = statusCode === 440;

            console.warn(`⚠️  Conexión cerrada (código ${statusCode}).`);

            if (isLoggedOut) {
                console.error("🚫 Sesión cerrada por WhatsApp. Borrando sesión de Firestore...");
                await limpiarSesionFirestore(CONFIG.SESSION_ID);
                setTimeout(iniciarWhatsApp, 3000);
            } else if (isReplaced) {
                // Otro proceso ya tomó la sesión — no reconectar de inmediato para evitar loop
                console.warn("🔄 Sesión reemplazada por otro proceso. Reconectando en 15s...");
                setTimeout(iniciarWhatsApp, 15000);
            } else {
                // Error de red u otro — reconexión normal
                setTimeout(iniciarWhatsApp, 5000);
            }
        }
    });


    sock.ev.on("messages.upsert", async ({ messages, type }) => {
        if (type !== "notify") return;
        for (const message of messages) {
            if (message.key.fromMe) continue;
            try {
                await procesarMensajeEntrante(message);
            } catch (err) {
                console.error("Error procesando mensaje:", err);
            }
        }
    });

    sock.ev.on("message-receipt.update", async (updates) => {
        for (const update of updates) {
            const remoteJid = update.key.remoteJid || "";
            let raw = remoteJid.split("@")[0].split(":")[0];
            // Resolver @lid igual que en procesarMensajeEntrante
            if (remoteJid.endsWith("@lid")) {
                const alt = update.key.remoteJidAlt;
                if (!alt) continue;
                raw = alt.split("@")[0].split(":")[0];
            }
            const jid = normalizePhone(raw);
            const ack = update.receipt.readTimestamp ? 3 : update.receipt.receiptTimestamp ? 2 : 1;
            if (!jid) continue;
            console.log(`[ACK] ${jid} msgId=${update.key.id} ack=${ack}`);
            try {
                const snap = await db.collection("chats").doc(jid)
                    .collection("messages").where("waMessageId", "==", update.key.id).limit(1).get();
                if (!snap.empty) await snap.docs[0].ref.update({ ack });
            } catch { /* no crítico */ }
        }
    });
}

// ==========================================
// PROCESADOR DE MENSAJES ENTRANTES
// ==========================================
async function procesarMensajeEntrante(message) {
    const jid = message.key.remoteJid;
    // Ignorar grupos; resolver @lid (Linked Device ID de WhatsApp multi-device) al teléfono real
    if (!jid || jid.endsWith("@g.us")) return;

    // Baileys multi-device puede dar JIDs como "573001234567:0@s.whatsapp.net" o "24820850884661@lid"
    let rawPhone = jid.split("@")[0].split(":")[0];

    if (jid.endsWith("@lid")) {
        // Baileys expone el JID real en message.key.remoteJidAlt (ej: "573238634992@s.whatsapp.net")
        const altJid = message.key.remoteJidAlt;
        if (!altJid) {
            console.warn(`[LID] Sin remoteJidAlt para ${jid} — ignorando`);
            return;
        }
        rawPhone = altJid.split("@")[0].split(":")[0];
        console.log(`[LID] ${jid} → ${rawPhone}`);
    }

    const userPhone = normalizePhone(rawPhone);
    console.log(`[MSG] JID=${jid}  raw=${rawPhone}  normalizado=${userPhone}`);
    const userName  = message.pushName || "Usuario";
    const msgType   = Object.keys(message.message || {})[0];

    // Deduplicación
    const msgRef = db.collection("processed_msgs").doc(message.key.id);
    if ((await msgRef.get()).exists) return;
    await msgRef.set({ ts: admin.firestore.FieldValue.serverTimestamp() });

    // Guardamos el mensaje y leemos todos los datos en paralelo — sin esperar uno por uno
    const [, excusaSnap, cliente, chatDocSnap, configSnap] = await Promise.all([
        guardarMensajeChat(userPhone, message, "in", userName),
        db.collection("esperando_excusa").doc(userPhone).get(),
        obtenerClientePorTelefono(userPhone),
        db.collection("chats").doc(userPhone).get(),
        db.collection("settings").doc("bot_config").get(),
    ]);

    // ¿Técnico respondiendo una excusa pendiente?
    if (excusaSnap.exists && msgType === "conversation") {
        const dataExcusa = excusaSnap.data();
        const motivo     = message.message.conversation;
        const instRef    = db.collection("installations").doc(dataExcusa.instalacionId);
        const instSnap   = await instRef.get();

        if (instSnap.exists) {
            const fechaHora = new Date().toLocaleString("es-CO", {
                timeZone: "America/Bogota", dateStyle: "short", timeStyle: "short",
            });
            const instData  = instSnap.data();
            const nuevaNota = instData.notes
                ? `${instData.notes}\n\n🤖 [${fechaHora}] EL BOT REPORTA (Excusa de ${dataExcusa.tecnicoNombre}): "${motivo}"`
                : `🤖 [${fechaHora}] EL BOT REPORTA (Excusa de ${dataExcusa.tecnicoNombre}): "${motivo}"`;
            await instRef.update({ notes: nuevaNota });
        }

        await excusaSnap.ref.delete();
        await enviarTexto(userPhone, `✅ He registrado tu justificación en la orden de *${dataExcusa.clienteNombre}*. ¡Gracias!`);
        return;
    }

    const isGlobalPause = configSnap.exists && configSnap.data().globalBotPaused === true;
    const isHumanMode   = (chatDocSnap.exists && chatDocSnap.data().humanMode === true) || isGlobalPause;

    if (isGlobalPause && !(chatDocSnap.exists && chatDocSnap.data().humanMode)) {
        await db.collection("chats").doc(userPhone).set({ humanMode: true }, { merge: true });
    }

    if (!isHumanMode) {
        if (!cliente) {
            await manejarUsuarioDesconocido(userPhone, userName, message);
        } else {
            await manejarClienteRegistrado(cliente, message);
        }
    }
}

// ==========================================
// LÓGICA DE NEGOCIO
// ==========================================
async function manejarClienteRegistrado(cliente, message) {
    const msgType = Object.keys(message.message || {})[0];

    if (["imageMessage", "image"].includes(msgType)) {
        await enviarTexto(cliente.telefono, "⏳ Procesando tu comprobante...");
        await procesarPago(message, cliente);
        return;
    }

    const texto = (
        message.message?.conversation ||
        message.message?.extendedTextMessage?.text ||
        ""
    ).toLowerCase().trim();

    if (!texto) return;

    if (/(asesor|humano|persona|alguien real|hablar con alguien|ayuda personal)/.test(texto)) {
        await db.collection("chats").doc(cliente.telefono).set({
            humanMode: true, unreadCount: admin.firestore.FieldValue.increment(1), sentiment: "urgente",
        }, { merge: true });
        await enviarTexto(cliente.telefono, `⏳ *Solicitud Recibida*\n\nHe pausado el asistente virtual. Un asesor humano revisará tu caso.`);
        return;
    }

    if (/(saldo|deuda|debo|factura|pagar|precio|cuanto)/.test(texto)) {
        const { totalDebt, monthsStr } = calcularDeudaCliente(cliente);
        if (totalDebt > 0) {
            const fmt = new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(totalDebt);
            await enviarTexto(cliente.telefono, `🧐 Hola ${cliente.nombre}.\n\nTu saldo pendiente es de: *${fmt}*.\nCorrespondiente a: ${monthsStr}.\n\n👉 Envía la foto del comprobante aquí.`);
        } else {
            await enviarTexto(cliente.telefono, `✅ ¡Estás al día, ${cliente.nombre}! No tienes deudas pendientes.`);
        }
        return;
    }

    if (/(falla|lento|sin internet|no sirve|daño|intermitente|rojo|los led|no carga)/.test(texto)) {
        await crearTicket(cliente, "soporte", texto);
        return;
    }

    if (/(queja|reclamo|pesimo|malo|grosero|demora|denuncia)/.test(texto)) {
        await crearTicket(cliente, "pqr", texto);
        return;
    }

    try {
        const model = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
        const { totalDebt, monthsStr } = calcularDeudaCliente(cliente);
        const deudaInfo = totalDebt > 0
            ? `Tiene deuda de $${new Intl.NumberFormat("es-CO").format(totalDebt)} en ${monthsStr}.`
            : "Está al día.";
        const result = await model.generateContent([
            `Eres el asistente virtual de un ISP colombiano. Cliente: ${cliente.nombre}. ${deudaInfo}`,
            `Mensaje: "${texto}"`,
            `Si no entiendes, redirige a: 💰 "Saldo" | 🛠️ "Falla" | 👨‍💻 "Asesor" | 📸 Foto del comprobante`,
            `Máximo 3 líneas. Solo texto, sin markdown.`,
        ].join("\n"));
        await enviarTexto(cliente.telefono, result.response.text().trim());
    } catch {
        await enviarTexto(cliente.telefono, `Hola ${cliente.nombre} 👋\n\nEscribe:\n💰 *"Saldo"* - ver deuda\n🛠️ *"Falla"* - soporte técnico\n👨‍💻 *"Asesor"* - hablar con humano\n📸 Foto del comprobante - registrar pago`);
    }
}

async function manejarUsuarioDesconocido(phone, name, message) {
    const texto = (
        message.message?.conversation ||
        message.message?.extendedTextMessage?.text ||
        ""
    );

    if (!texto) {
        await enviarTexto(phone, `👋 Hola *${name}*. No tengo este número registrado. Escribe tu *Cédula* para vincularte.`);
        return;
    }

    const cedulaLimpia = texto.replace(/\D/g, "");
    if (cedulaLimpia.length >= 4 && cedulaLimpia.length <= 12) {
        const vinculado = await vincularCliente(cedulaLimpia, phone);
        if (vinculado) {
            await enviarTexto(phone, `✅ *¡Identidad Verificada!*\n\nHola *${vinculado.nombre}*, vinculé tu WhatsApp.\n\nAhora puedes:\n1️⃣ Enviar foto para pagar\n2️⃣ Escribir "Falla"\n3️⃣ Escribir "Asesor"`);
        } else {
            await enviarTexto(phone, `❌ La cédula *${cedulaLimpia}* no está en nuestra base de datos.`);
        }
        return;
    }

    await enviarTexto(phone, `👋 Bienvenido al Bot del ISP.\n\nNo reconozco este número. Responde *únicamente con tu cédula*.`);
}

async function procesarPago(message, cliente) {
    try {
        const pendientes = await db.collection("pagos_preaprobados")
            .where("senderPhone", "==", cliente.telefono)
            .where("status", "==", "pending").get();
        if (pendientes.size >= 2) {
            await enviarTexto(cliente.telefono, "✋ Ya tienes 2 pagos en revisión. Por favor espera.");
            return;
        }

        const buffer = await downloadMediaMessage(message, "buffer", {}, {
            logger: console,
            reuploadRequest: sock.updateMediaMessage,
        });

        const bucket   = admin.storage().bucket();
        const fileName = `pagos/${cliente.telefono}_${Date.now()}.jpg`;
        const file     = bucket.file(fileName);
        await file.save(buffer, { metadata: { contentType: "image/jpeg" } });
        await file.makePublic();
        const publicUrl = `https://storage.googleapis.com/${bucket.name}/${fileName}`;

        const analisis = await analizarConIA(buffer);

        if (!analisis.es_recibo || analisis.confianza < CONFIG.UMBRAL_CONFIANZA) {
            await enviarTexto(cliente.telefono, "⚠️ No pude leer el comprobante. Intenta con una foto más clara.");
            return;
        }

        const { totalDebt } = calcularDeudaCliente(cliente);
        const valorDetectado = analisis.valor || 0;
        const valorFmt = new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(valorDetectado);
        const deudaFmt = new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(totalDebt);

        let mensajeConf = "";
        if (totalDebt === 0)                mensajeConf = `✅ Recibo de *${valorFmt}* recibido. (Tu saldo ya estaba en $0).`;
        else if (valorDetectado < totalDebt) mensajeConf = `⚠️ *Abono Parcial*\nRecibo: ${valorFmt}\nDeuda: ${deudaFmt}\nEn revisión.`;
        else                                mensajeConf = `✅ *Pago Completo*\nRecibo: ${valorFmt}\nCubre tu deuda. Validando.`;

        await db.collection("pagos_preaprobados").add({
            senderName: cliente.nombre, extractedAmount: valorDetectado,
            extractedDate: analisis.fecha, currentDebt: totalDebt,
            imageUrl: publicUrl, date: new Date().toISOString(),
            senderPhone: cliente.telefono, clientId: cliente.id, status: "pending",
        });

        await enviarTexto(cliente.telefono, mensajeConf);
    } catch (e) {
        console.error("Error procesarPago:", e);
        await enviarTexto(cliente.telefono, "❌ Error procesando imagen. Un asesor revisará manualmente.");
    }
}

// ==========================================
// FUNCIONES AUXILIARES
// ==========================================
async function guardarMensajeChat(telefono, message, direction, nombreUsuario) {
    const chatRef = db.collection("chats").doc(normalizePhone(telefono));
    const msgType = Object.keys(message.message || {})[0] || "unknown";
    const texto   = message.message?.conversation
        || message.message?.extendedTextMessage?.text
        || (["imageMessage","image"].includes(msgType) ? "[Imagen]" : "[Archivo]");

    let sentimiento = "neutral";
    if (direction === "in" && typeof texto === "string" && texto.length > 5 && texto !== "[Imagen]") {
        try {
            const model  = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
            const result = await model.generateContent(
                `Clasifica el sentimiento. Responde SOLO una palabra: ENOJO, URGENTE, DUDA, FELIZ, o NEUTRAL.\nMensaje: "${texto}"`
            );
            const analisis = result.response.text().trim().toUpperCase().split(/\s/)[0];
            if (["ENOJO","URGENTE"].includes(analisis)) sentimiento = "urgente";
            else if (analisis === "DUDA")  sentimiento = "duda";
            else if (analisis === "FELIZ") sentimiento = "feliz";
        } catch { /* silencioso */ }
    }

    const updateData = {
        lastMessage:  texto,
        lastUpdated:  admin.firestore.FieldValue.serverTimestamp(),
        userName:     nombreUsuario || "Cliente",
        phone:        normalizePhone(telefono),
        ...(direction === "in" && { sentiment: sentimiento }),
    };
    if (direction === "in") {
        updateData.unreadCount = admin.firestore.FieldValue.increment(1);
        updateData.status      = "open";
    }
    const msgPayload = {
        type:      msgType === "conversation" ? "text" : msgType,
        text:      { body: texto },
        direction,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        ...(message.key?.id && { waMessageId: message.key.id }),
        ...(direction === "out" && { ack: 1 }),
    };
    // Ambas escrituras en paralelo — no dependen la una de la otra
    await Promise.all([
        chatRef.set(updateData, { merge: true }),
        chatRef.collection("messages").add(msgPayload),
    ]);
}

function calcularDeudaCliente(cliente) {
    let totalDebt = 0;
    const months  = [];
    const MESES   = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];
    const hoy     = new Date().getFullYear();
    if (cliente.pagos) {
        for (const year in cliente.pagos) {
            for (const month in cliente.pagos[year]) {
                const data = cliente.pagos[year][month];
                const debt = data.debt || cliente.pago || 0;
                const paid = (data.payments || []).reduce((a, p) => p.type !== "charge" ? a + p.amount : a, 0);
                if (paid < debt) {
                    totalDebt += (debt - paid);
                    if (parseInt(year) >= hoy - 1) months.push(`${MESES[month]} ${year}`);
                }
            }
        }
    }
    return { totalDebt, monthsStr: months.slice(0, 3).join(", ") + (months.length > 3 ? "..." : "") };
}

async function crearTicket(cliente, tipo, descripcion) {
    const existing = await db.collection("support_tickets")
        .where("clienteId", "==", cliente.id)
        .where("tipo", "==", tipo)
        .where("estado", "==", "abierto").get();
    if (!existing.empty) {
        await enviarTexto(cliente.telefono, "⚠️ Ya tienes un caso abierto. Estamos trabajando en ello.");
        return;
    }
    const ref = await db.collection("support_tickets").add({
        clienteId: cliente.id, clienteNombre: cliente.nombre,
        clienteTelefono: cliente.telefono, clienteDireccion: cliente.direccion || "Sin dirección",
        tipo, descripcion, estado: "abierto",
        prioridad: tipo === "pqr" ? "alta" : "media",
        fechaCreacion: admin.firestore.FieldValue.serverTimestamp(),
    });
    await enviarTexto(cliente.telefono, tipo === "soporte"
        ? `🛠️ *Reporte Creado*\nTicket #${ref.id.slice(0,5)}\nTécnicos notificados.`
        : `📝 *PQR Radicada*\nTicket #${ref.id.slice(0,5)}\nEscalado a administración.`
    );
}

async function obtenerClientePorTelefono(telefono) {
    const clean12 = normalizePhone(telefono);                         // 573001234567
    const clean10 = clean12.startsWith("57") && clean12.length === 12
        ? clean12.slice(2) : clean12;                                 // 3001234567
    // Busca en ambos formatos según cómo esté guardado en la BD
    let q = await db.collection("clients").where("telefono", "==", clean10).limit(1).get();
    if (q.empty)
        q = await db.collection("clients").where("telefono", "==", clean12).limit(1).get();
    return q.empty ? null : { id: q.docs[0].id, ...q.docs[0].data() };
}

async function vincularCliente(cedula, telefono) {
    const ref = db.collection("clients");
    let q = await ref.where("cedula", "==", String(cedula)).limit(1).get();
    if (q.empty) q = await ref.where("cedula", "==", Number(cedula)).limit(1).get();
    if (q.empty) return null;
    await q.docs[0].ref.update({ telefono, lastLinkDate: admin.firestore.FieldValue.serverTimestamp() });
    return { id: q.docs[0].id, ...q.docs[0].data() };
}

async function analizarConIA(buffer) {
    try {
        const model  = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
        const result = await model.generateContent([
            `Analiza comprobante. Responde SOLO JSON: {"es_recibo":boolean,"valor":number,"fecha":"YYYY-MM-DD","banco":string,"confianza":number}`,
            { inlineData: { data: buffer.toString("base64"), mimeType: "image/jpeg" } },
        ]);
        return JSON.parse(result.response.text().replace(/```json|```/gi, "").trim());
    } catch {
        return { es_recibo: false, valor: 0, fecha: null, confianza: 0 };
    }
}

// ==========================================
// NÚCLEO DE ENVÍO (Baileys)
// ==========================================
async function enviarTexto(phone, texto) {
    if (!sock || !isConnected) {
        console.warn("⚠️  Bot no conectado. No se puede enviar a", phone);
        return null;
    }
    const jid = toJID(phone);
    try {
        const sent = await sock.sendMessage(jid, { text: texto });
        return sent?.key?.id || null;
    } catch (err) {
        console.error(`❌ Error enviando a ${jid}:`, err.message);
        return null;
    }
}

async function enviarImagen(phone, imageSource, caption = "") {
    if (!sock || !isConnected) return null;
    const jid = toJID(phone);
    try {
        const payload = typeof imageSource === "string"
            ? { image: { url: imageSource }, caption }
            : { image: imageSource,          caption };
        const sent = await sock.sendMessage(jid, payload);
        return sent?.key?.id || null;
    } catch (err) {
        console.error(`❌ Error enviando imagen a ${jid}:`, err.message);
        return null;
    }
}

// ==========================================
// SERVIDOR EXPRESS
// ==========================================
const app = express();

app.use((req, res, next) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, x-admin-secret");
    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
});

app.use(express.json({ limit: "20mb" }));

function auth(req, res, next) {
    if (!CONFIG.ADMIN_SECRET) return next();
    const token = req.headers["x-admin-secret"] || req.query.secret;
    if (token !== CONFIG.ADMIN_SECRET) return res.status(401).json({ error: "No autorizado" });
    next();
}

// ── GET /status ────────────────────────────────────────────────────────────────
// Incluye el QR como data-URL para que el frontend lo muestre directamente (sin iframe).
app.get("/status", (req, res) => {
    res.json({ connected: isConnected, number: sock?.user?.id || null, hasQR: !!currentQR, qr: currentQR || null });
});

// ── POST /logout ───────────────────────────────────────────────────────────────
app.post("/logout", auth, async (_req, res) => {
    try {
        if (sock) {
            try { await sock.logout(); } catch { /* puede fallar si ya estaba desconectado */ }
            sock.ev.removeAllListeners();
            sock = null;
        }
        isConnected = false;
        currentQR   = null;
        await limpiarSesionFirestore(CONFIG.SESSION_ID);
        // Reconecta limpio para mostrar nuevo QR
        setTimeout(iniciarWhatsApp, 1500);
        res.json({ success: true, message: "Sesión cerrada. Escanea el nuevo QR." });
    } catch (err) {
        console.error("Error en /logout:", err);
        res.status(500).json({ success: false, error: err.message });
    }
});

// ── GET /qr ────────────────────────────────────────────────────────────────────
app.get("/qr", auth, async (req, res) => {
    if (isConnected) {
        return res.send(`
            <html><body style="font-family:sans-serif;text-align:center;padding:40px">
            <h2 style="color:green">✅ WhatsApp ya está conectado</h2>
            <p>Número: <strong>${sock?.user?.id || "desconocido"}</strong></p>
            </body></html>
        `);
    }
    if (!currentQR) {
        return res.send(`
            <html><body style="font-family:sans-serif;text-align:center;padding:40px">
            <h2>⏳ Generando QR...</h2>
            <p>Espera unos segundos y recarga la página.</p>
            <script>setTimeout(()=>location.reload(),3000)</script>
            </body></html>
        `);
    }
    res.send(`
        <html><head><meta charset="utf-8">
        <style>body{font-family:sans-serif;text-align:center;padding:40px;background:#f5f5f5}
        img{max-width:300px;border-radius:12px;box-shadow:0 4px 20px rgba(0,0,0,.2)}
        h2{color:#333}p{color:#666}</style></head>
        <body>
        <h2>📱 Escanea con WhatsApp</h2>
        <img src="${currentQR}" alt="QR Code" />
        <p>Abre WhatsApp → Dispositivos vinculados → Vincular dispositivo</p>
        <p style="font-size:12px;opacity:.5">Se recarga automáticamente cada 20s</p>
        <script>setTimeout(()=>location.reload(),20000)</script>
        </body></html>
    `);
});

// ── POST /enviarMensajeManual ──────────────────────────────────────────────────
// El frontend (SupportPage) ya guarda el mensaje en Firestore antes de llamar aquí.
// Solo enviamos por WhatsApp y devolvemos el waMessageId para que el frontend
// pueda actualizar su doc y recibir ACKs de doble-chulo/chulo azul.
app.post("/enviarMensajeManual", auth, async (req, res) => {
    const { telefono, mensaje, firestoreMsgId } = req.body;
    if (!telefono || !mensaje) return res.status(400).json({ error: "Faltan datos" });
    const msgId = await enviarTexto(telefono, mensaje);
    // Si el frontend pasó el ID del doc que guardó, lo vinculamos con el waMessageId
    // para que los ACK receipts (doble chulo / chulo azul) funcionen.
    if (msgId && firestoreMsgId) {
        const jid = normalizePhone(telefono);
        db.collection("chats").doc(jid).collection("messages").doc(firestoreMsgId)
            .update({ waMessageId: msgId }).catch(() => {});
    }
    res.json({ success: !!msgId, msgId });
});

// ── POST /enviarCampana ────────────────────────────────────────────────────────
app.post("/enviarCampana", auth, async (req, res) => {
    const { telefono, nombre, mensaje } = req.body;
    if (!telefono || !mensaje) return res.status(400).json({ error: "Faltan datos" });
    const textoFinal = mensaje.replace(/\{nombre\}/gi, nombre || "Cliente").trim();
    const msgId      = await enviarTexto(telefono, textoFinal);
    if (!msgId) return res.json({ success: false, reason: "Bot no conectado o número inválido" });
    // Crear/actualizar el chat para que aparezca en SupportPage y se puedan ver las respuestas
    await guardarMensajeChat(telefono, { message: { conversation: textoFinal } }, "out", nombre || "Campaña");
    res.json({ success: true, msgId });
});

// ── POST /enviarReciboWA ───────────────────────────────────────────────────────
app.post("/enviarReciboWA", auth, async (req, res) => {
    const { telefono, base64Image, nombreCliente, precio, mes, nombreEmpresa, tipoDocumento } = req.body;
    if (!telefono || !base64Image) return res.status(400).json({ error: "Faltan datos" });

    const bucket     = admin.storage().bucket();
    const cleanPhone = telefono.replace(/\D/g, "");
    const fileName   = `recibos_enviados/ticket_${cleanPhone}_${Date.now()}.png`;
    const file       = bucket.file(fileName);
    const buffer     = Buffer.from(base64Image, "base64");
    await file.save(buffer, { metadata: { contentType: "image/png" } });
    await file.makePublic();
    const publicUrl = `https://storage.googleapis.com/${bucket.name}/${fileName}`;

    const esCobro = tipoDocumento === "cobro";
    const caption = esCobro
        ? `📋 *Factura ${mes || ""}*\n\nCliente: ${nombreCliente || "Cliente"}\nValor: ${precio || "$0"}\n\n${nombreEmpresa || "ISP"}`
        : `✅ *Comprobante de Pago*\n\nCliente: ${nombreCliente || "Cliente"}\nValor: ${precio || "$0"}\n\n${nombreEmpresa || "ISP"}`;

    const msgId  = await enviarImagen(telefono, buffer, caption);
    const resumen = `📎 ${esCobro ? "Factura" : "Comprobante"} enviado — ${precio || ""}`;
    await guardarMensajeChat(cleanPhone, { message: { conversation: resumen } }, "out", nombreCliente || "Cliente");

    res.json({ success: !!msgId, url: publicUrl, msgId });
});

// ── POST /exigirExcusaTecnico ──────────────────────────────────────────────────
app.post("/exigirExcusaTecnico", auth, async (req, res) => {
    const { tecnicoNombre, clienteNombre, instalacionId } = req.body;
    const techQuery = await db.collection("user_profiles")
        .where("role", "==", "tecnico")
        .where("displayName", "==", tecnicoNombre)
        .limit(1).get();

    if (techQuery.empty) return res.status(404).json({ error: "Técnico no encontrado" });

    const tecnicoData     = techQuery.docs[0].data();
    const tecnicoTelefono = tecnicoData.phone || tecnicoData.telefono || tecnicoData.whatsapp;
    if (!tecnicoTelefono) return res.status(400).json({ error: "El técnico no tiene celular registrado" });

    let cleanPhone = tecnicoTelefono.replace(/\D/g, "");
    if (cleanPhone.length === 10) cleanPhone = "57" + cleanPhone;

    await db.collection("esperando_excusa").doc(cleanPhone).set({
        instalacionId, clienteNombre, tecnicoNombre,
        fechaSolicitud: admin.firestore.FieldValue.serverTimestamp(),
    });

    const mensaje = `⚠️ *ALERTA OPERATIVA*\n\nHola *${tecnicoNombre}*. Tienes una orden ATRASADA para el cliente *${clienteNombre}*.\n\n👉 *Responde este mensaje* con el motivo del atraso para registrarlo en el sistema.`;
    const msgId   = await enviarTexto(cleanPhone, mensaje);
    res.json({ success: !!msgId, telefonoContactado: cleanPhone });
});

// ── POST /sugerirRespuestaIA ───────────────────────────────────────────────────
app.post("/sugerirRespuestaIA", auth, async (req, res) => {
    const { historial, clienteInfo } = req.body;
    try {
        const model = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
        const infoCliente = clienteInfo
            ? `Nombre: ${clienteInfo.nombre}. Deuda: $${clienteInfo.deuda?.toLocaleString("es-CO") || 0}. Estado: ${clienteInfo.estado}.`
            : "Cliente no identificado.";
        const historialTexto = Array.isArray(historial)
            ? historial.map(m => `[${m.role === "asesor" ? "ASESOR" : "CLIENTE"}]: ${m.text}`).join("\n")
            : String(historial || "");
        const result = await model.generateContent(
            `Copiloto de soporte ISP. ${infoCliente}\n\nHistorial:\n${historialTexto}\n\nGenera UNA respuesta corta (máx 4 líneas). Tono profesional, español colombiano. Sin markdown.`
        );
        res.json({ sugerencia: result.response.text().trim() });
    } catch (error) {
        console.error("Error sugerirRespuestaIA:", error);
        res.json({ sugerencia: "" });
    }
});

// ── POST /generarMensajeMasivo ─────────────────────────────────────────────────
app.post("/generarMensajeMasivo", auth, async (req, res) => {
    const { customPrompt } = req.body;
    if (!customPrompt) return res.status(400).json({ error: "Falta el prompt" });
    try {
        const model  = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
        const result = await model.generateContent(
            `${customPrompt}\n\nREGLAS: Solo el texto. Sin comillas. Mantén {nombre},{deuda},{empresa}. Máximo 5 líneas.`
        );
        res.json({ mensaje: result.response.text().trim() });
    } catch {
        res.status(500).json({ error: "Error generando mensaje" });
    }
});

// ── POST /escanearRecibo ───────────────────────────────────────────────────────
app.post("/escanearRecibo", auth, async (req, res) => {
    const { base64Image, mimeType } = req.body;
    if (!base64Image) return res.status(400).json({ error: "Falta imagen" });
    try {
        const model  = genAI.getGenerativeModel({ model: "gemini-1.5-flash" });
        const result = await model.generateContent([
            { inlineData: { mimeType: mimeType || "image/jpeg", data: base64Image } },
            `Analiza este recibo. Responde SOLO con JSON:\n{"monto":number,"concepto":"string","categoria":"Operativo|Técnico|Servicios|Nómina|Arriendo|Transporte|Comunicaciones|Otro","fecha":"YYYY-MM-DD","proveedor":"string|null"}`,
        ]);
        const match = result.response.text().match(/\{[\s\S]*\}/);
        if (!match) return res.status(500).json({ error: "No se pudo extraer datos" });
        res.json(JSON.parse(match[0]));
    } catch {
        res.status(500).json({ error: "No se pudo procesar la imagen" });
    }
});

// ── POST /analizarFinanzas ─────────────────────────────────────────────────────
app.post("/analizarFinanzas", auth, async (req, res) => {
    const { totalIn, totalOut, profit, topCategorias, periodo, empresa } = req.body;
    const margen = totalIn > 0 ? Math.round((profit / totalIn) * 100) : 0;
    try {
        const model  = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
        const result = await model.generateContent(
            `Asesor financiero para ${empresa || "ISP colombiano"}. Período "${periodo}": Ingresos $${Number(totalIn).toLocaleString("es-CO")} | Gastos $${Number(totalOut).toLocaleString("es-CO")} | Utilidad $${Number(profit).toLocaleString("es-CO")} | Margen ${margen}% | Categorías: ${topCategorias}\n\nGenera 4 observaciones. SOLO JSON:\n[{"tipo":"positivo","titulo":"…","detalle":"…"},{"tipo":"alerta","titulo":"…","detalle":"…"},{"tipo":"sugerencia","titulo":"…","detalle":"…"},{"tipo":"negativo","titulo":"…","detalle":"…"}]`
        );
        const match = result.response.text().match(/\[[\s\S]*?\]/);
        if (!match) return res.status(500).json({ error: "Respuesta inválida" });
        res.json({ insights: JSON.parse(match[0]) });
    } catch {
        res.status(500).json({ error: "Error analizando finanzas" });
    }
});

// ── POST /generarFirmaWompi ────────────────────────────────────────────────────
app.post("/generarFirmaWompi", auth, (req, res) => {
    const { reference, amountInCents, currency } = req.body;
    if (!reference || !amountInCents || !currency)
        return res.status(400).json({ error: "Datos incompletos" });
    const signature = crypto.createHash("sha256")
        .update(`${reference}${amountInCents}${currency}${CONFIG.WOMPI_INTEGRITY_SECRET}`)
        .digest("hex");
    res.json({ signature });
});

// ── POST /webhookWompi ─────────────────────────────────────────────────────────
app.post("/webhookWompi", async (req, res) => {
    try {
        const evento = req.body;
        if (evento.event !== "transaction.updated") return res.status(200).send("Ignorado");

        const transaccion = (await axios.get(
            `https://production.wompi.co/v1/transactions/${evento.data.transaction.id}`,
            { headers: { Authorization: `Bearer ${CONFIG.WOMPI_PRIVATE_KEY}` } }
        )).data.data;

        if (transaccion.status !== "APPROVED") return res.status(200).send("Ignorado");

        const clientId  = transaccion.reference.split("-")[1];
        let valorPagado = transaccion.amount_in_cents / 100;

        await db.runTransaction(async (t) => {
            const clientRef  = db.collection("clients").doc(clientId);
            const clientSnap = await t.get(clientRef);
            if (!clientSnap.exists) throw new Error("Cliente no encontrado");

            const cliente = clientSnap.data();
            let pagos = JSON.parse(JSON.stringify(cliente.pagos || {}));

            for (const year in pagos) {
                for (const month in pagos[year]) {
                    if (valorPagado <= 0) break;
                    const mes       = pagos[year][month];
                    const pagado    = (mes.payments || []).filter(p => p.status !== "voided").reduce((s, p) => s + p.amount, 0);
                    const pendiente = (mes.debt || cliente.pago || 0) - pagado;
                    if (pendiente > 0) {
                        const aplicar = Math.min(pendiente, valorPagado);
                        if (!pagos[year][month].payments) pagos[year][month].payments = [];
                        pagos[year][month].payments.push({
                            amount: aplicar, date: new Date().toISOString(),
                            method: transaccion.payment_method_type, status: "approved",
                            reference: transaccion.reference,
                        });
                        valorPagado -= aplicar;
                    }
                }
            }

            t.update(clientRef, { pagos, ultimoPago: admin.firestore.FieldValue.serverTimestamp() });
            t.set(db.collection("pagos_aprobados").doc(transaccion.id), {
                clientId, nombre: cliente.nombre, metodo: transaccion.payment_method_type,
                valorOriginal: transaccion.amount_in_cents / 100, referencia: transaccion.reference,
                fecha: admin.firestore.FieldValue.serverTimestamp(),
            });

            const valorFmt = new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(transaccion.amount_in_cents / 100);
            await enviarTexto(cliente.telefono, `✅ *¡Pago Exitoso!*\n\nRecibimos tu pago de *${valorFmt}* (Ref: ${transaccion.reference}).\nServicio al día. ¡Gracias!`);
        });

        res.status(200).send("OK");
    } catch (error) {
        console.error("Error webhookWompi:", error);
        res.status(500).send("Error");
    }
});

// ==========================================
// ARRANQUE
// ==========================================
const server = app.listen(CONFIG.PORT, async () => {
    console.log(`🚀 Servidor Railway corriendo en puerto ${CONFIG.PORT}`);
    console.log(`📱 QR disponible en: /qr`);
    await iniciarWhatsApp();
});

// Cierre limpio cuando Railway envía SIGTERM (redeploy / stop)
// Evita que WhatsApp detecte dos sesiones activas (código 440)
async function shutdown(signal) {
    console.log(`\n🛑 ${signal} recibido — cerrando conexión de WhatsApp...`);
    isConnected = false;
    if (sock) {
        try {
            sock.ev.removeAllListeners();
            await sock.logout().catch(() => {});
        } catch { /* silencioso */ }
        sock = null;
    }
    server.close(() => {
        console.log("✅ Servidor HTTP cerrado. Saliendo.");
        process.exit(0);
    });
    // Forzar salida si tarda más de 8 segundos
    setTimeout(() => process.exit(0), 8000);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT",  () => shutdown("SIGINT"));
