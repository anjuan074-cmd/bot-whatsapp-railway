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

    const isImgMsg = ["imageMessage", "image"].includes(msgType);

    // Leer estado del chat PRIMERO para saber si es humanMode
    // (necesario antes de decidir si subir imagen a Storage)
    const [chatDocSnap, configSnap] = await Promise.all([
        db.collection("chats").doc(userPhone).get(),
        db.collection("settings").doc("bot_config").get(),
    ]);
    const isGlobalPause = configSnap.exists && configSnap.data().globalBotPaused === true;
    const isHumanMode   = (chatDocSnap.exists && chatDocSnap.data().humanMode === true) || isGlobalPause;

    // Siempre descargar buffer de imagen para poder procesarla
    // humanMode  → subir a chat/  y guardar mensaje con URL ahora
    // bot mode   → NO guardar mensaje aún; procesarPago sube a pagos/ y guarda con esa URL
    let imageBuffer = null;
    let imagePublicUrl = null;
    if (isImgMsg) {
        try {
            imageBuffer = await downloadMediaMessage(message, "buffer", {}, {
                logger: console,
                reuploadRequest: sock.updateMediaMessage,
            });
        } catch (e) {
            console.error("[IMG] Error descargando imagen:", e.message);
        }

        if (isHumanMode && imageBuffer) {
            try {
                const bucket   = admin.storage().bucket();
                const fileName = `chat/${userPhone}_${Date.now()}.jpg`;
                const file     = bucket.file(fileName);
                await file.save(imageBuffer, { metadata: { contentType: "image/jpeg" } });
                await file.makePublic();
                imagePublicUrl = `https://storage.googleapis.com/${bucket.name}/${fileName}`;
            } catch (e) {
                console.error("[IMG] Error subiendo imagen a chat/:", e.message);
            }
        }
    }

    // humanMode  → guardar mensaje ahora (con URL si se pudo subir)
    // bot + img  → posponer; procesarPago guardará el mensaje con imageUrl de pagos/
    // texto/otro → guardar ahora
    const [, excusaSnap, cliente] = await Promise.all([
        (isImgMsg && !isHumanMode)
            ? Promise.resolve()
            : guardarMensajeChat(userPhone, message, "in", userName,
                imagePublicUrl ? { imageUrl: imagePublicUrl, type: "image" } : {}),
        db.collection("esperando_excusa").doc(userPhone).get(),
        obtenerClientePorTelefono(userPhone),
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
        await botResponder(userPhone, `✅ He registrado tu justificación en la orden de *${dataExcusa.clienteNombre}*. ¡Gracias!`);
        return;
    }

    if (isGlobalPause && !(chatDocSnap.exists && chatDocSnap.data().humanMode)) {
        await db.collection("chats").doc(userPhone).set({ humanMode: true }, { merge: true });
    }

    if (!isHumanMode) {
        const configData = configSnap.exists ? configSnap.data() : {};
        if (!cliente) {
            await manejarUsuarioDesconocido(userPhone, userName, message, configData);
        } else {
            await manejarClienteRegistrado(cliente, message, imageBuffer, imagePublicUrl);
        }
    }
}

// ==========================================
// LÓGICA DE NEGOCIO
// ==========================================
async function manejarClienteRegistrado(cliente, message, imageBuffer = null, imagePublicUrl = null) {
    const msgType = Object.keys(message.message || {})[0];

    // ── Imagen → procesar comprobante de pago ──────────────────────────────────
    if (["imageMessage", "image"].includes(msgType)) {
        await botResponder(cliente.telefono, "⏳ Procesando tu comprobante...");
        await procesarPago(message, cliente, imageBuffer, imagePublicUrl);
        return;
    }

    const textoOriginal = (
        message.message?.conversation ||
        message.message?.extendedTextMessage?.text ||
        ""
    ).trim();

    if (!textoOriginal) return;

    // ── Gemini clasifica la intención y genera respuesta natural ───────────────
    const { totalDebt, monthsStr } = calcularDeudaCliente(cliente);
    const empresa  = process.env.NOMBRE_EMPRESA || "el ISP";
    const nequi    = process.env.CUENTA_NEQUI   || "";
    const deudaCtx = totalDebt > 0
        ? `Tiene deuda de $${new Intl.NumberFormat("es-CO").format(totalDebt)} correspondiente a: ${monthsStr}.`
        : "Está al día con sus pagos.";

    let intent     = "otro";
    let respuestaIA = "";

    try {
        const model  = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
        const result = await model.generateContent(`Eres el asistente virtual de ${empresa}, un proveedor de internet colombiano.
Cliente: *${cliente.nombre}*. Estado de cuenta: ${deudaCtx}

Mensaje recibido: "${textoOriginal}"

Tu tarea:
1. Clasifica la intención en UNO de: saludo | saldo | soporte | pqr | asesor | pago | otro
   - saludo: saludos, preguntas casuales, cómo estás, etc.
   - saldo: preguntas sobre deuda, factura, cuánto debe, fecha de corte, etc.
   - soporte: cualquier problema técnico de internet (lento, sin señal, caído, cable robado/cortado, router, luz roja, etc.)
   - pqr: quejas, reclamos, mala atención, inconformidades, insultos al servicio
   - asesor: quiere hablar con una persona humana
   - pago: cómo pagar, métodos de pago, dónde consignar
   - otro: cualquier otra cosa

2. Genera una respuesta natural, amigable y corta (máximo 3 líneas) en español colombiano.
   NO uses menús de opciones numerados ni le pidas que escriba palabras específicas.
   Responde a lo que dijo de forma conversacional.

Responde SOLO con JSON válido sin texto adicional:
{"intent":"<uno de los valores>","respuesta":"<texto de respuesta>"}`);

        const raw    = result.response.text().replace(/```json|```/gi, "").trim();
        const match  = raw.match(/\{[\s\S]*\}/);
        const parsed = match ? JSON.parse(match[0]) : {};
        intent      = parsed.intent     || "otro";
        respuestaIA = parsed.respuesta  || "";
    } catch (err) {
        console.error("[IA] Error clasificando intención:", err.message);
    }

    // ── Ejecutar acción según intención ────────────────────────────────────────
    const fmt = new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 });

    switch (intent) {

        case "asesor":
            await db.collection("chats").doc(cliente.telefono).set({
                humanMode: true,
                unreadCount: admin.firestore.FieldValue.increment(1),
                sentiment: "urgente",
            }, { merge: true });
            await botResponder(cliente.telefono,
                respuestaIA || `Entendido ${cliente.nombre}, pauso el asistente. Un asesor humano te atenderá pronto. 🙋`
            );
            break;

        case "saldo":
            if (totalDebt > 0) {
                const nequiSaldo = nequi ? `\n\n📱 *Solo aceptamos pago por Nequi*\nNequi: *${nequi}*\n\n📸 Después de pagar, envía aquí la foto del comprobante de Nequi.` : "\n\n📸 Envía aquí la foto del comprobante de pago.";
                await botResponder(cliente.telefono,
                    `${respuestaIA}\n\n💰 Saldo pendiente: *${fmt.format(totalDebt)}*\nCorresponde a: ${monthsStr}${nequiSaldo}`
                );
            } else {
                await botResponder(cliente.telefono,
                    respuestaIA || `✅ ¡Estás al día, ${cliente.nombre}! No tienes ningún saldo pendiente.`
                );
            }
            break;

        case "pago": {
            const infoPago = nequi
                ? `\n\n✅ *Único método de pago aceptado: Nequi*\n\n📱 Número Nequi: *${nequi}*\n\nPasos:\n1️⃣ Abre Nequi y envía el pago a ese número\n2️⃣ Toma captura del comprobante\n3️⃣ Envíala aquí y quedará registrada automáticamente`
                : "\n\n📸 Envía la foto del comprobante de pago aquí.";
            await botResponder(cliente.telefono, (respuestaIA || "Para pagar es muy fácil 😊") + infoPago);
            break;
        }

        case "soporte": {
            const existente = await db.collection("support_tickets")
                .where("clienteId", "==", cliente.id)
                .where("tipo", "==", "soporte")
                .where("estado", "==", "abierto").get();
            if (!existente.empty) {
                await botResponder(cliente.telefono,
                    `${respuestaIA || "Entiendo, eso es molesto 😞"}\n\nYa tienes un reporte de soporte abierto. Nuestro equipo técnico lo está revisando.`
                );
            } else {
                const ref = await db.collection("support_tickets").add({
                    clienteId: cliente.id, clienteNombre: cliente.nombre,
                    clienteTelefono: cliente.telefono, clienteDireccion: cliente.direccion || "",
                    tipo: "soporte", descripcion: textoOriginal, estado: "abierto",
                    prioridad: "media", fechaCreacion: admin.firestore.FieldValue.serverTimestamp(),
                });
                await botResponder(cliente.telefono,
                    `${respuestaIA || "Entiendo tu situación 🛠️"}\n\nCreé un reporte técnico (ticket #${ref.id.slice(0, 5)}). Un técnico lo revisará y te contactará pronto.`
                );
            }
            break;
        }

        case "pqr": {
            const existentePqr = await db.collection("support_tickets")
                .where("clienteId", "==", cliente.id)
                .where("tipo", "==", "pqr")
                .where("estado", "==", "abierto").get();
            if (!existentePqr.empty) {
                await botResponder(cliente.telefono,
                    `${respuestaIA || "Lamento que estés pasando por esto."}\n\nYa tienes un caso de PQR abierto. Lo estamos gestionando.`
                );
            } else {
                const ref = await db.collection("support_tickets").add({
                    clienteId: cliente.id, clienteNombre: cliente.nombre,
                    clienteTelefono: cliente.telefono, clienteDireccion: cliente.direccion || "",
                    tipo: "pqr", descripcion: textoOriginal, estado: "abierto",
                    prioridad: "alta", fechaCreacion: admin.firestore.FieldValue.serverTimestamp(),
                });
                await botResponder(cliente.telefono,
                    `${respuestaIA || "Entiendo tu inconformidad y la tomamos muy en serio."}\n\nRadiqué tu PQR (caso #${ref.id.slice(0, 5)}). La administración lo revisará.`
                );
            }
            break;
        }

        default:
            // Saludo o cualquier otro mensaje — la IA ya generó una respuesta natural
            await botResponder(cliente.telefono,
                respuestaIA || `Hola ${cliente.nombre} 👋 ¿En qué te puedo ayudar hoy?`
            );
            break;
    }
}

async function manejarUsuarioDesconocido(phone, name, message, configData) {
    const texto = (
        message.message?.conversation ||
        message.message?.extendedTextMessage?.text ||
        ""
    ).trim();

    // Lee qué métodos están habilitados desde la config del bot (ya leída en procesarMensajeEntrante)
    const metodosHabilitados =
        (configData?.verificacionMetodos && configData.verificacionMetodos.length > 0)
            ? configData.verificacionMetodos
            : ["cedula"];
    const maxIntentos = configData?.verificacionIntentos || 3;

    const verifRef  = db.collection("verificacion_pendiente").doc(phone);
    const verifSnap = await verifRef.get();

    // Comprobar si el estado de verificación sigue vigente (expira en 30 min)
    let estado = null;
    if (verifSnap.exists) {
        const d          = verifSnap.data();
        const expiraEn   = d.expires?.toDate?.();
        if (expiraEn && expiraEn > new Date()) estado = d;
        else await verifRef.delete();                       // expirado
    }

    // ¿Es un saludo o primer mensaje? → reinicia el flujo
    const esGreeting = !texto || /^(hola|hi|hey|buenas?|buen[ao]s?\s*(d[ií]as?|tardes?|noches?)|saludos?|ola)\b/i.test(texto);

    if (!estado || esGreeting) {
        const primerMetodo = metodosHabilitados[0];
        const meta         = METODO_VERIF[primerMetodo] || { pregunta: "Escribe tu cédula." };
        const expires      = new Date();
        expires.setMinutes(expires.getMinutes() + 30);

        await verifRef.set({
            metodoActual:     primerMetodo,
            metodosRestantes: metodosHabilitados.slice(1),
            intentos:         0,
            maxIntentos,
            expires:          admin.firestore.Timestamp.fromDate(expires),
        });

        const saludo = esGreeting && texto
            ? `👋 ¡Hola *${name}*! Soy el asistente virtual.\n\nPara atenderte, necesito verificar tu identidad. 🔐\n\n${meta.pregunta}`
            : `👋 Hola *${name}*. Este número no está registrado aún.\n\nVamos a verificar tu identidad. 🔐\n\n${meta.pregunta}`;

        await botResponder(phone, saludo);
        return;
    }

    // — Flujo activo de verificación —
    const { metodoActual, metodosRestantes, intentos } = estado;

    if (!texto) {
        const meta = METODO_VERIF[metodoActual] || { pregunta: "Responde para continuar." };
        await botResponder(phone, `Por favor responde con tu *${meta.nombre || metodoActual}*.\n\n${meta.pregunta}`);
        return;
    }

    const cliente = await encontrarClientePorMetodo(metodoActual, texto);

    if (cliente) {
        // ✅ Verificación exitosa
        await vincularClienteAlTelefono(cliente.id, phone);
        await verifRef.delete();
        await botResponder(phone,
            `✅ *¡Identidad Verificada!*\n\nHola *${cliente.nombre}*, ya vinculé tu WhatsApp a nuestra base de datos. 🎉\n\nAhora puedes:\n1️⃣ 📸 Enviar foto de tu comprobante de pago\n2️⃣ Escribir *"Falla"* para soporte técnico\n3️⃣ Escribir *"Saldo"* para ver tu deuda\n4️⃣ Escribir *"Asesor"* para hablar con alguien`
        );
        return;
    }

    // ❌ Falló este intento
    const nuevosIntentos = (intentos || 0) + 1;

    if (nuevosIntentos >= maxIntentos) {
        // Pasar al siguiente método si hay uno disponible
        if (metodosRestantes && metodosRestantes.length > 0) {
            const siguienteMetodo = metodosRestantes[0];
            const meta            = METODO_VERIF[siguienteMetodo] || { pregunta: "Escribe el dato solicitado." };
            const expires         = new Date();
            expires.setMinutes(expires.getMinutes() + 30);

            await verifRef.set({
                metodoActual:     siguienteMetodo,
                metodosRestantes: metodosRestantes.slice(1),
                intentos:         0,
                maxIntentos,
                expires:          admin.firestore.Timestamp.fromDate(expires),
            });

            await botResponder(phone,
                `❌ No encontré coincidencias con ese dato.\n\nProbemos de otra forma:\n\n${meta.pregunta}`
            );
        } else {
            // Sin más métodos disponibles
            await verifRef.delete();
            await botResponder(phone,
                `😔 No pude verificar tu identidad con los datos proporcionados.\n\nEscribe *"Asesor"* para hablar con un humano que pueda ayudarte.`
            );
        }
        return;
    }

    // Intento fallido — quedan más
    const meta           = METODO_VERIF[metodoActual] || { nombre: metodoActual };
    const restantes      = maxIntentos - nuevosIntentos;
    await verifRef.update({ intentos: nuevosIntentos });
    await botResponder(phone,
        `❌ No encontré a nadie con ese dato en el sistema.\n\nIntenta de nuevo con tu *${meta.nombre}* (${restantes} intento${restantes !== 1 ? "s" : ""} restante${restantes !== 1 ? "s" : ""}).`
    );
}

async function procesarPago(message, cliente, buffer = null, publicUrl = null) {
    try {
        const pendientes = await db.collection("pagos_preaprobados")
            .where("senderPhone", "==", cliente.telefono)
            .where("status", "==", "pending").get();
        if (pendientes.size >= 2) {
            await botResponder(cliente.telefono, "✋ Ya tienes 2 pagos en revisión. Por favor espera.");
            return;
        }

        // Buffer viene del handler principal; subir a pagos/ si no tiene URL aún
        if (!buffer) {
            buffer = await downloadMediaMessage(message, "buffer", {}, {
                logger: console,
                reuploadRequest: sock.updateMediaMessage,
            });
        }
        if (!publicUrl) {
            const bucket   = admin.storage().bucket();
            const fileName = `pagos/${cliente.telefono}_${Date.now()}.jpg`;
            const file     = bucket.file(fileName);
            await file.save(buffer, { metadata: { contentType: "image/jpeg" } });
            await file.makePublic();
            publicUrl = `https://storage.googleapis.com/${bucket.name}/${fileName}`;
        }

        // Guardar el mensaje en el chat CON la URL — siempre, sea válido o no
        // Así el asesor puede ver el comprobante en el chat para aprobarlo
        await guardarMensajeChat(cliente.telefono, message, "in", cliente.nombre,
            { imageUrl: publicUrl, type: "image" });

        const nequiConfig  = process.env.CUENTA_NEQUI || "";
        const analisis     = await analizarConIA(buffer, nequiConfig);

        // ── Validar que sea comprobante de Nequi ──────────────────────────────
        if (!analisis.es_recibo || !analisis.es_nequi) {
            const aviso = nequiConfig
                ? `❌ Solo aceptamos pagos por *Nequi*.\n\n📱 Número Nequi: *${nequiConfig}*\n\nPor favor realiza el pago a ese número y envía la foto del comprobante de Nequi.`
                : "❌ Solo aceptamos comprobantes de *Nequi*. Envía la foto del comprobante de Nequi.";
            await botResponder(cliente.telefono, aviso);
            return;
        }

        if (analisis.confianza < CONFIG.UMBRAL_CONFIANZA) {
            await botResponder(cliente.telefono, "⚠️ No pude leer bien el comprobante de Nequi. Intenta con una foto más clara y sin recortes.");
            return;
        }

        // ── Validar número destinatario coincida con CUENTA_NEQUI ────────────
        if (nequiConfig && analisis.numero_destino) {
            const destLimpio   = analisis.numero_destino.replace(/\D/g, "");
            const nequiLimpio  = nequiConfig.replace(/\D/g, "");
            if (destLimpio.length >= 7 && nequiLimpio.length >= 7
                && !destLimpio.includes(nequiLimpio.slice(-7))
                && !nequiLimpio.includes(destLimpio.slice(-7))) {
                await botResponder(cliente.telefono,
                    `❌ El comprobante no corresponde a nuestro Nequi.\n\n📱 El pago debe ser al número: *${nequiConfig}*\n\nRevisa el destinatario y envía el comprobante correcto.`
                );
                return;
            }
        }

        const { totalDebt } = calcularDeudaCliente(cliente);
        const valorDetectado = analisis.valor || 0;
        const fmt2 = new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 });
        const valorFmt = fmt2.format(valorDetectado);
        const deudaFmt = fmt2.format(totalDebt);
        const refStr   = analisis.referencia ? `\nRef: ${analisis.referencia}` : "";

        let mensajeConf = "";
        if (totalDebt === 0)                mensajeConf = `✅ Comprobante Nequi de *${valorFmt}* recibido.${refStr} (Tu saldo ya estaba en $0).`;
        else if (valorDetectado < totalDebt) mensajeConf = `⚠️ *Abono Parcial Nequi*\nRecibo: ${valorFmt}\nDeuda: ${deudaFmt}${refStr}\nEn revisión por un asesor.`;
        else                                mensajeConf = `✅ *Pago Completo vía Nequi*\nRecibo: ${valorFmt}\nCubre tu deuda.${refStr}\nValidando con el equipo.`;

        await db.collection("pagos_preaprobados").add({
            senderName: cliente.nombre, extractedAmount: valorDetectado,
            extractedDate: analisis.fecha, currentDebt: totalDebt,
            imageUrl: publicUrl, date: new Date().toISOString(),
            senderPhone: cliente.telefono, clientId: cliente.id, status: "pending",
            referencia: analisis.referencia || "",
            numeroDestino: analisis.numero_destino || "",
            confianza: analisis.confianza,
        });

        await botResponder(cliente.telefono, mensajeConf);
    } catch (e) {
        console.error("Error procesarPago:", e);
        await botResponder(cliente.telefono, "❌ Error procesando imagen. Un asesor revisará manualmente.");
    }
}

// ==========================================
// FUNCIONES AUXILIARES
// ==========================================
async function guardarMensajeChat(telefono, message, direction, nombreUsuario, extraFields = {}) {
    const chatRef = db.collection("chats").doc(normalizePhone(telefono));
    const msgType = Object.keys(message.message || {})[0] || "unknown";
    const caption = message.message?.imageMessage?.caption || message.message?.videoMessage?.caption || "";
    const texto   = message.message?.conversation
        || message.message?.extendedTextMessage?.text
        || caption
        || (["imageMessage","image"].includes(msgType) ? "[Imagen]"
          : ["audioMessage","pttMessage"].includes(msgType) ? "[Audio]"
          : ["videoMessage"].includes(msgType) ? "[Video]"
          : ["documentMessage"].includes(msgType) ? (message.message?.documentMessage?.fileName || "[Archivo]")
          : "[Archivo]");

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
        ...extraFields,
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
        await botResponder(cliente.telefono, "⚠️ Ya tienes un caso abierto. Estamos trabajando en ello.");
        return;
    }
    const ref = await db.collection("support_tickets").add({
        clienteId: cliente.id, clienteNombre: cliente.nombre,
        clienteTelefono: cliente.telefono, clienteDireccion: cliente.direccion || "Sin dirección",
        tipo, descripcion, estado: "abierto",
        prioridad: tipo === "pqr" ? "alta" : "media",
        fechaCreacion: admin.firestore.FieldValue.serverTimestamp(),
    });
    await botResponder(cliente.telefono, tipo === "soporte"
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

// ==========================================
// VERIFICACIÓN MULTI-MÉTODO
// ==========================================

// Etiquetas legibles para cada método de verificación
const METODO_VERIF = {
    cedula:          { nombre: "cédula",                 pregunta: "Por favor escríbeme tu número de *cédula*." },
    nombre:          { nombre: "nombre completo",         pregunta: "Escríbeme tu *nombre completo* tal como está registrado." },
    direccion:       { nombre: "dirección",               pregunta: "¿Cuál es tu *dirección de servicio* registrada?" },
    plan_valor:      { nombre: "valor del plan mensual",  pregunta: "¿Cuánto pagas mensualmente por tu plan? (Solo el número, ej: 45000)" },
    codigo_cliente:  { nombre: "código de cliente",       pregunta: "Escríbeme tu *código de cliente* (lo encuentras en tu factura)." },
};

// Normaliza texto: minúsculas, sin tildes, sin espacios extras
function normalText(str) {
    return (str || "")
        .toLowerCase()
        .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
        .replace(/\s+/g, " ").trim();
}

// Busca un cliente en Firestore usando el método y valor dados.
// Retorna { id, ...data } o null. NO actualiza el teléfono.
async function encontrarClientePorMetodo(metodo, valor) {
    const ref = db.collection("clients");
    const v   = (valor || "").trim();

    if (metodo === "cedula") {
        const num = v.replace(/\D/g, "");
        if (!num || num.length < 3) return null;
        let q = await ref.where("cedula", "==", String(num)).limit(1).get();
        if (q.empty) q = await ref.where("cedula", "==", Number(num)).limit(1).get();
        return q.empty ? null : { id: q.docs[0].id, ...q.docs[0].data() };
    }

    if (metodo === "nombre") {
        const normV = normalText(v);
        if (normV.length < 3) return null;
        // Firestore no soporta búsqueda case-insensitive; cargamos hasta 300 docs
        const snap = await ref.limit(300).get();
        const hit  = snap.docs.find(d => {
            const n = normalText(d.data().nombre || "");
            return n === normV || n.includes(normV) || normV.includes(n.split(" ")[0]);
        });
        return hit ? { id: hit.id, ...hit.data() } : null;
    }

    if (metodo === "direccion") {
        const normV = normalText(v);
        if (normV.length < 4) return null;
        const snap = await ref.limit(300).get();
        const hit  = snap.docs.find(d => {
            const dir = normalText(d.data().direccion || "");
            // Coincide si comparten al menos los primeros 6 caracteres significativos
            return dir.includes(normV.slice(0, 8)) || normV.includes(dir.slice(0, 8));
        });
        return hit ? { id: hit.id, ...hit.data() } : null;
    }

    if (metodo === "plan_valor") {
        const num = Number(v.replace(/\D/g, ""));
        if (!num) return null;
        const q = await ref.where("pago", "==", num).limit(5).get();
        // Solo es válido si el resultado es único (evitar ambigüedad)
        if (q.size === 1) return { id: q.docs[0].id, ...q.docs[0].data() };
        return null;
    }

    if (metodo === "codigo_cliente") {
        const snap = await ref.doc(v).get();
        return snap.exists ? { id: snap.id, ...snap.data() } : null;
    }

    return null;
}

async function vincularClienteAlTelefono(clienteId, telefono) {
    await db.collection("clients").doc(clienteId).update({
        telefono,
        lastLinkDate: admin.firestore.FieldValue.serverTimestamp(),
    });
}

async function analizarConIA(buffer, nequiEsperado = "") {
    try {
        const model  = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
        const destHint = nequiEsperado
            ? `El número Nequi del destinatario que esperamos es: ${nequiEsperado}.`
            : "";
        const prompt = `Eres un experto en comprobantes de pago colombianos. Analiza esta imagen.
Busca específicamente comprobantes de la app *Nequi* (Bancolombia):
- Busca el logo o texto "Nequi" en la imagen.
- Extrae el monto de la transferencia.
- Extrae el número de celular destinatario (10 dígitos colombianos).
- Extrae la referencia o ID de transacción si la hay.
- Extrae la fecha del comprobante.
${destHint}

Responde SOLO con JSON sin texto adicional ni bloques de código:
{"es_recibo":boolean,"es_nequi":boolean,"valor":number,"fecha":"YYYY-MM-DD","numero_destino":"string","referencia":"string","confianza":number}

Donde:
- es_recibo: true si la imagen parece un comprobante de pago
- es_nequi: true SOLO si claramente es un comprobante de Nequi
- valor: monto en pesos colombianos (solo número, sin símbolos)
- numero_destino: número celular destino de 10 dígitos (solo dígitos, sin espacios ni guiones)
- confianza: 0-100 qué tan seguro estás de que es un comprobante de Nequi válido`;

        const result = await model.generateContent([
            prompt,
            { inlineData: { data: buffer.toString("base64"), mimeType: "image/jpeg" } },
        ]);
        const raw   = result.response.text().replace(/```json|```/gi, "").trim();
        const match = raw.match(/\{[\s\S]*\}/);
        return match ? JSON.parse(match[0]) : { es_recibo: false, es_nequi: false, valor: 0, fecha: null, numero_destino: "", referencia: "", confianza: 0 };
    } catch {
        return { es_recibo: false, es_nequi: false, valor: 0, fecha: null, numero_destino: "", referencia: "", confianza: 0 };
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

// Envía texto Y guarda en Firestore (para mensajes automáticos del bot)
async function botResponder(phone, texto) {
    const msgId = await enviarTexto(phone, texto);
    const fakeMsg = { message: { conversation: texto }, key: msgId ? { id: msgId } : {} };
    guardarMensajeChat(phone, fakeMsg, "out", "Bot").catch(() => {});
    return msgId;
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
            await botResponder(cliente.telefono, `✅ *¡Pago Exitoso!*\n\nRecibimos tu pago de *${valorFmt}* (Ref: ${transaccion.reference}).\nServicio al día. ¡Gracias!`);
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
