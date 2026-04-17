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
const { createCanvas, registerFont } = require("canvas");
// Registrar fuentes del sistema (DejaVu siempre presente en Railway/Debian)
try {
    registerFont("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",       { family: "DejaVu" });
    registerFont("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",  { family: "DejaVu", weight: "bold" });
} catch (_) { /* En local usa fallback del sistema */ }
const RECEIPT_FONT       = '"DejaVu", "Liberation Sans", Arial, sans-serif';
const RECEIPT_FONT_BOLD  = 'bold ' + RECEIPT_FONT;
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

// ── Caché Nequi (evita leer Firestore en cada mensaje) ─────────────────────────
let _nequiCache = { value: null, ts: 0 };
async function getCuentaNequi() {
    const now = Date.now();
    if (_nequiCache.value !== null && now - _nequiCache.ts < 5 * 60 * 1000) {
        return _nequiCache.value;
    }
    try {
        const snap = await db.collection("settings").doc("bot_config").get();
        const val  = snap.exists ? (snap.data().cuentaNequi || "") : (process.env.CUENTA_NEQUI || "");
        _nequiCache = { value: val, ts: now };
        return val;
    } catch {
        return process.env.CUENTA_NEQUI || "";
    }
}

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

    // messages.update: Baileys dispara este evento cuando cambia el estado (ack)
    // de los mensajes ENVIADOS por nosotros (enviados → entregado → leído).
    sock.ev.on("messages.update", async (updates) => {
        for (const update of updates) {
            if (!update.key.fromMe) continue; // solo nuestros mensajes salientes
            const status = update.update?.status;
            if (!status) continue;
            // proto.WebMessageInfo.Status: SERVER_ACK=1, DELIVERY_ACK=2, READ=3, PLAYED=4
            const ack = status >= 3 ? 3 : status >= 2 ? 2 : 1;
            const remoteJid = update.key.remoteJid || "";
            let raw = remoteJid.split("@")[0].split(":")[0];
            if (remoteJid.endsWith("@lid")) {
                const alt = update.key.remoteJidAlt;
                if (!alt) continue;
                raw = alt.split("@")[0].split(":")[0];
            }
            const jid = normalizePhone(raw);
            if (!jid) continue;
            console.log(`[MSG-UPDATE] ${jid} msgId=${update.key.id} status=${status} ack=${ack}`);
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

    // ── Detectar si es personal interno (admin / cobrador / tecnico) ──────────
    // Traemos todos los perfiles de staff y comparamos normalizando los dígitos
    // para no fallar por diferencias de formato (+57, espacios, 10 vs 12 dígitos).
    const userPhone10 = userPhone.slice(-10); // últimos 10 dígitos del número entrante
    const staffSnap = await db.collection("user_profiles")
        .where("role", "in", ["admin", "cobrador", "tecnico"])
        .get();
    const staffDoc = staffSnap.docs.find(d => {
        const data = d.data();
        // Puede estar en el campo "phone" o "telefono"
        const raw = (data.phone || data.telefono || "").replace(/\D/g, "");
        // Coincide si los últimos 10 dígitos son iguales
        return raw.slice(-10) === userPhone10;
    });
    const staffMember = staffDoc ? { id: staffDoc.id, ...staffDoc.data() } : null;
    const isStaff = !!staffMember;

    if (isStaff) {
        const texto = (
            message.message?.conversation ||
            message.message?.extendedTextMessage?.text ||
            message.message?.imageMessage?.caption ||
            ""
        ).trim();

        // Si hay imagen Y hay un estado gasto_recibo activo → delegar con buffer
        if (isImgMsg && imageBuffer) {
            const staffStateSnap = await db.collection("staff_states").doc(userPhone).get();
            const staffStage = staffStateSnap.exists ? (staffStateSnap.data().stage || "") : "";
            if (staffStage === "gasto_recibo") {
                // Subir imagen a Storage en carpeta receipts/
                let receiptUrl = null;
                try {
                    const bucket   = admin.storage().bucket();
                    const fileName = `receipts/${userPhone}_${Date.now()}.jpg`;
                    const file     = bucket.file(fileName);
                    await file.save(imageBuffer, { metadata: { contentType: "image/jpeg" } });
                    await file.makePublic();
                    receiptUrl = `https://storage.googleapis.com/${bucket.name}/${fileName}`;
                } catch (e) {
                    console.error("[IMG] Error subiendo recibo de gasto:", e.message);
                }
                await manejarConsultaStaff(userPhone, "__imagen_recibo__", staffMember, receiptUrl);
                return;
            }
        }

        if (texto) await manejarConsultaStaff(userPhone, texto, staffMember);
        return;
    }

    if (!isHumanMode) {
        const configData = configSnap.exists ? configSnap.data() : {};
        if (!cliente) {
            await manejarUsuarioDesconocido(userPhone, userName, message, configData);
        } else {
            await manejarClienteRegistrado(cliente, message, imageBuffer);
        }
    }
}

// ==========================================
// LÓGICA DE NEGOCIO
// ==========================================
async function manejarClienteRegistrado(cliente, message, imageBuffer = null) {
    const msgType = Object.keys(message.message || {})[0];

    // ── Imagen → procesar comprobante de pago ──────────────────────────────────
    if (["imageMessage", "image"].includes(msgType)) {
        await botResponder(cliente.telefono, "⏳ Procesando tu comprobante...");
        await procesarPago(message, cliente, imageBuffer);
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
    const nequi    = await getCuentaNequi();
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
                const nequiSaldo = nequi
                    ? `\n\n💳 *Métodos de pago aceptados:*\n• Nequi\n• Bre-B\n• Transferencia desde cualquier banco\n\n📱 Número destino: *${nequi}*\n\n📸 Después de pagar, envía aquí la foto del comprobante.`
                    : "\n\n📸 Envía aquí la foto del comprobante de pago.";
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
                ? `\n\n💳 *Métodos de pago aceptados:*\n• *Nequi* — envía directamente desde la app\n• *Bre-B* — desde cualquier banco al número\n• *Transferencia bancaria* — desde tu banco al número\n\n📱 Número destino: *${nequi}*\n\nPasos:\n1️⃣ Realiza el pago a ese número por cualquier método\n2️⃣ Toma captura del comprobante\n3️⃣ Envíala aquí y quedará registrada automáticamente ✅`
                : "\n\n📸 Envía la foto del comprobante de pago aquí.";
            await botResponder(cliente.telefono, (respuestaIA || "Para pagar es muy fácil 😊") + infoPago);
            break;
        }

        case "soporte":
            await crearTicket(cliente, "soporte", textoOriginal, respuestaIA);
            break;

        case "pqr":
            await crearTicket(cliente, "pqr", textoOriginal, respuestaIA);
            break;

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

async function procesarPago(message, cliente, buffer = null) {
    try {
        if (!buffer) {
            buffer = await downloadMediaMessage(message, "buffer", {}, {
                logger: console, reuploadRequest: sock.updateMediaMessage,
            });
        }

        // ── 1. ANALIZAR CON IA PRIMERO (sin subir nada a Storage todavía) ──────
        const nequiConfig = await getCuentaNequi();
        const analisis    = await analizarConIA(buffer, nequiConfig);

        // ── 2. VALIDACIONES ──────────────────────────────────────────────────────
        // No es comprobante de pago en absoluto
        if (!analisis.es_recibo) {
            await guardarMensajeChat(cliente.telefono, message, "in", cliente.nombre,
                { verificationFailed: true, failReason: "not_receipt" });
            const aviso = nequiConfig
                ? `❌ La imagen no parece un comprobante de pago.\n\nAceptamos comprobantes de *Nequi, Bre-B o transferencia bancaria* al número *${nequiConfig}*.\n\nEnvía la foto del comprobante.`
                : "❌ La imagen no parece un comprobante de pago. Envía la foto del comprobante de transferencia.";
            await botResponder(cliente.telefono, aviso);
            return;
        }

        // Imagen borrosa o ilegible
        if (analisis.confianza < CONFIG.UMBRAL_CONFIANZA) {
            await guardarMensajeChat(cliente.telefono, message, "in", cliente.nombre,
                { verificationFailed: true, failReason: "low_confidence" });
            await botResponder(cliente.telefono, "⚠️ No pude leer bien el comprobante. Intenta con una foto más clara, sin recortes y que se vea el destinatario y el monto.");
            return;
        }

        // Verificar que el destinatario sea el número correcto
        if (nequiConfig) {
            if (analisis.numero_correcto === false) {
                const destVisual = analisis.numero_destino ? ` (detectado: ${analisis.numero_destino})` : "";
                await guardarMensajeChat(cliente.telefono, message, "in", cliente.nombre,
                    { verificationFailed: true, failReason: "wrong_number" });
                await botResponder(cliente.telefono,
                    `❌ El comprobante no está dirigido a nuestro número.${destVisual}\n\n📱 El pago debe ser al: *${nequiConfig}*\n\nPuedes pagar por *Nequi, Bre-B o transferencia bancaria* a ese número. Revisa el destinatario y envía el comprobante correcto.`
                );
                return;
            }
            if (analisis.numero_correcto === null) {
                await guardarMensajeChat(cliente.telefono, message, "in", cliente.nombre,
                    { verificationFailed: true, failReason: "unverifiable_number" });
                await botResponder(cliente.telefono,
                    `⚠️ No pude leer el destinatario en el comprobante.\n\nAsegúrate de que la foto muestre claramente el número o llave al que se envió el dinero. Debe ser al: *${nequiConfig}*`
                );
                return;
            }
            // numero_correcto === true → continuar
        }

        const pendientes = await db.collection("pagos_preaprobados")
            .where("senderPhone", "==", cliente.telefono)
            .where("status", "==", "pending").get();
        if (pendientes.size >= 2) {
            await guardarMensajeChat(cliente.telefono, message, "in", cliente.nombre, {});
            await botResponder(cliente.telefono, "✋ Ya tienes 2 pagos en revisión. Por favor espera.");
            return;
        }

        // ── 3. VÁLIDO — ahora sí subir a Storage y guardar con imageUrl ────────
        const bucket   = admin.storage().bucket();
        const fileName = `pagos/${cliente.telefono}_${Date.now()}.jpg`;
        const file     = bucket.file(fileName);
        await file.save(buffer, { metadata: { contentType: "image/jpeg" } });
        await file.makePublic();
        const publicUrl = `https://storage.googleapis.com/${bucket.name}/${fileName}`;

        await guardarMensajeChat(cliente.telefono, message, "in", cliente.nombre,
            { imageUrl: publicUrl, type: "image" });

        // ── 4. Guardar en pagos_preaprobados ────────────────────────────────────
        const { totalDebt } = calcularDeudaCliente(cliente);
        const valorDetectado = analisis.valor || 0;
        const fmt2     = new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 });
        const valorFmt = fmt2.format(valorDetectado);
        const deudaFmt = fmt2.format(totalDebt);
        const refStr   = analisis.referencia ? `\nRef: ${analisis.referencia}` : "";

        await db.collection("pagos_preaprobados").add({
            senderName:      cliente.nombre,
            extractedAmount: valorDetectado,
            extractedDate:   analisis.fecha,
            extractedHora:   analisis.hora || "",
            currentDebt:     totalDebt,
            imageUrl:        publicUrl,
            date:            new Date().toISOString(),
            senderPhone:     cliente.telefono,
            clientId:        cliente.id,
            status:          "pending",
            referencia:      analisis.referencia || "",
            numeroDestino:   analisis.numero_destino || "",
            bancoOrigen:     analisis.banco_origen || "Desconocido",
            nombreRemitente: analisis.nombre_remitente || "",
            confianza:       analisis.confianza,
        });

        let mensajeConf = "";
        if (totalDebt === 0)                mensajeConf = `✅ Comprobante Nequi de *${valorFmt}* recibido.${refStr} (Tu saldo ya estaba en $0).`;
        else if (valorDetectado < totalDebt) mensajeConf = `⚠️ *Abono Parcial Nequi*\nRecibo: ${valorFmt}\nDeuda: ${deudaFmt}${refStr}\nEn revisión por un asesor.`;
        else                                mensajeConf = `✅ *Pago Completo vía Nequi*\nRecibo: ${valorFmt}\nCubre tu deuda.${refStr}\nValidando con el equipo.`;

        await botResponder(cliente.telefono, mensajeConf);
    } catch (e) {
        console.error("Error procesarPago:", e);
        await guardarMensajeChat(cliente.telefono, message, "in", cliente.nombre, {}).catch(() => {});
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

// ── Genera imagen de recibo de pago (canvas) ──────────────────────────────
// Devuelve un Buffer PNG listo para enviar con enviarImagen()
function generarReciboImagen({ clienteNombre, clienteDireccion = "", monto, fecha, metodo = "Efectivo", mesPago = "", empresa = "", cajero = "Admin", referencia = "" }) {
    const W   = 520, PAD = 32;
    const fnt = (size, bold = false) => `${bold ? "bold " : ""}${size}px ${RECEIPT_FONT}`;
    const fmtCOP   = (n) => new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(n);
    const fmtFecha = (d) => { try { return new Date(d).toLocaleDateString("es-CO", { day: "2-digit", month: "long", year: "numeric" }); } catch { return d || ""; } };
    const truncate = (s, max = 38) => String(s).length > max ? String(s).slice(0, max - 2) + ".." : String(s);

    const lineH = 24, headerH = 94, footerH = 52;
    const rows = [
        ["Cliente",    clienteNombre],
        ["Direccion",  clienteDireccion || "-"],
        ["Mes pagado", mesPago || "-"],
        ["Metodo",     metodo],
        ["Cajero",     cajero],
        ...(referencia ? [["Referencia", referencia]] : []),
    ];
    const H = headerH + 20 + rows.length * (lineH + 10) + 20 + 64 + 20 + footerH;

    const canvas = createCanvas(W, H);
    const ctx    = canvas.getContext("2d");

    // Fondo
    ctx.fillStyle = "#ffffff";
    ctx.fillRect(0, 0, W, H);
    ctx.strokeStyle = "#e2e8f0";
    ctx.lineWidth = 1.5;
    ctx.strokeRect(1, 1, W - 2, H - 2);

    // Header verde
    ctx.fillStyle = "#16a34a";
    ctx.fillRect(0, 0, W, headerH);

    // Línea decorativa más oscura en la parte superior
    ctx.fillStyle = "#15803d";
    ctx.fillRect(0, 0, W, 4);

    ctx.fillStyle = "#ffffff";
    ctx.textAlign = "center";
    ctx.font = fnt(22, true);
    ctx.fillText(empresa || "ISP", W / 2, 38);

    ctx.font = fnt(13);
    ctx.fillText("Recibo de Pago", W / 2, 59);

    ctx.font = fnt(11);
    ctx.fillStyle = "rgba(255,255,255,0.7)";
    ctx.fillText(fmtFecha(fecha), W / 2, 76);

    // Badge PAGADO (sin roundRect — solo rect redondeado manual con arco)
    const bx = W / 2 - 38, by = headerH - 16, bw = 76, bh = 22, br = 11;
    ctx.fillStyle = "#dcfce7";
    ctx.beginPath();
    ctx.moveTo(bx + br, by);
    ctx.lineTo(bx + bw - br, by);
    ctx.arcTo(bx + bw, by, bx + bw, by + br, br);
    ctx.lineTo(bx + bw, by + bh - br);
    ctx.arcTo(bx + bw, by + bh, bx + bw - br, by + bh, br);
    ctx.lineTo(bx + br, by + bh);
    ctx.arcTo(bx, by + bh, bx, by + bh - br, br);
    ctx.lineTo(bx, by + br);
    ctx.arcTo(bx, by, bx + br, by, br);
    ctx.closePath();
    ctx.fill();
    ctx.fillStyle = "#15803d";
    ctx.font = fnt(11, true);
    ctx.fillText("PAGADO", W / 2, headerH + 4);

    // Tabla de datos
    let y = headerH + 28;
    ctx.textAlign = "left";
    rows.forEach(([label, value], i) => {
        ctx.fillStyle = i % 2 === 0 ? "#f8fafc" : "#ffffff";
        ctx.fillRect(PAD, y - 16, W - PAD * 2, lineH + 6);

        ctx.fillStyle = "#64748b";
        ctx.font = fnt(11);
        ctx.fillText(label, PAD + 10, y);

        ctx.fillStyle = "#1e293b";
        ctx.font = fnt(12, true);
        ctx.fillText(truncate(value), PAD + 136, y);

        y += lineH + 10;
    });

    // Separador
    ctx.strokeStyle = "#e2e8f0";
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(PAD, y + 4); ctx.lineTo(W - PAD, y + 4); ctx.stroke();
    y += 18;

    // Caja total
    ctx.fillStyle = "#f0fdf4";
    ctx.fillRect(PAD, y, W - PAD * 2, 60);
    ctx.strokeStyle = "#bbf7d0";
    ctx.lineWidth = 1;
    ctx.strokeRect(PAD, y, W - PAD * 2, 60);

    ctx.textAlign = "center";
    ctx.fillStyle = "#64748b";
    ctx.font = fnt(11);
    ctx.fillText("TOTAL PAGADO", W / 2, y + 18);

    ctx.fillStyle = "#15803d";
    ctx.font = fnt(26, true);
    ctx.fillText(fmtCOP(monto), W / 2, y + 48);
    y += 60 + 18;

    // Footer
    ctx.fillStyle = "#f1f5f9";
    ctx.fillRect(0, H - footerH, W, footerH);
    ctx.strokeStyle = "#e2e8f0";
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(0, H - footerH); ctx.lineTo(W, H - footerH); ctx.stroke();

    ctx.fillStyle = "#94a3b8";
    ctx.font = fnt(10);
    ctx.textAlign = "center";
    ctx.fillText("Documento generado automaticamente  -  " + (empresa || "ISP"), W / 2, H - footerH + 18);
    ctx.fillText("Este recibo es valido como comprobante de pago", W / 2, H - footerH + 36);

    return canvas.toBuffer("image/png");
}

function calcularDeudaCliente(cliente) {
    const MESES = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];
    const hoy   = new Date();
    // Igual que getClientDetailedStatus de la app React:
    // iterar desde la fecha de registro hasta el mes actual inclusive
    const regDate    = cliente.fecha ? new Date(cliente.fecha) : new Date(2022, 0, 1);
    const startAbs   = (regDate.getFullYear() * 12 + regDate.getMonth()) + 1;
    const targetAbs  = hoy.getFullYear() * 12 + hoy.getMonth(); // mes actual inclusive

    let totalDebt = 0;
    const months  = [];

    for (let abs = startAbs; abs <= targetAbs; abs++) {
        const y = Math.floor(abs / 12);
        const m = abs % 12;
        const data = cliente.pagos?.[y]?.[m] || {};
        const debt = data.debt !== undefined ? data.debt : (cliente.pago || 0);
        const paid = (data.payments || [])
            .filter(p => p.status !== "voided" && p.type !== "charge")
            .reduce((s, p) => s + p.amount, 0);
        if (paid < debt) {
            totalDebt += (debt - paid);
            months.push(`${MESES[m]} ${y}`);
        }
    }

    return { totalDebt, monthsStr: months.slice(0, 4).join(", ") + (months.length > 4 ? "..." : "") };
}

async function crearTicket(cliente, tipo, descripcion, mensajeIA) {
    // Anti-duplicado: no crear si ya hay ticket abierto del mismo tipo para este cliente
    const existing = await db.collection("support_tickets")
        .where("clienteId", "==", cliente.id)
        .where("tipo", "==", tipo)
        .where("estado", "==", "abierto").get();
    if (!existing.empty) {
        await botResponder(cliente.telefono,
            `${mensajeIA || "Entendido."}\n\n⚠️ Ya tienes un caso abierto (tipo: ${tipo}). Nuestro equipo lo está atendiendo.`
        );
        return;
    }

    // Auto-asignación: técnico con menos tickets abiertos
    let tecnicoAsignado = null;
    try {
        const [techSnap, openSnap] = await Promise.all([
            db.collection("user_profiles").where("role", "==", "tecnico").get(),
            db.collection("support_tickets").where("estado", "==", "abierto").get(),
        ]);
        if (!techSnap.empty) {
            const carga = {};
            openSnap.docs.forEach(d => {
                const tid = d.data().tecnicoAsignado?.id;
                if (tid) carga[tid] = (carga[tid] || 0) + 1;
            });
            const candidato = techSnap.docs
                .map(d => ({ id: d.id, ...d.data(), carga: carga[d.id] || 0 }))
                .sort((a, b) => a.carga - b.carga)[0];
            if (candidato) {
                tecnicoAsignado = {
                    id: candidato.id,
                    nombre: candidato.displayName || candidato.name || "Técnico",
                    telefono: candidato.phone || candidato.telefono || "",
                    carga: candidato.carga,
                };
            }
        }
    } catch (e) {
        console.error("[crearTicket] Error al asignar técnico:", e.message);
    }

    const ref = await db.collection("support_tickets").add({
        clienteId: cliente.id, clienteNombre: cliente.nombre,
        clienteTelefono: cliente.telefono, clienteDireccion: cliente.direccion || "Sin dirección",
        tipo, descripcion, estado: "abierto",
        prioridad: tipo === "pqr" ? "alta" : "media",
        tecnicoAsignado,
        fechaCreacion: admin.firestore.FieldValue.serverTimestamp(),
    });

    const ticketId = ref.id.slice(0, 5).toUpperCase();

    // Notificar al técnico asignado si tiene número registrado
    if (tecnicoAsignado?.telefono) {
        try {
            await botResponder(
                normalizePhone(tecnicoAsignado.telefono),
                `🔔 *Nuevo ticket asignado*\n\nCliente: *${cliente.nombre}*\n📍 ${cliente.direccion || "Sin dirección"}\nTipo: ${tipo}\nID: #${ticketId}\n\nDescripción:\n"${descripcion.slice(0, 150)}"`
            );
        } catch (e) { /* Notificación opcional — no bloquear */ }
    }

    if (tipo === "soporte") {
        await botResponder(cliente.telefono,
            `${mensajeIA || "Entendido, registré tu caso 🛠️"}\n\nTicket #${ticketId} creado. Un técnico te contactará pronto.\n${tecnicoAsignado ? `👷 Asignado a: *${tecnicoAsignado.nombre}*` : ""}`
        );
    } else {
        await botResponder(cliente.telefono,
            `${mensajeIA || "Lamento la inconformidad. La tomaremos muy en serio 📝"}\n\nPQR #${ticketId} radicada. Administración la revisará.`
        );
    }
}

// ==========================================
// CONSULTAS DE SISTEMA — STAFF INTERNO
// admin / cobrador / tecnico pueden consultar
// el sistema enviando mensajes de WhatsApp al bot.
// La IA entiende lenguaje natural: no se necesitan comandos específicos.
// ==========================================

// ── Helpers de búsqueda fuzzy ──────────────────────────────────────────────
function norm(s) {
    return (s || "").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").trim();
}
function levenshtein(a, b) {
    const dp = Array.from({ length: a.length + 1 }, (_, i) => [i]);
    for (let j = 0; j <= b.length; j++) dp[0][j] = j;
    for (let i = 1; i <= a.length; i++)
        for (let j = 1; j <= b.length; j++)
            dp[i][j] = Math.min(
                dp[i-1][j] + 1, dp[i][j-1] + 1,
                dp[i-1][j-1] + (a[i-1] !== b[j-1] ? 1 : 0)
            );
    return dp[a.length][b.length];
}
// Devuelve true si cada token del query coincide con algún token del nombre
// (substring exacto o distancia de edición ≤1 para tokens ≥5 chars)
function fuzzyNombre(nombre, query) {
    const nameN  = norm(nombre);
    const queryN = norm(query);
    if (nameN.includes(queryN)) return true;
    const nameTk  = nameN.split(/\s+/);
    const queryTk = queryN.split(/\s+/);
    return queryTk.every(qt =>
        nameTk.some(nt =>
            nt.includes(qt) || qt.includes(nt) ||
            (qt.length >= 5 && levenshtein(qt, nt) <= 1)
        )
    );
}

// ── Busca UN cliente por nombre (fuzzy) o teléfono ────────────────────────
async function buscarClienteEnDB(parametro) {
    const digits = parametro.replace(/\D/g, "");
    const snap   = await db.collection("clients").get();
    const todos  = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    // Exacto primero, luego fuzzy, luego teléfono
    return (
        todos.find(c => norm(c.nombre) === norm(parametro)) ||
        todos.find(c =>
            (c.nombre && fuzzyNombre(c.nombre, parametro)) ||
            (digits.length >= 6 && c.telefono && c.telefono.replace(/\D/g,"").includes(digits))
        ) || null
    );
}

// ── Busca TODOS los clientes que coincidan (fuzzy) ────────────────────────
async function buscarClientesEnDB(parametro) {
    const digits = parametro.replace(/\D/g, "");
    const snap   = await db.collection("clients").get();
    return snap.docs
        .map(d => ({ id: d.id, ...d.data() }))
        .filter(c =>
            (c.nombre && fuzzyNombre(c.nombre, parametro)) ||
            (digits.length >= 6 && c.telefono && c.telefono.replace(/\D/g,"").includes(digits))
        );
}

// ── Último pago registrado de un cliente (fecha, monto, método) ───────────
function ultimoPago(cliente) {
    let lastDate = null, lastAmount = 0, lastMethod = "";
    if (cliente.pagos) {
        for (const yr of Object.keys(cliente.pagos)) {
            for (const mo of Object.keys(cliente.pagos[yr])) {
                for (const p of (cliente.pagos[yr][mo].payments || [])) {
                    if (p.type !== "charge" && p.status !== "voided" && p.date) {
                        const d = new Date(p.date);
                        if (!isNaN(d) && (!lastDate || d > lastDate)) {
                            lastDate = d; lastAmount = p.amount; lastMethod = p.method || "";
                        }
                    }
                }
            }
        }
    }
    return {
        ts:     lastDate ? lastDate.getTime() : 0,
        fecha:  lastDate ? lastDate.toLocaleDateString("es-CO", { day: "2-digit", month: "short", year: "numeric" }) : null,
        monto:  lastAmount,
        metodo: lastMethod,
    };
}

// ── Guarda el intercambio en el historial de conversación del staff ────────
async function appendConversationHistory(phone, userText, botText) {
    const ref  = db.collection("staff_states").doc(phone);
    const snap = await ref.get();
    const prev = snap.exists ? (snap.data().conversationHistory || []) : [];
    const updated = [
        ...prev,
        { role: "user", text: userText.slice(0, 400) },
        { role: "bot",  text: botText.slice(0, 400) },
    ].slice(-20); // máximo 10 turnos (20 entradas)
    await ref.set({
        conversationHistory: updated,
        expiresAt: admin.firestore.Timestamp.fromMillis(Date.now() + 24 * 60 * 60 * 1000),
    }, { merge: true });
}

// ── Genera respuesta natural con Gemini a partir de datos del sistema ──────
async function generarRespuestaNatural(preguntaOriginal, datosContexto, nombre, rolLabel, empresa, history = []) {
    try {
        const model = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
        const historialStr = history.length > 0
            ? `\nConversación previa:\n${history.map(h => `${h.role === "user" ? nombre : "Bot"}: ${h.text}`).join("\n")}\n`
            : "";
        const result = await model.generateContent(
`Eres el asistente interno de ${empresa}. Respóndele a ${nombre} (${rolLabel}).
${historialStr}
Lo que escribió ahora: "${preguntaOriginal}"

Información del sistema para responder:
${datosContexto}

Reglas:
- Responde en español colombiano, informal pero profesional
- Máximo 6 líneas, directo al punto
- Usa *negritas* para números o nombres importantes
- Si hay varios ítems, usa viñetas con "•"
- No digas "Hola" ni repitas el nombre si es una respuesta a datos
- Si no hay datos suficientes, pregunta naturalmente qué necesita
- Tono de colega que sabe del sistema, no de robot
- Ten en cuenta la conversación previa para dar continuidad`
        );
        return result.response.text().trim();
    } catch {
        return datosContexto || "No pude obtener la información. Intenta de nuevo.";
    }
}

// Aplica un pago manual al registro de deuda de un cliente en Firestore
async function aplicarPagoManual(cliente, monto, registradoPor, opciones = {}) {
    const { method = "efectivo" } = opciones; // imageUrl no se guarda en payments[] (evita acumulación de espacio)
    let remaining = monto;
    const pagos   = cliente.pagos ? JSON.parse(JSON.stringify(cliente.pagos)) : {};

    // Iterar meses desde el más antiguo al más reciente
    const regDate  = cliente.fecha ? new Date(cliente.fecha) : new Date(2022, 0, 1);
    const startAbs = (regDate.getFullYear() * 12 + regDate.getMonth()) + 1;
    const hoy      = new Date();
    const endAbs   = hoy.getFullYear() * 12 + hoy.getMonth();

    for (let abs = startAbs; abs <= endAbs && remaining > 0; abs++) {
        const y  = String(Math.floor(abs / 12));
        const mo = String(abs % 12);
        const m  = pagos[y]?.[mo] || {};
        const debt = m.debt !== undefined ? m.debt : (cliente.pago || 0);
        const paid = (m.payments || [])
            .filter(p => p.status !== "voided" && p.type !== "charge")
            .reduce((s, p) => s + p.amount, 0);
        const owed = Math.max(0, debt - paid);
        if (owed > 0) {
            const amount = Math.min(remaining, owed);
            if (!pagos[y]) pagos[y] = {};
            if (!pagos[y][mo]) pagos[y][mo] = { debt: cliente.pago || 0, payments: [] };
            const entry = { amount, date: new Date().toISOString(), method, status: "active", type: "payment", source: `manual_staff:${registradoPor}` };
            pagos[y][mo].payments = [...(pagos[y]?.[mo]?.payments || []), entry];
            remaining -= amount;
        }
    }

    // Saldo a favor si sobra
    if (remaining > 0) {
        const now = new Date();
        const y = String(now.getFullYear());
        const mo = String(now.getMonth());
        if (!pagos[y]) pagos[y] = {};
        if (!pagos[y][mo]) pagos[y][mo] = { debt: cliente.pago || 0, payments: [] };
        const entry = { amount: remaining, date: new Date().toISOString(), method, status: "active", type: "payment", source: `manual_staff_favor:${registradoPor}` };
        pagos[y][mo].payments = [...(pagos[y][mo].payments || []), entry];
    }

    await db.collection("clients").doc(cliente.id).set({ pagos }, { merge: true });
    return remaining;
}

// Flujo multi-paso: registrar pago manual
// stages: pago_manual_cliente → pago_manual_monto → pago_manual_confirmar
async function manejarFlujoPagoManual(phone, texto, nombre, state) {
    const fmt = (n) => new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(n);
    const textoNorm = texto.toLowerCase().trim();

    // Cancelación en cualquier etapa
    if (/^(cancelar|salir|cancel|stop|no gracias)$/i.test(textoNorm)) {
        await clearStaffState(phone);
        await botResponder(phone, "❌ Registro de pago cancelado.");
        return true;
    }

    if (state.stage === "pago_manual_cliente") {
        // Esperando nombre del cliente
        const cliente = await buscarClienteEnDB(texto);
        if (!cliente) {
            await botResponder(phone, `🔍 No encontré un cliente con "*${texto}*".\n\nEscribe el nombre completo o teléfono, o escribe *cancelar* para salir.`);
            return true;
        }
        const { totalDebt } = calcularDeudaCliente(cliente);
        await setStaffState(phone, { stage: "pago_manual_monto", clienteId: cliente.id, clienteNombre: cliente.nombre, clienteTelefono: cliente.telefono || "", deudaActual: totalDebt });
        await botResponder(phone, `👤 Cliente encontrado: *${cliente.nombre}*\n💰 Deuda actual: *${totalDebt > 0 ? fmt(totalDebt) : "Al día ✓"}*\n\n¿Cuánto vas a registrar? Escribe el monto en pesos (ej: *50000*)\n\nO escribe *cancelar* para salir.`);
        return true;
    }

    if (state.stage === "pago_manual_monto") {
        // Esperando monto
        const monto = parseInt(texto.replace(/\D/g, ""), 10);
        if (!monto || monto < 1000) {
            await botResponder(phone, "Escribe un monto válido en pesos (mínimo $1.000). Ej: *50000*");
            return true;
        }
        await setStaffState(phone, { ...state, stage: "pago_manual_confirmar", monto });
        await botResponder(phone,
            `📋 *Confirma el pago manual:*\n\n👤 Cliente: *${state.clienteNombre}*\n💵 Monto: *${fmt(monto)}*\n👷 Registrado por: *${nombre}*\n\nEscribe *sí* para confirmar o *cancelar* para salir.`
        );
        return true;
    }

    if (state.stage === "pago_manual_confirmar") {
        const esSi = /^(s[ií]|yes|confirmar|ok|listo|confirmo)$/i.test(textoNorm);
        if (!esSi) {
            await botResponder(phone, `Responde *sí* para confirmar el pago de *${state.clienteNombre}* o *cancelar* para salir.`);
            return true;
        }
        // Ejecutar el registro
        try {
            const clienteSnap = await db.collection("clients").doc(state.clienteId).get();
            if (!clienteSnap.exists) {
                await clearStaffState(phone);
                await botResponder(phone, "❌ No encontré el cliente en la base de datos. El pago no fue registrado.");
                return true;
            }
            const clienteData = { id: clienteSnap.id, ...clienteSnap.data() };
            const favor = await aplicarPagoManual(clienteData, state.monto, nombre);
            const fmt2 = (n) => new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(n);
            const { totalDebt: deudaRestante } = calcularDeudaCliente({ ...clienteData }); // recalcular post-pago
            await clearStaffState(phone);

            // Notificar al cliente si tiene teléfono
            if (state.clienteTelefono) {
                botResponder(normalizePhone(state.clienteTelefono),
                    `✅ *Pago recibido*\n\nHola *${state.clienteNombre}*, se registró un abono de *${fmt2(state.monto)}*.\n${favor > 0 ? `💚 Saldo a favor: ${fmt2(favor)}` : deudaRestante > 0 ? `📌 Deuda restante: ${fmt2(deudaRestante)}` : "✅ Ya estás al día."}\n\n— ${process.env.NOMBRE_EMPRESA || "El ISP"}`
                ).catch(() => {});
            }

            await botResponder(phone, `✅ *Pago registrado exitosamente*\n\n👤 ${state.clienteNombre}\n💵 Monto: *${fmt2(state.monto)}*\n${favor > 0 ? `💚 Saldo a favor: ${fmt2(favor)}` : deudaRestante > 0 ? `📌 Deuda restante: ${fmt2(deudaRestante)}` : "✅ Cliente al día."}\n\n_Registrado por ${nombre}_`);
        } catch (e) {
            console.error("[PAGO_MANUAL] Error:", e.message);
            await clearStaffState(phone);
            await botResponder(phone, "❌ Error registrando el pago. Intenta de nuevo.");
        }
        return true;
    }

    return false;
}

// ── Aprueba comprobante pendiente y aplica el pago al cliente ─────────────
async function aprobarPagoPreaprobado(pagoId, pagoData, aprobadoPor) {
    const clientSnap = await db.collection("clients").doc(pagoData.clientId).get();
    if (!clientSnap.exists) throw new Error("Cliente no encontrado");
    const cliente = { id: clientSnap.id, ...clientSnap.data() };
    const monto   = pagoData.extractedAmount || 0;
    const favor   = await aplicarPagoManual(cliente, monto, `aprobado_bot:${aprobadoPor}`, {
        imageUrl: pagoData.imageUrl || null,
        method:   "transferencia",
    });
    await db.collection("pagos_preaprobados").doc(pagoId).set({
        status: "approved", approvedBy: aprobadoPor,
        approvedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    const fmt2 = (n) => new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(n);
    if (pagoData.senderPhone) {
        botResponder(normalizePhone(pagoData.senderPhone),
            `✅ *Pago aprobado — ${process.env.NOMBRE_EMPRESA || "El ISP"}*\n\nHola *${pagoData.senderName || cliente.nombre}*, tu pago de *${fmt2(monto)}* fue confirmado.\n${favor > 0 ? `💚 Saldo a favor: ${fmt2(favor)}` : "✅ Todo en orden."}`
        ).catch(() => {});
    }
    return favor;
}

// ── Flujo: editar usuario del sistema ─────────────────────────────────────
async function manejarFlujoEditarUsuario(phone, texto, nombre, state) {
    const tn = texto.toLowerCase().trim();
    if (/^(cancelar|salir|cancel)$/i.test(tn)) {
        await clearStaffState(phone); await botResponder(phone, "❌ Edición cancelada."); return true;
    }
    const fmt2 = (n) => new Intl.NumberFormat("es-CO",{style:"currency",currency:"COP",maximumFractionDigits:0}).format(n);

    if (state.stage === "editar_usuario_buscar") {
        const snap = await db.collection("user_profiles").get();
        const lower = texto.toLowerCase();
        const found = snap.docs.map(d=>({id:d.id,...d.data()}))
            .find(u => (u.nombre||u.displayName||u.name||"").toLowerCase().includes(lower) || u.id.toLowerCase().includes(lower));
        if (!found) {
            await botResponder(phone, `🔍 No encontré un usuario con "${texto}".\nEscribe nombre o email, o *cancelar* para salir.`);
            return true;
        }
        await setStaffState(phone, { ...state, stage:"editar_usuario_campo", userId:found.id,
            userNombre: found.nombre||found.displayName||found.id, userRolActual: found.role||"sin rol",
            userPhone: found.phone||found.telefono||"" });
        await botResponder(phone,
            `👤 *${found.nombre||found.displayName||found.id}*\n📧 ${found.id}\n🏷️ Rol: *${found.role||"sin rol"}*\n📱 Tel: ${found.phone||found.telefono||"S/N"}\n\n¿Qué cambias?\n• *rol* → admin / cobrador / tecnico / caja\n• *nombre* → cambiar nombre\n• *teléfono* → cambiar teléfono\n\nO *cancelar*`
        );
        return true;
    }

    if (state.stage === "editar_usuario_campo") {
        const sin = tn.normalize("NFD").replace(/[\u0300-\u036f]/g,"");
        const campo = ["rol","nombre","telefono"].find(c => sin.includes(c));
        if (!campo) { await botResponder(phone, "Escribe *rol*, *nombre* o *teléfono*. O *cancelar*."); return true; }
        await setStaffState(phone, { ...state, stage:"editar_usuario_valor", campoElegido:campo });
        const hints = { rol:"Roles: *admin*, *cobrador*, *tecnico*, *caja*", nombre:"Escribe el nuevo nombre:", telefono:"Escribe el teléfono (10 dígitos):" };
        await botResponder(phone, hints[campo] + "\n\nO *cancelar*");
        return true;
    }

    if (state.stage === "editar_usuario_valor") {
        const { campoElegido } = state;
        if (campoElegido==="rol" && !["admin","cobrador","tecnico","caja"].includes(tn)) {
            await botResponder(phone, "Rol inválido. Usa: *admin*, *cobrador*, *tecnico* o *caja*."); return true;
        }
        if (campoElegido==="telefono" && texto.replace(/\D/g,"").length < 10) {
            await botResponder(phone, "Teléfono inválido. Escribe 10 dígitos."); return true;
        }
        const nuevoValor = campoElegido==="rol" ? tn : texto.trim();
        await setStaffState(phone, { ...state, stage:"editar_usuario_confirmar", nuevoValor });
        await botResponder(phone,
            `📋 *Confirma el cambio:*\n\n👤 ${state.userNombre}\n📝 Campo: *${campoElegido}*\n🔄 Nuevo: *${nuevoValor}*\n\nEscribe *sí* o *cancelar*`
        );
        return true;
    }

    if (state.stage === "editar_usuario_confirmar") {
        if (!/^(s[ií]|yes|ok|listo|confirmo)$/i.test(tn)) {
            await botResponder(phone, "Responde *sí* para confirmar o *cancelar* para salir."); return true;
        }
        const map = { rol:"role", nombre:"nombre", telefono:"phone" };
        await db.collection("user_profiles").doc(state.userId).set({ [map[state.campoElegido]]: state.nuevoValor }, { merge:true });
        await clearStaffState(phone);
        await botResponder(phone, `✅ *Actualizado*\n👤 ${state.userNombre}\n📝 ${state.campoElegido}: *${state.nuevoValor}*\n_Por ${nombre}_`);
        return true;
    }
    return false;
}

// ── Flujo: anular pago aplicado en cuenta de cliente ─────────────────────
async function manejarFlujoAnularPago(phone, texto, nombre, state) {
    const tn = texto.toLowerCase().trim();
    if (/^(cancelar|salir|cancel)$/i.test(tn)) {
        await clearStaffState(phone); await botResponder(phone, "❌ Anulación cancelada."); return true;
    }
    const fmt2 = (n) => new Intl.NumberFormat("es-CO",{style:"currency",currency:"COP",maximumFractionDigits:0}).format(n);
    const MN   = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];

    if (state.stage === "anular_pago_buscar") {
        const c = await buscarClienteEnDB(texto);
        if (!c) { await botResponder(phone, `🔍 No encontré "${texto}". Escribe nombre o teléfono, o *cancelar*.`); return true; }
        const lista = [];
        if (c.pagos) {
            for (const yr of Object.keys(c.pagos).sort((a,b)=>b-a)) {
                for (const mo of Object.keys(c.pagos[yr]).sort((a,b)=>b-a)) {
                    (c.pagos[yr][mo].payments||[]).forEach((p,i) => {
                        if (p.status!=="voided" && p.type!=="charge") {
                            const d = p.date ? new Date(p.date).toLocaleDateString("es-CO") : "S/F";
                            lista.push({ yr, mo, i, desc:`${MN[mo]} ${yr}: ${fmt2(p.amount)} el ${d}${p.method?" ("+p.method+")":""}` });
                        }
                    });
                    if (lista.length >= 12) break;
                }
                if (lista.length >= 12) break;
            }
        }
        if (!lista.length) { await clearStaffState(phone); await botResponder(phone, `${c.nombre} no tiene pagos para anular.`); return true; }
        await setStaffState(phone, { ...state, stage:"anular_pago_seleccionar", clienteId:c.id, clienteNombre:c.nombre, lista });
        await botResponder(phone, `💳 *Pagos de ${c.nombre}:*\n\n${lista.map((p,i)=>`${i+1}. ${p.desc}`).join("\n")}\n\nEscribe el *número* a anular, o *cancelar*.`);
        return true;
    }

    if (state.stage === "anular_pago_seleccionar") {
        const num = parseInt(texto.trim(), 10);
        if (isNaN(num)||num<1||num>state.lista.length) {
            await botResponder(phone, `Escribe un número del 1 al ${state.lista.length}, o *cancelar*.`); return true;
        }
        const elegido = state.lista[num-1];
        await setStaffState(phone, { ...state, stage:"anular_pago_confirmar", elegido });
        await botResponder(phone, `⚠️ *Confirma anulación:*\n\n👤 ${state.clienteNombre}\n💳 ${elegido.desc}\n\nEscribe *sí* para confirmar (irreversible) o *cancelar*.`);
        return true;
    }

    if (state.stage === "anular_pago_confirmar") {
        if (!/^(s[ií]|yes|ok|confirmo)$/i.test(tn)) {
            await botResponder(phone, "Responde *sí* para anular o *cancelar* para salir."); return true;
        }
        try {
            const { yr, mo, i } = state.elegido;
            const snap = await db.collection("clients").doc(state.clienteId).get();
            if (!snap.exists) throw new Error("Cliente no encontrado");
            const pagos = snap.data().pagos || {};
            if (pagos[yr]?.[mo]?.payments?.[i] !== undefined) {
                pagos[yr][mo].payments[i].status  = "voided";
                pagos[yr][mo].payments[i].voidedBy = nombre;
                pagos[yr][mo].payments[i].voidedAt = new Date().toISOString();
                await db.collection("clients").doc(state.clienteId).set({ pagos }, { merge:true });
            }
            await clearStaffState(phone);
            await botResponder(phone, `✅ Pago anulado.\n👤 ${state.clienteNombre}\n💳 ${state.elegido.desc}\n_Por ${nombre}_`);
        } catch(e) {
            await clearStaffState(phone);
            await botResponder(phone, `❌ Error: ${e.message}`);
        }
        return true;
    }
    return false;
}

// ── Flujo: aprobar/rechazar comprobante pendiente con confirmación ─────────
async function manejarFlujoAprobarPago(phone, texto, nombre, state) {
    const tn = texto.toLowerCase().trim();
    if (/^(cancelar|salir|cancel)$/i.test(tn)) {
        await clearStaffState(phone); await botResponder(phone, "❌ Operación cancelada."); return true;
    }
    const fmt2 = (n) => new Intl.NumberFormat("es-CO",{style:"currency",currency:"COP",maximumFractionDigits:0}).format(n);

    if (state.stage === "aprobar_pago_confirmar") {
        const esAprobar  = state.accionPago === "aprobar";
        const esSi       = /^(s[ií]|yes|ok|listo|confirmo)$/i.test(tn);
        if (!esSi) { await botResponder(phone, `Responde *sí* para ${esAprobar?"aprobar":"rechazar"} o *cancelar*.`); return true; }
        try {
            if (esAprobar) {
                const snap = await db.collection("pagos_preaprobados").doc(state.pagoId).get();
                if (!snap.exists) throw new Error("Pago no encontrado");
                await aprobarPagoPreaprobado(state.pagoId, snap.data(), nombre);
                await clearStaffState(phone);
                await botResponder(phone, `✅ Pago aprobado y registrado.\n👤 ${state.pagoNombre}\n💵 ${fmt2(state.pagoMonto)}\n_Por ${nombre}_`);
            } else {
                await db.collection("pagos_preaprobados").doc(state.pagoId).set({
                    status:"rejected", rejectedBy:nombre,
                    rejectedAt: admin.firestore.FieldValue.serverTimestamp()
                }, { merge:true });
                if (state.pagoPhone) {
                    botResponder(normalizePhone(state.pagoPhone),
                        `❌ *Comprobante rechazado*\n\nEl comprobante enviado no pudo ser aprobado.\nPor favor contáctanos para más información.`
                    ).catch(()=>{});
                }
                await clearStaffState(phone);
                await botResponder(phone, `❌ Pago rechazado.\n👤 ${state.pagoNombre}\n💵 ${fmt2(state.pagoMonto)}\n_Por ${nombre}_`);
            }
        } catch(e) {
            await clearStaffState(phone);
            await botResponder(phone, `❌ Error: ${e.message}`);
        }
        return true;
    }
    return false;
}

// ── Flujo: agregar barrio / empresa ───────────────────────────────────────
async function manejarFlujoAgregar(phone, texto, nombre, state) {
    const tn = texto.toLowerCase().trim();
    if (/^(cancelar|salir|cancel)$/i.test(tn)) {
        await clearStaffState(phone); await botResponder(phone, "❌ Cancelado."); return true;
    }

    if (state.stage === "agregar_barrio_confirmar") {
        if (!/^(s[ií]|yes|ok|listo)$/i.test(tn)) {
            await botResponder(phone, `Responde *sí* para agregar "${state.valor}" o *cancelar*.`); return true;
        }
        const ref  = db.collection("settings").doc("barrios");
        const snap = await ref.get();
        const lista = snap.exists ? (snap.data().lista||[]) : [];
        if (lista.map(b=>b.toLowerCase()).includes(state.valor.toLowerCase())) {
            await clearStaffState(phone);
            await botResponder(phone, `⚠️ El barrio "*${state.valor}*" ya existe en la lista.`); return true;
        }
        lista.push(state.valor);
        lista.sort();
        await ref.set({ lista }, { merge:true });
        await clearStaffState(phone);
        await botResponder(phone, `✅ Barrio "*${state.valor}*" agregado.\nTotal barrios: ${lista.length}\n_Por ${nombre}_`);
        return true;
    }

    if (state.stage === "agregar_empresa_nombre") {
        await setStaffState(phone, { ...state, stage:"agregar_empresa_confirmar", valor:texto.trim() });
        await botResponder(phone, `¿Confirmas agregar la empresa "*${texto.trim()}*"?\n\nEscribe *sí* o *cancelar*.`);
        return true;
    }

    if (state.stage === "agregar_empresa_confirmar") {
        if (!/^(s[ií]|yes|ok|listo)$/i.test(tn)) {
            await botResponder(phone, `Responde *sí* para confirmar o *cancelar*.`); return true;
        }
        const ref = await db.collection("empresas").add({
            nombre: state.valor,
            creadoPor: nombre,
            creadoEn: admin.firestore.FieldValue.serverTimestamp(),
        });
        await clearStaffState(phone);
        await botResponder(phone, `✅ Empresa "*${state.valor}*" creada.\nID: ${ref.id.slice(0,6).toUpperCase()}\n_Por ${nombre}_`);
        return true;
    }
    // ── Eliminar barrio / empresa ─────────────────────────────────────────────
    if (state.stage === "eliminar_barrio_confirmar") {
        if (!/^(s[ií]|yes|ok|listo)$/i.test(tn)) {
            await botResponder(phone, `Responde *sí* para eliminar "${state.barrio}" o *cancelar*.`); return true;
        }
        const nuevaLista = (state.lista||[]).filter((_,i)=>i!==state.idx);
        await db.collection("settings").doc("barrios").set({ lista:nuevaLista }, { merge:true });
        await clearStaffState(phone);
        await botResponder(phone, `✅ Barrio "*${state.barrio}*" eliminado.\nTotal: ${nuevaLista.length}\n_Por ${nombre}_`);
        return true;
    }
    if (state.stage === "eliminar_empresa_confirmar") {
        if (!/^(s[ií]|yes|ok|listo)$/i.test(tn)) {
            await botResponder(phone, `Responde *sí* para eliminar "${state.empresaNombre}" o *cancelar*.`); return true;
        }
        await db.collection("empresas").doc(state.empresaId).delete();
        await clearStaffState(phone);
        await botResponder(phone, `✅ Empresa "*${state.empresaNombre}*" eliminada.\n_Por ${nombre}_`);
        return true;
    }
    return false;
}

// ── Flujo: cambiar estado de cliente ──────────────────────────────────────
async function manejarFlujoCambiarEstado(phone, texto, nombre, state) {
    const tn = texto.toLowerCase().trim();
    if (/^(cancelar|salir|cancel)$/i.test(tn)) {
        await clearStaffState(phone); await botResponder(phone, "❌ Cancelado."); return true;
    }
    if (state.stage === "estado_confirmar") {
        if (!/^(s[ií]|yes|ok|listo|confirmo)$/i.test(tn)) {
            await botResponder(phone, "Responde *sí* para confirmar o *cancelar*."); return true;
        }
        await db.collection("clients").doc(state.clienteId).set({ estado:state.nuevoEstado }, { merge:true });
        await clearStaffState(phone);
        await botResponder(phone, `✅ *${state.clienteNombre}* → estado *${state.nuevoEstado}*\n_Por ${nombre}_`);
        return true;
    }
    return false;
}

// ── Flujo: registrar gasto ────────────────────────────────────────────────
async function manejarFlujoRegistrarGasto(phone, texto, nombre, state, receiptUrl = null) {
    const tn   = texto.toLowerCase().trim();
    const fmt2 = (n) => new Intl.NumberFormat("es-CO",{style:"currency",currency:"COP",maximumFractionDigits:0}).format(n);
    if (/^(cancelar|salir|cancel)$/i.test(tn)) {
        await clearStaffState(phone); await botResponder(phone, "❌ Gasto cancelado."); return true;
    }

    if (state.stage === "gasto_monto") {
        const monto = parseInt(texto.replace(/\D/g,""),10);
        if (!monto||monto<100) { await botResponder(phone,"Escribe un monto válido (ej: *50000*). O *cancelar*."); return true; }
        await setStaffState(phone, {...state, stage:"gasto_categoria", monto});
        const cats = state.tipo==="in"
            ? ["Instalación","Reconexión","Venta Extra","Otro"]
            : ["Operativo","Técnico","Servicios","Nómina","Arriendo","Transporte","Comunicaciones","Otro"];
        await botResponder(phone, `💵 Monto: *${fmt2(monto)}*\n\n¿Categoría?\n${cats.map(c=>`• ${c}`).join("\n")}\n\nO *cancelar*.`);
        return true;
    }

    if (state.stage === "gasto_categoria") {
        const cats = state.tipo==="in"
            ? ["Instalación","Reconexión","Venta Extra","Otro"]
            : ["Operativo","Técnico","Servicios","Nómina","Arriendo","Transporte","Comunicaciones","Otro"];
        const sin  = tn.normalize("NFD").replace(/[\u0300-\u036f]/g,"");
        const cat  = cats.find(c => sin.includes(c.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"")));
        if (!cat) { await botResponder(phone,`Elige una:\n${cats.map(c=>`• ${c}`).join("\n")}\n\nO *cancelar*.`); return true; }
        await setStaffState(phone, {...state, stage:"gasto_confirmar", categoria:cat});
        await botResponder(phone, `📋 *Confirma el ${state.tipo==="in"?"ingreso":"gasto"}:*\n\n💵 ${fmt2(state.monto)}\n🏷️ ${cat}\n📝 ${state.descripcion||cat}\n💳 ${state.metodo||"Efectivo"}\n\nEscribe *sí* o *cancelar*.`);
        return true;
    }

    if (state.stage === "gasto_confirmar") {
        if (!/^(s[ií]|yes|ok|listo|confirmo)$/i.test(tn)) {
            await botResponder(phone,"Responde *sí* para registrar o *cancelar*."); return true;
        }
        // Guardar ID del documento para luego adjuntar el recibo
        const docRef = await db.collection("expenses").add({
            type:        state.tipo||"out",
            amount:      state.monto,
            description: state.descripcion||state.categoria,
            category:    state.categoria,
            method:      state.metodo||"Efectivo",
            date:        new Date().toISOString(),
            user:        nombre,
            source:      "bot",
        });
        // Ir al estado de esperar recibo
        await setStaffState(phone, {
            ...state,
            stage:    "gasto_recibo",
            expenseId: docRef.id,
        });
        await botResponder(phone,
            `✅ *${state.tipo==="in"?"Ingreso":"Gasto"} registrado*\n💵 ${fmt2(state.monto)} | ${state.categoria}\n_Por ${nombre}_\n\n📸 ¿Tienes el recibo o foto de la factura? Envíala ahora.\nO escribe *omitir* si no tienes.`
        );
        return true;
    }

    if (state.stage === "gasto_recibo") {
        // Caso 1: llegó imagen (receiptUrl ya viene subido desde el handler principal)
        if (texto === "__imagen_recibo__") {
            if (receiptUrl) {
                await db.collection("expenses").doc(state.expenseId).update({ receiptUrl });
                await clearStaffState(phone);
                await botResponder(phone, `📎 Recibo guardado correctamente.\n_El comprobante quedó adjunto al gasto._`);
            } else {
                await botResponder(phone, "❌ No pude guardar la imagen. Intenta enviarla de nuevo o escribe *omitir*.");
            }
            return true;
        }
        // Caso 2: omitir
        if (/^(omitir|no|sin recibo|skip)$/i.test(tn)) {
            await clearStaffState(phone);
            await botResponder(phone, "👌 Gasto guardado sin recibo.");
            return true;
        }
        // Cualquier otro texto → recordar
        await botResponder(phone, "📸 Envía la *foto del recibo* o escribe *omitir* para continuar sin adjuntarlo.");
        return true;
    }

    return false;
}

// ── Flujo: crear cliente nuevo ────────────────────────────────────────────
async function manejarFlujoCrearCliente(phone, texto, nombre, state) {
    const tn   = texto.toLowerCase().trim();
    const fmt2 = (n) => new Intl.NumberFormat("es-CO",{style:"currency",currency:"COP",maximumFractionDigits:0}).format(n);
    if (/^(cancelar|salir|cancel)$/i.test(tn)) {
        await clearStaffState(phone); await botResponder(phone,"❌ Creación de cliente cancelada."); return true;
    }

    if (state.stage === "cliente_nombre") {
        if (texto.trim().length < 3) { await botResponder(phone,"Escribe el nombre completo del cliente. O *cancelar*."); return true; }
        await setStaffState(phone,{...state, stage:"cliente_telefono", clienteNombre:texto.trim()});
        await botResponder(phone,`📱 ¿Cuál es el teléfono de *${texto.trim()}*? (10 dígitos)\n\nO escribe *omitir* si no tiene. O *cancelar*.`);
        return true;
    }

    if (state.stage === "cliente_telefono") {
        const tel = texto.replace(/\D/g,"");
        if (tn!=="omitir" && tel.length < 10) { await botResponder(phone,"Teléfono inválido (10 dígitos). O escribe *omitir* si no tiene."); return true; }
        await setStaffState(phone,{...state, stage:"cliente_direccion", telefono: tn==="omitir"?"":tel});
        await botResponder(phone,"🏠 ¿Cuál es la dirección? O *omitir*. O *cancelar*.");
        return true;
    }

    if (state.stage === "cliente_direccion") {
        const dir = tn==="omitir" ? "" : texto.trim();
        await setStaffState(phone,{...state, stage:"cliente_confirmar", direccion:dir});
        await botResponder(phone,
            `📋 *Confirma el nuevo cliente:*\n\n👤 ${state.clienteNombre}\n📱 ${state.telefono||"Sin teléfono"}\n🏠 ${dir||"Sin dirección"}\n\nEscribe *sí* para crear o *cancelar*.`
        );
        return true;
    }

    if (state.stage === "cliente_confirmar") {
        if (!/^(s[ií]|yes|ok|listo|confirmo)$/i.test(tn)) {
            await botResponder(phone,"Responde *sí* para crear el cliente o *cancelar*."); return true;
        }
        const ref = await db.collection("clients").add({
            nombre:    state.clienteNombre,
            telefono:  state.telefono||"",
            direccion: state.direccion||"",
            estado:    "instalacion",
            pago:      0,
            pagos:     {},
            creadoPor: nombre,
            creadoEn:  admin.firestore.FieldValue.serverTimestamp(),
        });
        await clearStaffState(phone);
        await botResponder(phone,`✅ *Cliente creado*\n👤 ${state.clienteNombre}\n📱 ${state.telefono||"S/N"}\n🏠 ${state.direccion||"S/D"}\nID: ${ref.id.slice(0,6).toUpperCase()}\n_Por ${nombre}_`);
        return true;
    }
    return false;
}


async function manejarConsultaStaff(phone, texto, staffMember, receiptUrl = null) {
    const empresa  = process.env.NOMBRE_EMPRESA || "el ISP";
    const rol      = staffMember.role;
    const nombre   = staffMember.displayName || staffMember.name || "Staff";
    const rolLabel = rol === "cobrador" ? "cobrador" : rol === "admin" ? "administrador" : "técnico";

    // ── Flujos multi-paso (tienen prioridad) ──────────────────────────────────
    if (rol === "tecnico") {
        const handled = await manejarRespuestaTecnico(phone, texto, nombre);
        if (handled) return;
    }
    const staffState = await getStaffState(phone);
    const stage = staffState?.stage || "";
    if (stage.startsWith("pago_manual_"))      { if (await manejarFlujoPagoManual(phone, texto, nombre, staffState))     return; }
    if (stage.startsWith("editar_usuario_"))   { if (await manejarFlujoEditarUsuario(phone, texto, nombre, staffState))   return; }
    if (stage.startsWith("anular_pago_"))      { if (await manejarFlujoAnularPago(phone, texto, nombre, staffState))      return; }
    if (stage.startsWith("aprobar_pago_"))     { if (await manejarFlujoAprobarPago(phone, texto, nombre, staffState))     return; }
    if (stage.startsWith("agregar_") ||
        stage.startsWith("eliminar_"))         { if (await manejarFlujoAgregar(phone, texto, nombre, staffState))         return; }
    if (stage.startsWith("estado_"))           { if (await manejarFlujoCambiarEstado(phone, texto, nombre, staffState))   return; }
    if (stage.startsWith("gasto_"))            { if (await manejarFlujoRegistrarGasto(phone, texto, nombre, staffState, receiptUrl))  return; }
    if (stage.startsWith("cliente_"))          { if (await manejarFlujoCrearCliente(phone, texto, nombre, staffState))    return; }

    // ── Historial de conversación ─────────────────────────────────────────────
    const history = staffState?.conversationHistory || [];
    const historialStr = history.length > 0
        ? `\nConversación previa:\n${history.map(h => `${h.role === "user" ? nombre : "Bot"}: ${h.text}`).join("\n")}\n`
        : "";

    // ── Fase 1: Clasificar con Gemini ─────────────────────────────────────────
    const fmt = (n) => new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(n);
    let accion = "conversacion", buscar = "", zona = "", ordenar = "", pagoInfo = "", mensajeCliente = "", accionPago = "", nuevoValor = "";
    try {
        const model = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
        const result = await model.generateContent(
`Eres el clasificador de consultas internas de ${empresa}.
Empleado: ${nombre} (${rolLabel}).
${historialStr}
Mensaje actual: "${texto}"

Usa el historial para resolver referencias implícitas ("ese cliente", "las mismas", "y cuándo pagó?", "la que más debe", etc.).

ACCIONES:
- buscar_clientes: buscar/listar uno o varios clientes por nombre o teléfono; filtrar/ordenar grupo ya mencionado
- ver_historial: historial completo de pagos de UN cliente específico
- mora_global: estadísticas de mora de TODOS los clientes (sin filtro de grupo)
- al_dia_global: clientes al día de TODOS (sin filtro de grupo)
- ver_zona: clientes de una zona o barrio específico
- ver_tickets: tickets de soporte abiertos
- pagos_pendientes: listar comprobantes por revisar/aprobar
- ver_comprobante: ver la FOTO/IMAGEN que el cliente envió como comprobante de pago (captura de Nequi, transferencia, etc.). Usar cuando dicen "muéstrame el comprobante", "la foto que mandó", "el comprobante de Nequi".
- generar_recibo: generar y enviar el RECIBO DE PAGO oficial de la empresa (documento que confirma que el cliente pagó). Usar cuando dicen "recibo de pago", "genera el recibo", "manda el recibo", "generalo", "el recibo para el cliente".
- aprobar_pago: aprobar un comprobante pendiente y aplicarlo a la cuenta
- rechazar_pago: rechazar un comprobante pendiente
- eliminar_pago_pendiente: eliminar/borrar un comprobante pendiente de la lista (sin aplicar)
- anular_pago: anular/revertir un pago ya aplicado en la cuenta de un cliente
- registrar_pago: registrar pago en efectivo (sin comprobante)
- enviar_mensaje: enviar WhatsApp a un cliente
- ver_usuarios: listar usuarios del sistema (admins, cobradores, técnicos)
- editar_usuario: cambiar rol, nombre o teléfono de un usuario del sistema
- listar_barrios: ver los barrios/zonas registradas en el sistema
- agregar_barrio: agregar un nuevo barrio/zona al sistema
- listar_empresas: ver las empresas registradas
- agregar_empresa: agregar una nueva empresa al sistema
- cambiar_estado_cliente: cambiar estado de un cliente a activo/cortado/inactivo/retirado
- registrar_gasto: registrar un gasto o ingreso en el sistema
- ver_gastos: ver resumen de gastos del día/semana/mes
- listar_planes: ver planes de internet disponibles
- ver_instalaciones: ver órdenes de instalación pendientes o por técnico
- crear_cliente: crear/registrar un nuevo cliente en el sistema
- toggle_humanmode: pausar o reactivar el bot para UN cliente específico
- ver_caja: ver resumen de caja del día
- carga_tecnico: carga de trabajo de técnicos
- resumen: resumen general del día
- conversacion: saludo, pregunta general, o respuesta deducible del historial

PARÁMETROS (solo los que apliquen):
- "buscar": nombre parcial o teléfono. Si es follow-up de grupo previo, reutiliza el término del historial.
- "ordenar": "ultimo_pago" si pregunta quién pagó recientemente; "mora" si pregunta quién más debe; "" si no.
- "zona": barrio o sector.
- "pago_info": "NOMBRE|MONTO" para pago manual; "MONTO|CATEGORIA|DESCRIPCION|METODO|TIPO" para gasto (TIPO: out=egreso, in=ingreso; METODO: Efectivo/Nequi/Banco/Daviplata).
- "mensaje_cliente": "NOMBRE|TEXTO" para enviar mensaje.
- "accion_pago": "aprobar" o "rechazar".
- "nuevo_valor": estado nuevo para cambiar_estado_cliente (activo/cortado/inactivo/retirado); barrio/empresa a agregar o eliminar; "pausar"/"activar" para toggle_humanmode; "hoy"/"semana"/"mes" para ver_gastos; "pending"/"all" para ver_instalaciones.

REGLA CLAVE: follow-ups de grupo → buscar_clientes con mismo "buscar" del historial. NUNCA mora_global/al_dia_global para seguimientos de grupo.

Responde SOLO JSON válido: {"accion":"...","buscar":"...","ordenar":"...","zona":"...","pago_info":"...","mensaje_cliente":"...","accion_pago":"...","nuevo_valor":"..."}`
        );
        const raw = result.response.text().replace(/```json|```/gi, "").trim();
        const m   = raw.match(/\{[\s\S]*\}/);
        if (m) {
            const p       = JSON.parse(m[0]);
            accion         = p.accion          || "conversacion";
            buscar         = p.buscar          || "";
            ordenar        = p.ordenar         || "";
            zona           = p.zona            || "";
            pagoInfo       = p.pago_info       || "";
            mensajeCliente = p.mensaje_cliente || "";
            accionPago     = p.accion_pago     || "";
            nuevoValor     = p.nuevo_valor     || "";
        }
    } catch { /* accion=conversacion */ }

    // ── Fase 2: Obtener datos ──────────────────────────────────────────────────
    let datosContexto = "";
    try {
        switch (accion) {

            case "buscar_clientes": {
                if (!buscar) { datosContexto = "No se especificó el término de búsqueda."; break; }
                const resultados = await buscarClientesEnDB(buscar);
                if (resultados.length === 0) { datosContexto = `No hay clientes con "${buscar}" en la base de datos.`; break; }

                // Enriquecer con deuda y último pago
                const enriquecidos = resultados.map(c => {
                    const { totalDebt, monthsStr } = calcularDeudaCliente(c);
                    const up = ultimoPago(c);
                    return { ...c, totalDebt, monthsStr, up };
                });

                // Ordenar según pedido
                if (ordenar === "ultimo_pago") {
                    enriquecidos.sort((a, b) => b.up.ts - a.up.ts);
                } else if (ordenar === "mora" || ordenar === "deuda") {
                    enriquecidos.sort((a, b) => b.totalDebt - a.totalDebt);
                }

                const enMora = enriquecidos.filter(c => c.totalDebt > 0).length;
                const filas  = enriquecidos.slice(0, 15).map(c => {
                    const deudaStr = c.totalDebt > 0 ? `debe ${fmt(c.totalDebt)} (${c.monthsStr})` : "al día";
                    const pagoStr  = c.up.fecha
                        ? `último pago: ${c.up.fecha} — ${fmt(c.up.monto)}${c.up.metodo ? " vía " + c.up.metodo : ""}`
                        : "sin pagos registrados";
                    return `• ${c.nombre} | tel: ${c.telefono || "S/N"} | dir: ${c.direccion || "S/D"} | ${deudaStr} | ${pagoStr}`;
                });
                const extra = enriquecidos.length > 15 ? ` (y ${enriquecidos.length - 15} más)` : "";
                datosContexto = `Clientes con "${buscar}": ${enriquecidos.length} encontrados — ${enMora} en mora, ${enriquecidos.length - enMora} al día.\n${filas.join("\n")}${extra}`;
                break;
            }

            case "ver_historial": {
                const termino = buscar || pagoInfo;
                if (!termino) { datosContexto = "No se especificó el cliente."; break; }
                const c = await buscarClienteEnDB(termino);
                if (!c) { datosContexto = `No se encontró el cliente "${termino}".`; break; }
                const MESES_N = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];
                const lineas = []; let deudaTotal = 0;
                if (c.pagos) {
                    for (const año of Object.keys(c.pagos).sort((a, b) => b - a)) {
                        for (const mes of Object.keys(c.pagos[año]).sort((a, b) => b - a)) {
                            const m    = c.pagos[año][mes];
                            const deuda  = m.debt || c.pago || 0;
                            const abonos = (m.payments || []).filter(p => p.status !== "voided" && p.type !== "charge");
                            const pagado = abonos.reduce((s, p) => s + p.amount, 0);
                            const saldo  = deuda - pagado;
                            if (saldo > 0) deudaTotal += saldo;
                            const est = saldo <= 0 ? "pagado" : pagado > 0 ? `parcial (faltan ${fmt(saldo)})` : "sin pagar";
                            lineas.push(`${MESES_N[mes]} ${año}: ${est}`);
                            abonos.forEach(p => {
                                const d = p.date ? new Date(p.date).toLocaleDateString("es-CO") : "fecha N/A";
                                lineas.push(`  ↳ ${fmt(p.amount)} el ${d}${p.method ? ` (${p.method})` : ""}`);
                            });
                            if (lineas.length > 24) break;
                        }
                        if (lineas.length > 24) break;
                    }
                }
                const up = ultimoPago(c);
                datosContexto = `Historial de ${c.nombre} (tel: ${c.telefono || "S/N"}, dir: ${c.direccion || "S/D"}):\n${lineas.join("\n") || "Sin registros."}\nDeuda acumulada: ${deudaTotal > 0 ? fmt(deudaTotal) : "al día"}. Último pago: ${up.fecha ? `${up.fecha} — ${fmt(up.monto)}${up.metodo ? " vía " + up.metodo : ""}` : "ninguno registrado"}.`;
                break;
            }

            case "mora_global": {
                const snap = await db.collection("clients").get();
                let totalMora = 0, count = 0, criticos = 0;
                snap.forEach(d => {
                    const c = d.data(); let debt = 0;
                    if (c.pagos) Object.values(c.pagos).forEach(ms => Object.values(ms).forEach(m => {
                        const d2 = m.debt || c.pago || 0;
                        const paid = (m.payments||[]).filter(p=>p.status!=="voided"&&p.type!=="charge").reduce((s,p)=>s+p.amount,0);
                        if (paid < d2) debt += (d2 - paid);
                    }));
                    if (debt > 0) { count++; totalMora += debt; if (debt > (c.pago||0)*2) criticos++; }
                });
                datosContexto = `Mora general: ${count} de ${snap.size} clientes deben. Críticos (≥2 meses): ${criticos}. Cartera total: ${fmt(totalMora)}.`;
                break;
            }

            case "al_dia_global": {
                const snap = await db.collection("clients").get();
                let alDia = 0;
                snap.forEach(d => {
                    const c = d.data(); let debt = 0;
                    if (c.pagos) Object.values(c.pagos).forEach(ms => Object.values(ms).forEach(m => {
                        const d2 = m.debt || c.pago || 0;
                        const paid = (m.payments||[]).filter(p=>p.status!=="voided"&&p.type!=="charge").reduce((s,p)=>s+p.amount,0);
                        if (paid < d2) debt += (d2 - paid);
                    }));
                    if (debt === 0) alDia++;
                });
                datosContexto = `Clientes al día: ${alDia} de ${snap.size} (${snap.size > 0 ? Math.round(alDia/snap.size*100) : 0}%). El resto tiene deuda pendiente.`;
                break;
            }

            case "ver_zona": {
                if (!zona) { datosContexto = "No se especificó la zona o barrio."; break; }
                const snap  = await db.collection("clients").get();
                const zl    = zona.toLowerCase();
                const lista = snap.docs.map(d => ({id:d.id,...d.data()}))
                    .filter(c => (c.barrio||"").toLowerCase().includes(zl) || (c.direccion||"").toLowerCase().includes(zl));
                if (lista.length === 0) { datosContexto = `No se encontraron clientes en la zona "${zona}".`; break; }
                const enMora = lista.filter(c => calcularDeudaCliente(c).totalDebt > 0);
                const filas  = enMora.slice(0, 12).map(c => {
                    const { totalDebt } = calcularDeudaCliente(c);
                    const up = ultimoPago(c);
                    return `• ${c.nombre} | ${fmt(totalDebt)} | tel: ${c.telefono||"S/N"} | último pago: ${up.fecha||"nunca"}`;
                });
                datosContexto = `Zona "${zona}": ${lista.length} clientes en total, ${enMora.length} en mora.\n${filas.join("\n")||"Ninguno en mora."}${enMora.length>12?`\n(y ${enMora.length-12} más)`:""}`;
                break;
            }

            case "ver_tickets": {
                const snap    = await db.collection("support_tickets").where("estado","==","abierto").get();
                const tickets = snap.docs.map(d => d.data());
                const porTec  = {};
                tickets.forEach(t => { const n=t.tecnicoAsignado?.nombre||"Sin asignar"; porTec[n]=(porTec[n]||0)+1; });
                const desglose = Object.entries(porTec).sort((a,b)=>b[1]-a[1])
                    .map(([n,c])=>`${n}: ${c} ticket${c!==1?"s":""}`).join(", ");
                const recientes = tickets.slice(0,5).map(t=>`• [#${t.clienteId?.slice(0,5)||"?????"}] ${t.clienteNombre||"?"} — ${t.tipo||"??"} — ${t.prioridad||"media"}`).join("\n");
                datosContexto = `Tickets abiertos: ${tickets.length}. Por técnico: ${desglose||"sin datos"}.\nRecientes:\n${recientes||"ninguno"}`;
                break;
            }

            case "pagos_pendientes": {
                const snap  = await db.collection("pagos_preaprobados").where("status","==","pending").get();
                const pagos = snap.docs.map(d => d.data());
                const total = pagos.reduce((s,p)=>s+(p.extractedAmount||0),0);
                const lista = pagos.slice(0,8).map(p=>`${p.senderName||"?"}: ${fmt(p.extractedAmount||0)} vía ${p.bancoOrigen||"??"} (${p.date ? new Date(p.date).toLocaleDateString("es-CO") : ""})`).join("; ");
                datosContexto = pagos.length===0
                    ? "No hay pagos pendientes por aprobar."
                    : `Pendientes: ${pagos.length}. Total: ${fmt(total)}. Detalle: ${lista}.`;
                break;
            }

            case "registrar_pago": {
                if (pagoInfo && pagoInfo.includes("|")) {
                    const [nomP, montoP] = pagoInfo.split("|");
                    const monto = parseInt((montoP||"").replace(/\D/g,""),10);
                    if (nomP && monto >= 1000) {
                        const c = await buscarClienteEnDB(nomP.trim());
                        if (c) {
                            const { totalDebt } = calcularDeudaCliente(c);
                            await setStaffState(phone, { stage:"pago_manual_confirmar", clienteId:c.id, clienteNombre:c.nombre, clienteTelefono:c.telefono||"", deudaActual:totalDebt, monto });
                            await botResponder(phone, `📋 *Confirma el pago manual:*\n\n👤 ${c.nombre}\n💵 ${fmt(monto)}\n💰 Deuda actual: ${totalDebt>0?fmt(totalDebt):"Al día"}\n👷 Registrado por: ${nombre}\n\nEscribe *sí* o *cancelar*.`);
                            return;
                        }
                    }
                }
                const termPago = pagoInfo || buscar;
                if (termPago) {
                    const c = await buscarClienteEnDB(termPago);
                    if (c) {
                        const { totalDebt } = calcularDeudaCliente(c);
                        await setStaffState(phone, { stage:"pago_manual_monto", clienteId:c.id, clienteNombre:c.nombre, clienteTelefono:c.telefono||"", deudaActual:totalDebt });
                        await botResponder(phone, `👤 *${c.nombre}*\n💰 Deuda: ${totalDebt>0?fmt(totalDebt):"Al día ✓"}\n\n¿Cuánto vas a registrar? Escribe el monto.\n\nO escribe *cancelar* para salir.`);
                        return;
                    }
                }
                await setStaffState(phone, { stage:"pago_manual_cliente" });
                await botResponder(phone, "💵 ¿A qué cliente le vas a registrar el pago?\nEscribe el nombre o teléfono.\n\nO escribe *cancelar* para salir.");
                return;
            }

            case "enviar_mensaje": {
                if (!mensajeCliente) { datosContexto = "No se indicó a quién enviar ni el mensaje."; break; }
                const [nomCli, textoMsg] = mensajeCliente.includes("|")
                    ? mensajeCliente.split("|").map(s=>s.trim())
                    : [mensajeCliente, ""];
                if (!textoMsg) { datosContexto = `Falta el texto del mensaje para enviar a "${nomCli}".`; break; }
                const c = await buscarClienteEnDB(nomCli);
                if (!c)           { datosContexto = `No se encontró el cliente "${nomCli}".`; break; }
                if (!c.telefono)  { datosContexto = `${c.nombre} no tiene teléfono registrado.`; break; }
                await botResponder(normalizePhone(c.telefono), textoMsg);
                await db.collection("chats").doc(normalizePhone(c.telefono)).set({
                    lastMessage: textoMsg, lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
                    userName: c.nombre, phone: normalizePhone(c.telefono),
                }, { merge: true });
                datosContexto = `Mensaje enviado exitosamente a ${c.nombre} (${c.telefono}): "${textoMsg.slice(0,120)}".`;
                break;
            }

            case "carga_tecnico": {
                const [techSnap, tickSnap] = await Promise.all([
                    db.collection("user_profiles").where("role","==","tecnico").get(),
                    db.collection("support_tickets").where("estado","==","abierto").get(),
                ]);
                const carga = {};
                tickSnap.docs.forEach(d=>{const n=d.data().tecnicoAsignado?.nombre||"Sin asignar"; carga[n]=(carga[n]||0)+1;});
                const lista = techSnap.docs.map(d=>{const td=d.data(); const n=td.displayName||td.name||"Técnico"; return `${n}: ${carga[n]||0} ticket${(carga[n]||0)!==1?"s":""}`;});
                const sinAsg = carga["Sin asignar"]||0;
                datosContexto = `Carga de técnicos: ${lista.join(", ")||"sin técnicos"}${sinAsg>0?`. Sin asignar: ${sinAsg}`:""}. Total tickets abiertos: ${tickSnap.size}.`;
                break;
            }

            case "resumen": {
                const [cl, ti, pa] = await Promise.all([
                    db.collection("clients").get(),
                    db.collection("support_tickets").where("estado","==","abierto").get(),
                    db.collection("pagos_preaprobados").where("status","==","pending").get(),
                ]);
                let mora=0, cartera=0;
                cl.forEach(d=>{
                    const c=d.data(); let debt=0;
                    if(c.pagos) Object.values(c.pagos).forEach(ms=>Object.values(ms).forEach(m=>{
                        const d2=m.debt||c.pago||0;
                        const paid=(m.payments||[]).filter(p=>p.status!=="voided"&&p.type!=="charge").reduce((s,p)=>s+p.amount,0);
                        if(paid<d2) debt+=(d2-paid);
                    }));
                    if(debt>0){mora++;cartera+=debt;}
                });
                const hoy = new Date().toLocaleDateString("es-CO",{weekday:"long",day:"numeric",month:"long"});
                datosContexto = `Resumen ${hoy}: ${cl.size} clientes, ${mora} en mora, cartera ${fmt(cartera)}, ${ti.size} tickets abiertos, ${pa.size} pagos por aprobar.`;
                break;
            }

            // ─── Comprobante: foto Nequi/transferencia que envió el cliente ─────
            case "ver_comprobante": {
                const termComp = buscar || pagoInfo;
                if (!termComp) { datosContexto = "No se especificó de qué cliente ver el comprobante."; break; }
                const c = await buscarClienteEnDB(termComp);
                const snapPre = await db.collection("pagos_preaprobados")
                    .orderBy("date", "desc").limit(100).get();
                const allPre = snapPre.docs.map(d=>({id:d.id,...d.data()}));
                const matches = c
                    ? allPre.filter(p => p.clientId === c.id || fuzzyNombre(p.senderName||"", termComp))
                    : allPre.filter(p => fuzzyNombre(p.senderName||"", termComp));
                matches.sort((a,b) => (b.date||"").localeCompare(a.date||""));
                const enviados = [];
                for (const p of matches.slice(0,3)) {
                    if (p.imageUrl) {
                        const lbl = p.status==="pending"?"Pendiente":p.status==="approved"?"Aprobado":"Rechazado";
                        const cap = `Comprobante de ${p.senderName||"?"} — ${fmt(p.extractedAmount||0)}\n${p.bancoOrigen||""} | ${lbl}\n${p.date?new Date(p.date).toLocaleDateString("es-CO"):""}`;
                        await enviarImagen(phone, p.imageUrl, cap);
                        enviados.push(p.senderName||"?");
                    }
                }
                datosContexto = enviados.length
                    ? `Se enviaron ${enviados.length} comprobante(s) de pago de ${termComp}.`
                    : `No se encontraron fotos de comprobante de "${termComp}". Si quieres el recibo oficial usa "generar recibo".`;
                break;
            }

            // ─── Recibo: documento oficial generado por la empresa ────────────
            case "generar_recibo": {
                const termRec = buscar || pagoInfo;
                if (!termRec) { datosContexto = "No se especificó para qué cliente generar el recibo."; break; }
                const c = await buscarClienteEnDB(termRec);
                if (!c) { datosContexto = `No se encontró el cliente "${termRec}".`; break; }
                const up = ultimoPago(c);
                if (!up.monto) { datosContexto = `${c.nombre} no tiene pagos registrados para generar un recibo.`; break; }
                const MESES_N = ["Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];
                const dt = up.ts ? new Date(up.ts) : new Date();
                const imgBuf = generarReciboImagen({
                    clienteNombre:    c.nombre,
                    clienteDireccion: c.direccion || c.barrio || "",
                    monto:            up.monto,
                    fecha:            dt.toISOString(),
                    metodo:           up.metodo || "Efectivo",
                    mesPago:          `${MESES_N[dt.getMonth()]} ${dt.getFullYear()}`,
                    empresa:          process.env.NOMBRE_EMPRESA || "ISP",
                    cajero:           nombre,
                });
                await enviarImagen(phone, imgBuf, `Recibo de pago — ${c.nombre} | ${fmt(up.monto)}`);
                datosContexto = `Recibo generado y enviado para ${c.nombre} — ultimo pago: ${fmt(up.monto)} el ${up.fecha||"sin fecha"}.`;
                break;
            }

            // ─── Aprobar / rechazar comprobante pendiente ─────────────────────
            case "aprobar_pago":
            case "rechazar_pago": {
                const esAprobar = accion === "aprobar_pago";
                const termPago  = buscar || pagoInfo;
                if (!termPago) { datosContexto = `No se especificó a qué cliente ${esAprobar?"aprobar":"rechazar"} el pago.`; break; }
                const snapP = await db.collection("pagos_preaprobados")
                    .where("status","==","pending").orderBy("date","desc").limit(30).get();
                const lower2 = termPago.toLowerCase();
                const found  = snapP.docs.map(d=>({id:d.id,...d.data()}))
                    .find(p=>(p.senderName||"").toLowerCase().includes(lower2));
                if (!found) { datosContexto = `No hay comprobantes pendientes de "${termPago}".`; break; }
                // Iniciar flujo de confirmación
                await setStaffState(phone, {
                    stage:"aprobar_pago_confirmar", accionPago: esAprobar?"aprobar":"rechazar",
                    pagoId:found.id, pagoNombre:found.senderName||"?",
                    pagoMonto:found.extractedAmount||0, pagoPhone:found.senderPhone||"",
                    conversationHistory: staffState?.conversationHistory || [],
                });
                await botResponder(phone,
                    `${esAprobar?"✅":"❌"} *${esAprobar?"Aprobar":"Rechazar"} pago:*\n\n👤 ${found.senderName||"?"}\n💵 ${fmt(found.extractedAmount||0)}\n🏦 ${found.bancoOrigen||"?"}\n📅 ${found.date?new Date(found.date).toLocaleDateString("es-CO"):""}\n\nEscribe *sí* para confirmar o *cancelar* para salir.`
                );
                return;
            }

            // ─── Eliminar comprobante pendiente (sin aplicar) ─────────────────
            case "eliminar_pago_pendiente": {
                const termDel = buscar || pagoInfo;
                if (!termDel) { datosContexto = "No se especificó qué comprobante eliminar."; break; }
                const snapD = await db.collection("pagos_preaprobados")
                    .where("status","==","pending").orderBy("date","desc").limit(30).get();
                const foundD = snapD.docs.map(d=>({id:d.id,...d.data()}))
                    .find(p=>(p.senderName||"").toLowerCase().includes(termDel.toLowerCase()));
                if (!foundD) { datosContexto = `No hay comprobantes pendientes de "${termDel}".`; break; }
                await db.collection("pagos_preaprobados").doc(foundD.id).delete();
                datosContexto = `Comprobante de ${foundD.senderName||"?"} (${fmt(foundD.extractedAmount||0)}) eliminado de la lista de pendientes.`;
                break;
            }

            // ─── Anular pago aplicado (inicia flujo) ──────────────────────────
            case "anular_pago": {
                const termAnul = buscar || pagoInfo;
                await setStaffState(phone, {
                    stage:"anular_pago_buscar",
                    buscarInicial: termAnul,
                    conversationHistory: staffState?.conversationHistory || [],
                });
                if (termAnul) {
                    // Simular que ya escribió el nombre
                    const handled = await manejarFlujoAnularPago(phone, termAnul, nombre, { stage:"anular_pago_buscar", buscarInicial:termAnul });
                    if (handled) return;
                }
                await botResponder(phone, "¿A qué cliente quieres anular el pago?\nEscribe nombre o teléfono, o *cancelar* para salir.");
                return;
            }

            // ─── Ver usuarios del sistema ─────────────────────────────────────
            case "ver_usuarios": {
                const snap = await db.collection("user_profiles").get();
                const usuarios = snap.docs.map(d=>{
                    const u=d.data();
                    return `• ${u.nombre||u.displayName||d.id} | rol: ${u.role||"sin rol"} | tel: ${u.phone||u.telefono||"S/N"} | email: ${d.id}`;
                });
                datosContexto = `Usuarios del sistema (${snap.size}):\n${usuarios.join("\n")||"Sin usuarios registrados."}`;
                break;
            }

            // ─── Editar usuario (inicia flujo) ────────────────────────────────
            case "editar_usuario": {
                if (rol !== "admin") { datosContexto = "Solo los administradores pueden editar usuarios del sistema."; break; }
                await setStaffState(phone, {
                    stage:"editar_usuario_buscar",
                    conversationHistory: staffState?.conversationHistory || [],
                });
                const termEdit = buscar || nuevoValor;
                if (termEdit) {
                    const handled = await manejarFlujoEditarUsuario(phone, termEdit, nombre, { stage:"editar_usuario_buscar" });
                    if (handled) return;
                }
                await botResponder(phone, "¿A qué usuario quieres editar?\nEscribe el nombre o email, o *cancelar* para salir.");
                return;
            }

            // ─── Barrios ──────────────────────────────────────────────────────
            case "listar_barrios": {
                const snap = await db.collection("settings").doc("barrios").get();
                const lista = snap.exists ? (snap.data().lista||[]) : [];
                datosContexto = lista.length
                    ? `Barrios registrados (${lista.length}): ${lista.join(", ")}.`
                    : "No hay barrios registrados aún.";
                break;
            }

            case "agregar_barrio": {
                if (rol !== "admin") { datosContexto = "Solo los administradores pueden agregar barrios."; break; }
                const barrio = nuevoValor || buscar;
                if (!barrio) { await botResponder(phone, "¿Cómo se llama el barrio o zona que quieres agregar?\n\nO *cancelar* para salir."); await setStaffState(phone, { stage:"agregar_barrio_confirmar", valor:"", conversationHistory:staffState?.conversationHistory||[] }); return; }
                await setStaffState(phone, { stage:"agregar_barrio_confirmar", valor:barrio, conversationHistory:staffState?.conversationHistory||[] });
                await botResponder(phone, `¿Confirmas agregar el barrio "*${barrio}*"?\n\nEscribe *sí* o *cancelar*.`);
                return;
            }

            // ─── Empresas ─────────────────────────────────────────────────────
            case "listar_empresas": {
                const snap = await db.collection("empresas").get();
                const lista = snap.docs.map(d=>`• ${d.data().nombre||d.id}`);
                datosContexto = lista.length
                    ? `Empresas registradas (${snap.size}):\n${lista.join("\n")}`
                    : "No hay empresas registradas aún.";
                break;
            }

            case "agregar_empresa": {
                if (rol !== "admin") { datosContexto = "Solo los administradores pueden agregar empresas."; break; }
                const empresa2 = nuevoValor || buscar;
                if (!empresa2) { await botResponder(phone, "¿Cómo se llama la empresa que quieres agregar?\n\nO *cancelar* para salir."); await setStaffState(phone, { stage:"agregar_empresa_nombre", conversationHistory:staffState?.conversationHistory||[] }); return; }
                await setStaffState(phone, { stage:"agregar_empresa_confirmar", valor:empresa2, conversationHistory:staffState?.conversationHistory||[] });
                await botResponder(phone, `¿Confirmas agregar la empresa "*${empresa2}*"?\n\nEscribe *sí* o *cancelar*.`);
                return;
            }

            // ── Cambiar estado cliente ────────────────────────────────────────
            case "cambiar_estado_cliente": {
                const estadosOk = ["activo","cortado","inactivo","retirado","instalacion"];
                if (!buscar) { datosContexto = "No se especificó el cliente a modificar."; break; }
                const estNuevo = (nuevoValor||"").toLowerCase();
                if (!estadosOk.includes(estNuevo)) { datosContexto = `Estado inválido. Válidos: activo, cortado, inactivo, retirado.`; break; }
                const cEst = await buscarClienteEnDB(buscar);
                if (!cEst) { datosContexto = `No se encontró el cliente "${buscar}".`; break; }
                await setStaffState(phone, { stage:"estado_confirmar", clienteId:cEst.id, clienteNombre:cEst.nombre, estadoActual:cEst.estado||"activo", nuevoEstado:estNuevo, conversationHistory:staffState?.conversationHistory||[] });
                await botResponder(phone, `📋 *Confirma cambio de estado:*\n\n👤 ${cEst.nombre}\n🏷️ Actual: *${cEst.estado||"activo"}*\n🔄 Nuevo: *${estNuevo}*\n\nEscribe *sí* o *cancelar*.`);
                return;
            }

            // ── Registrar gasto / ingreso ─────────────────────────────────────
            case "registrar_gasto": {
                const fmt3 = (n)=>new Intl.NumberFormat("es-CO",{style:"currency",currency:"COP",maximumFractionDigits:0}).format(n);
                let gMonto=0, gCat="", gDesc="", gMet="Efectivo", gTipo="out";
                if (pagoInfo) {
                    const pp = pagoInfo.split("|");
                    gMonto = parseInt((pp[0]||"").replace(/\D/g,""),10)||0;
                    gCat   = pp[1]||""; gDesc = pp[2]||gCat; gMet = pp[3]||"Efectivo"; gTipo = pp[4]||"out";
                }
                if (!gMonto||gMonto<100) {
                    await setStaffState(phone,{stage:"gasto_monto", tipo:gTipo, descripcion:gDesc, metodo:gMet, conversationHistory:staffState?.conversationHistory||[]});
                    await botResponder(phone,"💵 ¿Cuánto es el gasto? Escribe el monto (ej: *50000*).\nO *cancelar*."); return;
                }
                if (!gCat) {
                    await setStaffState(phone,{stage:"gasto_categoria", monto:gMonto, tipo:gTipo, descripcion:gDesc, metodo:gMet, conversationHistory:staffState?.conversationHistory||[]});
                    const cats = gTipo==="in" ? ["Instalación","Reconexión","Venta Extra","Otro"] : ["Operativo","Técnico","Servicios","Nómina","Arriendo","Transporte","Comunicaciones","Otro"];
                    await botResponder(phone, `💵 ${fmt3(gMonto)}\n\n¿Categoría?\n${cats.map(c=>`• ${c}`).join("\n")}\n\nO *cancelar*.`); return;
                }
                // Directo a confirmar
                await setStaffState(phone,{stage:"gasto_confirmar", monto:gMonto, categoria:gCat, descripcion:gDesc, metodo:gMet, tipo:gTipo, conversationHistory:staffState?.conversationHistory||[]});
                await botResponder(phone, `📋 *Confirma el ${gTipo==="in"?"ingreso":"gasto"}:*\n\n💵 ${fmt3(gMonto)}\n🏷️ ${gCat}\n📝 ${gDesc||gCat}\n💳 ${gMet}\n\nEscribe *sí* o *cancelar*.`);
                return;
            }

            // ── Ver gastos ────────────────────────────────────────────────────
            case "ver_gastos": {
                const periodo = (nuevoValor||"hoy").toLowerCase();
                const desde   = new Date();
                if (periodo.includes("semana")) desde.setDate(desde.getDate()-7);
                else if (periodo.includes("mes")) desde.setMonth(desde.getMonth()-1);
                else desde.setHours(0,0,0,0);
                const snapG = await db.collection("expenses").where("date",">=",desde.toISOString()).get();
                const gastos = snapG.docs.map(d=>d.data());
                const egreso  = gastos.filter(g=>g.type!=="in").reduce((s,g)=>s+(g.amount||0),0);
                const ingreso = gastos.filter(g=>g.type==="in").reduce((s,g)=>s+(g.amount||0),0);
                const porCat  = {};
                gastos.filter(g=>g.type!=="in").forEach(g=>{ const c=g.category||"Otro"; porCat[c]=(porCat[c]||0)+(g.amount||0); });
                const desglose = Object.entries(porCat).sort((a,b)=>b[1]-a[1]).map(([c,v])=>`${c}: ${fmt(v)}`).join(", ");
                datosContexto = `Gastos ${periodo} (${gastos.length} registros): Egresos ${fmt(egreso)}, Ingresos adicionales ${fmt(ingreso)}, Balance ${fmt(ingreso-egreso)}. Desglose egresos: ${desglose||"sin datos"}.`;
                break;
            }

            // ── Ver caja ──────────────────────────────────────────────────────
            case "ver_caja": {
                const inicio = new Date(); inicio.setHours(0,0,0,0);
                const [expSnap, pagSnap] = await Promise.all([
                    db.collection("expenses").where("date",">=",inicio.toISOString()).get(),
                    db.collection("pagos_preaprobados").where("status","==","approved").orderBy("approvedAt","desc").limit(30).get(),
                ]);
                const gastos  = expSnap.docs.map(d=>d.data());
                const egreso  = gastos.filter(g=>g.type!=="in").reduce((s,g)=>s+(g.amount||0),0);
                const ingreso = gastos.filter(g=>g.type==="in").reduce((s,g)=>s+(g.amount||0),0);
                // Pagos aprobados hoy
                const hoy = new Date().toLocaleDateString("es-CO");
                const pagosHoy = pagSnap.docs.filter(d => {
                    const at = d.data().approvedAt?.toDate?.();
                    return at && at.toLocaleDateString("es-CO") === hoy;
                });
                const totalPagos = pagosHoy.reduce((s,d)=>s+(d.data().extractedAmount||0),0);
                datosContexto = `Caja hoy: Pagos aprobados ${fmt(totalPagos)} (${pagosHoy.length} transacciones). Ingresos extra ${fmt(ingreso)}, Egresos ${fmt(egreso)}, Efectivo neto aprox. ${fmt(totalPagos+ingreso-egreso)}.`;
                break;
            }

            // ── Listar planes ─────────────────────────────────────────────────
            case "listar_planes": {
                const snapPl = await db.collection("planes").get();
                if (snapPl.empty) { datosContexto = "No hay planes registrados. Se crean desde Configuración → Catálogos."; break; }
                const planes = snapPl.docs.map(d=>{ const p=d.data(); return `• ${p.nombre||d.id}: ${p.valor?fmt(p.valor):"precio no definido"}`; });
                datosContexto = `Planes disponibles (${snapPl.size}):\n${planes.join("\n")}`;
                break;
            }

            // ── Ver instalaciones ─────────────────────────────────────────────
            case "ver_instalaciones": {
                let q = db.collection("installations");
                const filtroStatus = nuevoValor==="all" ? null : "pending";
                if (filtroStatus) q = q.where("status","==",filtroStatus);
                const snapI = await q.orderBy("createdAt","desc").limit(30).get();
                let insts = snapI.docs.map(d=>({id:d.id,...d.data()}));
                if (buscar) {
                    const bl = buscar.toLowerCase();
                    insts = insts.filter(i=>(i.technician||i.clientName||"").toLowerCase().includes(bl));
                }
                if (!insts.length) { datosContexto = `No hay instalaciones ${filtroStatus||""}${buscar?` de "${buscar}"`:""}. `; break; }
                const filas = insts.slice(0,10).map(i=>`• ${i.clientName||"?"} | ${i.clientAddress||"S/D"} | Tec: ${i.technician||"Sin asignar"} | ${i.type||"instalacion"} | ${i.status||"?"}`);
                datosContexto = `Instalaciones${buscar?` de "${buscar}"`:""}${filtroStatus?" pendientes":""} (${insts.length}):\n${filas.join("\n")}${insts.length>10?`\n(y ${insts.length-10} más)`:""}`;
                break;
            }

            // ── Crear cliente ─────────────────────────────────────────────────
            case "crear_cliente": {
                await setStaffState(phone,{stage:"cliente_nombre", conversationHistory:staffState?.conversationHistory||[]});
                const inicialNombre = buscar || nuevoValor;
                if (inicialNombre) {
                    if (await manejarFlujoCrearCliente(phone, inicialNombre, nombre, {stage:"cliente_nombre"})) return;
                }
                await botResponder(phone,"👤 ¿Cómo se llama el nuevo cliente?\nEscribe el nombre completo. O *cancelar*.");
                return;
            }

            // ── Pausar / reactivar bot para un cliente ────────────────────────
            case "toggle_humanmode": {
                if (!buscar) { datosContexto = "No se especificó el cliente."; break; }
                const cHM = await buscarClienteEnDB(buscar);
                if (!cHM) { datosContexto = `No se encontró el cliente "${buscar}".`; break; }
                const telHM = normalizePhone(cHM.telefono||"");
                if (!telHM) { datosContexto = `${cHM.nombre} no tiene teléfono registrado.`; break; }
                const esActivar = (nuevoValor||"").toLowerCase().includes("activ")||
                                  (nuevoValor||"").toLowerCase().includes("bot")||
                                  (nuevoValor||"").toLowerCase().includes("resum");
                const nuevoModo = !esActivar; // humanMode=true → bot pausado
                await db.collection("chats").doc(telHM).set({ humanMode:nuevoModo }, { merge:true });
                datosContexto = `Bot ${nuevoModo?"pausado (atención humana activada)":"reactivado"} para ${cHM.nombre} (${telHM}).`;
                break;
            }

            // ── Eliminar barrio ───────────────────────────────────────────────
            case "eliminar_barrio": {
                if (rol!=="admin") { datosContexto="Solo admins pueden eliminar barrios."; break; }
                const termB = buscar||nuevoValor;
                if (!termB) { datosContexto="No se indicó qué barrio eliminar."; break; }
                const refB  = db.collection("settings").doc("barrios");
                const snapB = await refB.get();
                const listaB = snapB.exists?(snapB.data().lista||[]):[];
                const idxB   = listaB.findIndex(b=>b.toLowerCase().includes(termB.toLowerCase()));
                if (idxB===-1) { datosContexto=`No se encontró el barrio "${termB}".`; break; }
                await setStaffState(phone,{stage:"eliminar_barrio_confirmar", barrio:listaB[idxB], lista:listaB, idx:idxB, conversationHistory:staffState?.conversationHistory||[]});
                await botResponder(phone,`⚠️ ¿Confirmas eliminar el barrio "*${listaB[idxB]}*"?\n\nEscribe *sí* o *cancelar*.`);
                return;
            }

            // ── Eliminar empresa ──────────────────────────────────────────────
            case "eliminar_empresa": {
                if (rol!=="admin") { datosContexto="Solo admins pueden eliminar empresas."; break; }
                const termE = buscar||nuevoValor;
                if (!termE) { datosContexto="No se indicó qué empresa eliminar."; break; }
                const snapE = await db.collection("empresas").get();
                const foundE = snapE.docs.find(d=>(d.data().nombre||"").toLowerCase().includes(termE.toLowerCase()));
                if (!foundE) { datosContexto=`No se encontró la empresa "${termE}".`; break; }
                await setStaffState(phone,{stage:"eliminar_empresa_confirmar", empresaId:foundE.id, empresaNombre:foundE.data().nombre||foundE.id, conversationHistory:staffState?.conversationHistory||[]});
                await botResponder(phone,`⚠️ ¿Confirmas eliminar la empresa "*${foundE.data().nombre||foundE.id}*"?\n\nEscribe *sí* o *cancelar*.`);
                return;
            }

            default: {
                datosContexto = history.length > 0
                    ? "Responde basándote en el historial de conversación. Si no hay datos suficientes, pregunta qué necesita."
                    : `Saluda a ${nombre} y dile que puede consultarte sobre clientes, cobros, pagos, comprobantes, usuarios, barrios, empresas, tickets, gastos, instalaciones y más.`;
                break;
            }
        }
    } catch (e) {
        console.error("[STAFF] Error obteniendo datos:", e.message);
        datosContexto = "Hubo un error consultando el sistema. Intenta de nuevo en un momento.";
    }

    const respuesta = await generarRespuestaNatural(texto, datosContexto, nombre, rolLabel, empresa, history);
    await botResponder(phone, respuesta);
    await appendConversationHistory(phone, texto, respuesta).catch(() => {});
}

// ==========================================
// ESTADO DE CONVERSACIÓN — STAFF
// Persiste en Firestore: staff_states/{phone}
// ==========================================
async function getStaffState(phone) {
    const snap = await db.collection("staff_states").doc(phone).get();
    if (!snap.exists) return null;
    const data = snap.data();
    // Expirar estados viejos (+24h)
    if (data.expiresAt && data.expiresAt.toMillis() < Date.now()) {
        await db.collection("staff_states").doc(phone).delete();
        return null;
    }
    return data;
}

async function setStaffState(phone, state) {
    await db.collection("staff_states").doc(phone).set({
        ...state,
        expiresAt: admin.firestore.Timestamp.fromMillis(Date.now() + 24 * 60 * 60 * 1000),
    });
}

async function clearStaffState(phone) {
    await db.collection("staff_states").doc(phone).delete().catch(() => {});
}

// ==========================================
// RECORDATORIO DE TICKETS — TÉCNICOS
// Envía a cada técnico sus tickets abiertos
// y pide confirmación de estado uno a uno.
// ==========================================
async function recordarTecnicos() {
    console.log("[RECORDATORIO] Iniciando recordatorio de tickets a técnicos...");
    try {
        const [techSnap, ticketSnap] = await Promise.all([
            db.collection("user_profiles").where("role", "==", "tecnico").get(),
            db.collection("support_tickets").where("estado", "==", "abierto").get(),
        ]);

        if (techSnap.empty || ticketSnap.empty) return;

        const empresa = process.env.NOMBRE_EMPRESA || "el ISP";

        // Agrupar tickets por técnico (por id del técnico asignado)
        const ticketsPorTecnico = {};
        ticketSnap.docs.forEach(d => {
            const t = { id: d.id, ...d.data() };
            const tid = t.tecnicoAsignado?.id;
            if (!tid) return;
            if (!ticketsPorTecnico[tid]) ticketsPorTecnico[tid] = [];
            ticketsPorTecnico[tid].push(t);
        });

        for (const techDoc of techSnap.docs) {
            const tec = { id: techDoc.id, ...techDoc.data() };
            const phone = normalizePhone(tec.phone || tec.telefono || "");
            if (!phone || phone.length < 10) continue;

            const misTickets = (ticketsPorTecnico[tec.id] || [])
                .sort((a, b) => {
                    const ta = a.fechaCreacion?.toMillis?.() || 0;
                    const tb = b.fechaCreacion?.toMillis?.() || 0;
                    return ta - tb; // más antiguos primero
                });

            if (misTickets.length === 0) continue;

            // Verificar que no tenga ya un estado activo (no interrumpir flujo en curso)
            const existing = await getStaffState(phone);
            if (existing) continue;

            const nombre = tec.displayName || tec.name || "Técnico";
            const listaStr = misTickets.slice(0, 5).map((t, i) => {
                const dias = t.fechaCreacion
                    ? Math.floor((Date.now() - t.fechaCreacion.toMillis()) / 86400000)
                    : "?";
                return `  ${i + 1}. [#${t.id.slice(0, 5).toUpperCase()}] *${t.clienteNombre || "Cliente"}*\n     📍 ${t.clienteDireccion || "Sin dirección"}\n     🕐 Hace ${dias} día${dias !== 1 ? "s" : ""}`;
            }).join("\n\n");


            // Guardar estado: cola de tickets a revisar
            const pendingIds = misTickets.map(t => t.id);
            await setStaffState(phone, {
                stage: "tecnico_confirmar",
                ticketId: pendingIds[0],
                ticketDesc: misTickets[0].descripcion || "",
                clienteNombre: misTickets[0].clienteNombre || "",
                clienteTelefono: misTickets[0].clienteTelefono || "",
                pendingIds,
            });

            await botResponder(phone,
                `🔔 *Recordatorio de tickets — ${empresa}*\n\nHola *${nombre}*, tienes *${misTickets.length}* ticket${misTickets.length !== 1 ? "s" : ""} abierto${misTickets.length !== 1 ? "s" : ""}:\n\n${listaStr}\n\nEmpecemos por el más antiguo:\n\n🎫 Ticket *#${pendingIds[0].slice(0, 5).toUpperCase()}*\nCliente: *${misTickets[0].clienteNombre || "Desconocido"}*\nProblema: _"${(misTickets[0].descripcion || "").slice(0, 120)}"_\n\n¿Ya lo resolviste? Responde *sí* o *no*`
            );
        }
        console.log("[RECORDATORIO] Completado.");
    } catch (e) {
        console.error("[RECORDATORIO] Error:", e.message);
    }
}

// Maneja la respuesta del técnico cuando tiene un estado de seguimiento activo
async function manejarRespuestaTecnico(phone, texto, nombre) {
    const state = await getStaffState(phone);
    if (!state || !state.stage?.startsWith("tecnico_")) return false; // no hay estado activo

    const textoNorm = texto.toLowerCase().trim();
    const fmt = (id) => id.slice(0, 5).toUpperCase();

    // ── Etapa: esperando sí/no ────────────────────────────────────────────────
    if (state.stage === "tecnico_confirmar") {
        const esSi  = /^(s[ií]|yes|resuelto|listo|ok|ya|arregl)/i.test(textoNorm);
        const esNo  = /^(no|todav|pend|aún|aun|sin|falt)/i.test(textoNorm);

        if (esSi) {
            await setStaffState(phone, { ...state, stage: "tecnico_descripcion" });
            await botResponder(phone,
                `✅ ¡Excelente! ¿Cómo lo resolviste?\n\nEscribe una breve descripción de la solución para registrarla en el ticket *#${fmt(state.ticketId)}* (cliente: ${state.clienteNombre}).`
            );
            return true;
        }

        if (esNo) {
            await setStaffState(phone, { ...state, stage: "tecnico_motivo" });
            await botResponder(phone,
                `⏳ Entendido. ¿Por qué sigue pendiente el ticket *#${fmt(state.ticketId)}*?\n\nEscribe el motivo brevemente (ej: "esperando repuesto", "cliente no estaba", etc.)`
            );
            return true;
        }

        // Respuesta no reconocida
        await botResponder(phone, `No entendí tu respuesta. Para el ticket *#${fmt(state.ticketId)}* (${state.clienteNombre}) responde *sí* si ya lo resolviste o *no* si está pendiente.`);
        return true;
    }

    // ── Etapa: escribió la descripción de resolución ──────────────────────────
    if (state.stage === "tecnico_descripcion") {
        if (texto.length < 5) {
            await botResponder(phone, "Por favor escribe una descripción más completa de cómo resolviste el caso.");
            return true;
        }
        // Cerrar ticket en Firestore
        try {
            await db.collection("support_tickets").doc(state.ticketId).set({
                estado: "cerrado",
                resolucion: texto.trim(),
                resolvedBy: nombre,
                fechaCierre: admin.firestore.FieldValue.serverTimestamp(),
            }, { merge: true });

            // Notificar al cliente si tiene teléfono
            if (state.clienteTelefono) {
                const clientePhone = normalizePhone(state.clienteTelefono);
                botResponder(clientePhone,
                    `✅ *Caso resuelto*\n\nHola *${state.clienteNombre}*, tu reporte fue atendido:\n\n_"${state.ticketDesc.slice(0, 100)}"_\n\n📝 Resolución: ${texto.trim()}\n\nSi tienes otro problema, no dudes en escribirnos.`
                ).catch(() => {});
            }
        } catch (e) {
            console.error("[TECNICO] Error cerrando ticket:", e.message);
        }

        // ¿Hay más tickets en la cola?
        const remaining = (state.pendingIds || []).slice(1);
        if (remaining.length > 0) {
            // Cargar siguiente ticket
            const nextSnap = await db.collection("support_tickets").doc(remaining[0]).get();
            const next = nextSnap.exists ? { id: nextSnap.id, ...nextSnap.data() } : null;
            if (next && next.estado === "abierto") {
                await setStaffState(phone, {
                    stage: "tecnico_confirmar",
                    ticketId: remaining[0],
                    ticketDesc: next.descripcion || "",
                    clienteNombre: next.clienteNombre || "",
                    clienteTelefono: next.clienteTelefono || "",
                    pendingIds: remaining,
                });
                await botResponder(phone,
                    `✅ Ticket *#${fmt(state.ticketId)}* cerrado. ¡Gracias!\n\nSiguiente:\n\n🎫 Ticket *#${fmt(remaining[0])}*\nCliente: *${next.clienteNombre || "Desconocido"}*\nProblema: _"${(next.descripcion || "").slice(0, 120)}"_\n\n¿Ya lo resolviste? Responde *sí* o *no*`
                );
            } else {
                await clearStaffState(phone);
                await botResponder(phone, `✅ Ticket *#${fmt(state.ticketId)}* cerrado. ¡Todos tus tickets al día! 🎉`);
            }
        } else {
            await clearStaffState(phone);
            await botResponder(phone, `✅ Ticket *#${fmt(state.ticketId)}* cerrado. ¡No tienes más tickets pendientes! 🎉`);
        }
        return true;
    }

    // ── Etapa: escribió el motivo de por qué no resolvió ─────────────────────
    if (state.stage === "tecnico_motivo") {
        if (texto.length < 3) {
            await botResponder(phone, "Escribe el motivo por el que no has podido resolver el caso.");
            return true;
        }
        // Guardar comentario en el ticket
        try {
            await db.collection("support_tickets").doc(state.ticketId).set({
                comentarioTecnico: texto.trim(),
                ultimaActualizacion: admin.firestore.FieldValue.serverTimestamp(),
            }, { merge: true });
        } catch (e) { /* no bloquear */ }

        // ¿Hay más tickets?
        const remaining = (state.pendingIds || []).slice(1);
        if (remaining.length > 0) {
            const nextSnap = await db.collection("support_tickets").doc(remaining[0]).get();
            const next = nextSnap.exists ? { id: nextSnap.id, ...nextSnap.data() } : null;
            if (next && next.estado === "abierto") {
                await setStaffState(phone, {
                    stage: "tecnico_confirmar",
                    ticketId: remaining[0],
                    ticketDesc: next.descripcion || "",
                    clienteNombre: next.clienteNombre || "",
                    clienteTelefono: next.clienteTelefono || "",
                    pendingIds: remaining,
                });
                await botResponder(phone,
                    `📝 Registrado. Siguiente ticket:\n\n🎫 Ticket *#${fmt(remaining[0])}*\nCliente: *${next.clienteNombre || "Desconocido"}*\nProblema: _"${(next.descripcion || "").slice(0, 120)}"_\n\n¿Ya lo resolviste? Responde *sí* o *no*`
                );
            } else {
                await clearStaffState(phone);
                await botResponder(phone, `📝 Motivo registrado. ¡Ya revisamos todos tus tickets! 👍`);
            }
        } else {
            await clearStaffState(phone);
            await botResponder(phone, `📝 Motivo registrado. No tienes más tickets pendientes por revisar. 👍`);
        }
        return true;
    }

    return false; // estado no reconocido
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

        const verifyBlock = nequiEsperado ? `
VERIFICACIÓN DEL DESTINATARIO (CRÍTICO):
El número al que DEBE estar dirigido el pago es: ${nequiEsperado}
Busca en la imagen el número, celular, llave o cuenta del DESTINATARIO/RECEPTOR.
Puede aparecer como: "Para:", "Transferiste a:", "Enviaste a:", "Destinatario:", "Llave Bre-B:", "Número celular:", "Cuenta destino:", o simplemente el número del receptor.
- Si lo encuentras Y coincide con ${nequiEsperado} (o termina igual en los últimos 7 dígitos) → numero_correcto: true
- Si lo encuentras Y NO coincide con ${nequiEsperado} → numero_correcto: false
- Si el comprobante no muestra claramente el destinatario → numero_correcto: null` : "";

        const prompt = `Eres un experto en comprobantes de transferencias y pagos colombianos. Analiza esta imagen con máxima precisión.

TIPOS DE COMPROBANTE ACEPTADOS:
- Nequi (app morada de Bancolombia)
- Bre-B / Breb (sistema de pagos instantáneos del Banco de la República, opera entre todos los bancos colombianos)
- Transferencia bancaria desde cualquier banco colombiano (Bancolombia, Davivienda, BBVA, Banco de Bogotá, Falabella, Itaú, etc.)
- Daviplata
- PSE
- Cualquier otro medio de pago colombiano

PASO 1 — ¿Es un comprobante de pago/transferencia?
- es_recibo: true si la imagen muestra evidencia de una transferencia o pago realizado.
- es_nequi: true SOLO si hay logo o texto "Nequi" visible. Para Bre-B u otros bancos es false.

PASO 2 — Extrae TODOS los datos del comprobante:
- valor: monto en pesos colombianos (número entero sin símbolos, 0 si no se lee).
- fecha: fecha de la transacción en formato YYYY-MM-DD.
- hora: hora de la transacción en formato HH:MM (24h), cadena vacía si no aparece.
- banco_origen: nombre del banco o app desde donde se hizo el pago. Ejemplos: "Nequi", "Bancolombia", "Daviplata", "Davivienda", "BBVA", "Banco de Bogotá", "Bre-B", "Falabella", "Itaú", etc. Si no se identifica, devuelve "Desconocido".
- nombre_remitente: nombre completo de quien ENVIÓ el dinero, tal como aparece en el comprobante (campo "De:", "Remitente:", "Nombre:", o el nombre que aparece asociado al pagador). Cadena vacía si no aparece.
- numero_destino: número celular o llave del RECEPTOR/DESTINATARIO (solo dígitos, sin espacios). NO el del remitente.
- referencia: ID, código o referencia de la transacción.
- confianza: 0-100, qué tan seguro estás de que es un comprobante real y legible.
${verifyBlock}

Responde SOLO con JSON sin texto adicional ni bloques de código:
{"es_recibo":boolean,"es_nequi":boolean,"valor":number,"fecha":"YYYY-MM-DD","hora":"HH:MM","banco_origen":"string","nombre_remitente":"string","numero_destino":"string","referencia":"string","confianza":number,"numero_correcto":boolean|null}`;

        const result = await model.generateContent([
            prompt,
            { inlineData: { data: buffer.toString("base64"), mimeType: "image/jpeg" } },
        ]);
        const raw   = result.response.text().replace(/```json|```/gi, "").trim();
        const match = raw.match(/\{[\s\S]*\}/);
        const parsed = match ? JSON.parse(match[0]) : null;
        if (!parsed) return { es_recibo: false, es_nequi: false, valor: 0, fecha: null, hora: "", banco_origen: "Desconocido", nombre_remitente: "", numero_destino: "", referencia: "", confianza: 0, numero_correcto: null };
        // Fallback: si Gemini no devolvió numero_correcto, comparar manualmente
        if (parsed.numero_correcto === undefined || parsed.numero_correcto === null) {
            parsed.numero_correcto = null;
            if (nequiEsperado && parsed.numero_destino) {
                const d = parsed.numero_destino.replace(/\D/g, "");
                const n = nequiEsperado.replace(/\D/g, "");
                if (d.length >= 7 && n.length >= 7) {
                    parsed.numero_correcto = d.slice(-7) === n.slice(-7);
                }
            }
        }
        return parsed;
    } catch {
        return { es_recibo: false, es_nequi: false, valor: 0, fecha: null, hora: "", banco_origen: "Desconocido", nombre_remitente: "", numero_destino: "", referencia: "", confianza: 0, numero_correcto: null };
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
            : Buffer.isBuffer(imageSource)
                ? { image: imageSource, caption }
                : { image: imageSource, caption };
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
app.get("/status", (_req, res) => {
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
app.get("/qr", auth, async (_req, res) => {
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

// ── POST /recordarTecnicos ────────────────────────────────────────────────────
// Dispara el recordatorio manual (también se llama automáticamente cada día).
app.post("/recordarTecnicos", auth, async (_req, res) => {
    try {
        await recordarTecnicos();
        res.json({ success: true });
    } catch (e) {
        console.error("/recordarTecnicos error:", e.message);
        res.status(500).json({ success: false, error: e.message });
    }
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

// ── POST /confirmarPago ────────────────────────────────────────────────────────
// Envía el comprobante de pago al cliente como confirmación de aprobación.
// Recibe: { telefono, imageUrl, caption, nombreCliente }
app.post("/confirmarPago", auth, async (req, res) => {
    const { telefono, imageUrl, caption, nombreCliente } = req.body;
    if (!telefono || !imageUrl) return res.status(400).json({ error: "Faltan datos" });

    const texto = caption || `✅ *Pago aprobado*\n\nHola ${nombreCliente || ""}, tu comprobante fue verificado y el pago ha sido registrado correctamente. ¡Gracias! 🙏`;

    // 1. Enviar imagen con caption por WhatsApp
    const msgId = await enviarImagen(telefono, imageUrl, texto);

    // 2. Guardar en Firestore (mensaje saliente con la imagen)
    const jid = normalizePhone(telefono);
    const chatRef = db.collection("chats").doc(jid);
    const msgPayload = {
        type: "image",
        text: { body: texto },
        imageUrl,
        mediaUrl: imageUrl,
        direction: "out",
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        ack: 1,
        ...(msgId && { waMessageId: msgId }),
    };
    await Promise.all([
        chatRef.set({
            lastMessage: "📷 Comprobante aprobado",
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            ...(nombreCliente && { userName: nombreCliente }),
            phone: jid,
        }, { merge: true }),
        chatRef.collection("messages").add(msgPayload),
    ]);

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

    // Recordatorio diario a técnicos: cada 24h a partir del arranque.
    // Para enviar en un horario fijo (ej. 8am) usar una librería de cron.
    const RECORDATORIO_INTERVALO = 24 * 60 * 60 * 1000; // 24 horas
    setInterval(() => {
        recordarTecnicos().catch(e => console.error("[RECORDATORIO CRON]", e.message));
    }, RECORDATORIO_INTERVALO);
    console.log("⏰ Recordatorio automático de tickets configurado (cada 24h)");
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
