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
        const texto = (message.message?.conversation || message.message?.extendedTextMessage?.text || "").trim();
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
// ==========================================
async function manejarConsultaStaff(phone, texto, staffMember) {
    const empresa = process.env.NOMBRE_EMPRESA || "el ISP";
    const rol     = staffMember.role;
    const nombre  = staffMember.displayName || staffMember.name || "Staff";

    // ── Técnico: manejar flujo de seguimiento de tickets primero ─────────────
    if (rol === "tecnico") {
        const handled = await manejarRespuestaTecnico(phone, texto, nombre);
        if (handled) return;
    }

    // ── Clasificar intención con Gemini ──────────────────────────────────────
    let intent = "otro";
    let parametro = "";
    try {
        const model  = genAI.getGenerativeModel({ model: CONFIG.GEMINI_MODEL });
        const result = await model.generateContent(
            `Eres el clasificador de consultas internas de ${empresa}.
Un empleado (rol: ${rol}) envió: "${texto}"

Clasifica en UNO de estos intents:
- clientes_mora: cuántos/quiénes deben, mora, deudas
- tickets_abiertos: tickets, casos, soporte abierto, técnicos
- pagos_pendientes: comprobantes por aprobar, pagos en espera
- buscar_cliente: busca a un cliente específico por nombre o teléfono (info general, estado de cuenta, deuda, saldo)
- historial_pagos: pagos, abonos, comprobantes o historial de pagos de un cliente específico
- tecnico_carga: carga de trabajo por técnico, quién tiene más tickets
- resumen: resumen general, estadísticas, cómo vamos hoy
- otro: cualquier otra cosa

IMPORTANTE: "estado de cuenta de X", "cuánto debe X", "deuda de X", "busca a X" → buscar_cliente con parametro = nombre exacto de X.
Si el intent es buscar_cliente o historial_pagos, extrae el nombre o teléfono buscado en "parametro". El parametro debe ser SOLO el nombre del cliente, sin frases como "de", "para", etc.

Responde SOLO JSON: {"intent":"<valor>","parametro":"<cadena o vacío>"}`
        );
        const raw = result.response.text().replace(/```json|```/gi, "").trim();
        const m   = raw.match(/\{[\s\S]*\}/);
        if (m) { const p = JSON.parse(m[0]); intent = p.intent || "otro"; parametro = p.parametro || ""; }
    } catch { /* usa intent=otro */ }

    const fmt = (n) => new Intl.NumberFormat("es-CO", { style: "currency", currency: "COP", maximumFractionDigits: 0 }).format(n);

    // ── Ejecutar según intent ────────────────────────────────────────────────
    try {
        switch (intent) {

            case "clientes_mora": {
                const snap = await db.collection("clients").get();
                let totalMora = 0, count = 0, criticos = 0;
                snap.forEach(d => {
                    const c = d.data();
                    let debt = 0;
                    if (c.pagos) {
                        Object.entries(c.pagos).forEach(([, meses]) => {
                            Object.values(meses).forEach(m => {
                                const d2 = m.debt || c.pago || 0;
                                const paid = (m.payments || []).filter(p => p.status !== "voided" && p.type !== "charge").reduce((s, p) => s + p.amount, 0);
                                if (paid < d2) { debt += (d2 - paid); }
                            });
                        });
                    }
                    if (debt > 0) { count++; totalMora += debt; if (debt > (c.pago || 0) * 2) criticos++; }
                });
                await botResponder(phone,
                    `📊 *Clientes en mora — ${empresa}*\n\n👥 Total en mora: *${count}*\n🔴 Mora crítica (+2 meses): *${criticos}*\n💰 Cartera total: *${fmt(totalMora)}*\n\nPara ver un cliente específico escribe su nombre o teléfono.`
                );
                break;
            }

            case "tickets_abiertos": {
                const snap = await db.collection("support_tickets").where("estado", "==", "abierto").get();
                const tickets = snap.docs.map(d => d.data());
                const porTecnico = {};
                tickets.forEach(t => {
                    const tec = t.tecnicoAsignado?.nombre || "Sin asignar";
                    porTecnico[tec] = (porTecnico[tec] || 0) + 1;
                });
                const ranking = Object.entries(porTecnico).sort((a, b) => b[1] - a[1]).slice(0, 5);
                const rankStr = ranking.map(([n, c]) => `  • ${n}: ${c} ticket${c !== 1 ? "s" : ""}`).join("\n");
                await botResponder(phone,
                    `🎫 *Tickets abiertos — ${empresa}*\n\nTotal: *${tickets.length}*\n\n👷 Por técnico:\n${rankStr || "  (Sin datos)"}\n\nEscribe "detalle tickets" para ver la lista completa.`
                );
                break;
            }

            case "pagos_pendientes": {
                const snap = await db.collection("pagos_preaprobados").where("status", "==", "pending").get();
                const pagos = snap.docs.map(d => d.data());
                const totalMonto = pagos.reduce((s, p) => s + (p.extractedAmount || 0), 0);
                const lista = pagos.slice(0, 5).map(p => `  • ${p.senderName}: ${fmt(p.extractedAmount)}`).join("\n");
                await botResponder(phone,
                    `💳 *Comprobantes por aprobar — ${empresa}*\n\nPendientes: *${pagos.length}*\nMonto total: *${fmt(totalMonto)}*\n\nÚltimos recibidos:\n${lista || "  (Ninguno)"}`
                );
                break;
            }

            case "buscar_cliente": {
                if (!parametro) { await botResponder(phone, "¿A quién buscas? Escribe el nombre o teléfono del cliente."); break; }
                const termLower = parametro.toLowerCase();
                const termDigits = parametro.replace(/\D/g, "");
                const snap = await db.collection("clients").get();
                const matches = snap.docs.map(d => ({ id: d.id, ...d.data() })).filter(c =>
                    (c.nombre && c.nombre.toLowerCase().includes(termLower)) ||
                    (termDigits.length >= 6 && c.telefono && c.telefono.replace(/\D/g,"").includes(termDigits))
                ).slice(0, 3);
                if (matches.length === 0) {
                    await botResponder(phone, `🔍 No encontré clientes que coincidan con "*${parametro}*".`);
                } else {
                    const info = matches.map(c => {
                        const { totalDebt } = calcularDeudaCliente(c);
                        return `👤 *${c.nombre}*\n  📱 ${c.telefono || "S/N"}\n  💰 Deuda: ${totalDebt > 0 ? fmt(totalDebt) : "Al día ✓"}\n  📍 ${c.direccion || "Sin dirección"}`;
                    }).join("\n\n");
                    await botResponder(phone, `🔍 *Resultado para "${parametro}":*\n\n${info}`);
                }
                break;
            }

            case "tecnico_carga": {
                const [techSnap, tickSnap] = await Promise.all([
                    db.collection("user_profiles").where("role", "==", "tecnico").get(),
                    db.collection("support_tickets").where("estado", "==", "abierto").get(),
                ]);
                const tickets = tickSnap.docs.map(d => d.data());
                const carga = {};
                tickets.forEach(t => { const n = t.tecnicoAsignado?.nombre || "Sin asignar"; carga[n] = (carga[n] || 0) + 1; });
                const tecnicos = techSnap.docs.map(d => {
                    const td = d.data();
                    const n  = td.displayName || td.name || "Técnico";
                    return `  • ${n}: *${carga[n] || 0}* ticket${carga[n] !== 1 ? "s" : ""}`;
                });
                const sinAsignar = carga["Sin asignar"] || 0;
                await botResponder(phone,
                    `👷 *Carga de técnicos — ${empresa}*\n\n${tecnicos.join("\n") || "  Sin técnicos registrados"}\n${sinAsignar > 0 ? `\n⚠️ Sin asignar: *${sinAsignar}*` : ""}`
                );
                break;
            }

            case "resumen": {
                const [clientsSnap, ticketsSnap, pagosSnap] = await Promise.all([
                    db.collection("clients").get(),
                    db.collection("support_tickets").where("estado", "==", "abierto").get(),
                    db.collection("pagos_preaprobados").where("status", "==", "pending").get(),
                ]);
                let mora = 0;
                clientsSnap.forEach(d => {
                    const c = d.data(); let debt = 0;
                    if (c.pagos) Object.values(c.pagos).forEach(meses => Object.values(meses).forEach(m => {
                        const d2 = m.debt || c.pago || 0;
                        const paid = (m.payments || []).filter(p => p.status !== "voided" && p.type !== "charge").reduce((s, p) => s + p.amount, 0);
                        if (paid < d2) debt += (d2 - paid);
                    }));
                    if (debt > 0) mora++;
                });
                const fecha = new Date().toLocaleDateString("es-CO", { weekday:"long", day:"numeric", month:"long" });
                await botResponder(phone,
                    `📈 *Resumen ${empresa}*\n_${fecha}_\n\n👥 Clientes totales: *${clientsSnap.size}*\n🔴 En mora: *${mora}*\n🎫 Tickets abiertos: *${ticketsSnap.size}*\n💳 Pagos por aprobar: *${pagosSnap.size}*\n\nHola ${nombre} 👋 ¿En qué más te ayudo?`
                );
                break;
            }

            case "historial_pagos": {
                if (!parametro) {
                    await botResponder(phone, "¿De qué cliente quieres ver los pagos? Escribe su nombre o teléfono.");
                    break;
                }
                const termLower2 = parametro.toLowerCase();
                const termDigits2 = parametro.replace(/\D/g, "");
                const snapH = await db.collection("clients").get();
                const allClientsH = snapH.docs.map(d => ({ id: d.id, ...d.data() }));
                // Buscar coincidencia exacta primero, luego parcial
                const clienteH =
                    allClientsH.find(c => c.nombre && c.nombre.toLowerCase() === termLower2) ||
                    allClientsH.find(c =>
                        (c.nombre && c.nombre.toLowerCase().includes(termLower2)) ||
                        (termDigits2.length >= 6 && c.telefono && c.telefono.replace(/\D/g, "").includes(termDigits2))
                    );
                if (!clienteH) {
                    await botResponder(phone, `🔍 No encontré un cliente con "*${parametro}*".`);
                    break;
                }
                const MESES_N = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];
                const pagosLineas = [];
                let deudaTotal = 0;
                if (clienteH.pagos) {
                    const años = Object.keys(clienteH.pagos).sort((a, b) => b - a);
                    for (const año of años) {
                        const meses = Object.keys(clienteH.pagos[año]).sort((a, b) => b - a);
                        for (const mes of meses) {
                            const m = clienteH.pagos[año][mes];
                            const deuda = m.debt || clienteH.pago || 0;
                            const abonos = (m.payments || []).filter(p => p.status !== "voided" && p.type !== "charge");
                            const pagado = abonos.reduce((s, p) => s + p.amount, 0);
                            const saldo = deuda - pagado;
                            if (saldo > 0) deudaTotal += saldo;
                            const estadoStr = saldo <= 0 ? "✅ Pagado" : pagado > 0 ? `⚠️ Parcial (falta ${fmt(saldo)})` : `🔴 Sin pagar`;
                            pagosLineas.push(`  • *${MESES_N[mes]} ${año}*: ${estadoStr}`);
                            abonos.forEach(p => {
                                const d = p.date ? new Date(p.date).toLocaleDateString("es-CO") : "";
                                pagosLineas.push(`    ↳ ${fmt(p.amount)} el ${d}${p.method ? ` (${p.method})` : ""}`);
                            });
                            if (pagosLineas.length > 20) break; // limitar salida
                        }
                        if (pagosLineas.length > 20) break;
                    }
                }
                const resumenPagos = pagosLineas.length > 0
                    ? pagosLineas.join("\n")
                    : "  Sin registros de pago.";
                await botResponder(phone,
                    `💳 *Historial de pagos — ${clienteH.nombre}*\n📱 ${clienteH.telefono || "S/N"}\n📍 ${clienteH.direccion || "Sin dirección"}\n\n${resumenPagos}\n\n💰 *Deuda actual: ${deudaTotal > 0 ? fmt(deudaTotal) : "Al día ✓"}*`
                );
                break;
            }

            default:
                await botResponder(phone,
                    `Hola ${nombre} 👋\n\nPuedes consultarme:\n• *clientes en mora*\n• *tickets abiertos*\n• *pagos pendientes*\n• *carga de técnicos*\n• *buscar [nombre cliente]*\n• *pagos de [nombre cliente]*\n• *resumen*`
                );
        }
    } catch (e) {
        console.error("[STAFF] Error en consulta:", e.message);
        await botResponder(phone, "❌ Error al consultar el sistema. Intenta de nuevo.");
    }
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
