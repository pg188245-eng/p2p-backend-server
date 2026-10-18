const { cert, initializeApp } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');
const { getMessaging } = require('firebase-admin/messaging');

const DATABASE_URL = process.env.FIREBASE_DATABASE_URL ||
  'https://pairlink2-default-rtdb.asia-southeast1.firebasedatabase.app';
const MAX_PUSH_AGE_MS = 14 * 24 * 60 * 60 * 1000;
const INVALID_TOKEN_CODES = new Set([
  'messaging/registration-token-not-registered',
  'messaging/invalid-registration-token'
]);

let db;
let messaging;

function logPushError(label, error) {
  console.error(`[PairLink push] ${label}:`, error?.message || error);
}

function readServiceAccount() {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (error) {
    throw new Error(`FIREBASE_SERVICE_ACCOUNT_JSON must contain the full service-account JSON: ${error.message}`);
  }
}

function safeKey(value) {
  return [...String(value || '')].map((character) => '.#$[]/'.includes(character) ? '_' : character).join('').slice(0, 180);
}

function compactPhoto(...candidates) {
  for (const value of candidates) {
    const photo = String(value || '');
    if (/^data:image\/(?:jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(photo) && photo.length <= 2600) return photo;
  }
  return '';
}

async function markProcessed(ref) {
  try {
    await ref.transaction((current) => {
      if (!current || current.pushSentAt) return;
      return { ...current, pushSentAt: Date.now() };
    }, undefined, false);
  } catch (error) {
    logPushError('could not mark push event as processed', error);
  }
}

async function sendPushToUser(userId, payload, options = {}) {
  const userRef = db.ref(`users/${userId}`);
  const [tokensSnapshot, devicesSnapshot] = await Promise.all([
    userRef.child('pushTokens').get(),
    userRef.child('devices').get()
  ]);
  const devices = devicesSnapshot.val() || {};
  const tokenRecords = tokensSnapshot.val() || {};
  const entries = Object.entries(tokenRecords).filter(([, record]) => {
    if (typeof record?.token !== 'string' || !record.token) return false;
    const deviceId = safeKey(record.deviceId || '');
    return !deviceId || devices[deviceId]?.online !== true;
  });
  if (!entries.length) {
    console.info(`[PairLink push] no offline registered token (${options.kind || 'notification'})`);
    return 0;
  }

  let sent = 0;
  const invalidTokenUpdates = {};
  // FCM multicast accepts at most 500 registration tokens per request.
  for (let start = 0; start < entries.length; start += 500) {
    const batch = entries.slice(start, start + 500);
    const result = await messaging.sendEachForMulticast({
      tokens: batch.map(([, record]) => record.token),
      data: Object.fromEntries(Object.entries(payload).map(([key, value]) => [key, String(value ?? '')])),
      android: {
        priority: 'high',
        ttl: options.ttlMs || 60_000,
        ...(options.collapseKey ? { collapseKey: options.collapseKey } : {})
      },
      webpush: {
        headers: { TTL: String(Math.ceil((options.ttlMs || 60_000) / 1000)) },
        fcmOptions: { link: 'https://pairlink2.web.app/' }
      }
    });
    sent += result.successCount;
    result.responses.forEach((response, index) => {
      if (!response.success && INVALID_TOKEN_CODES.has(response.error?.code)) {
        invalidTokenUpdates[batch[index][0]] = null;
      }
    });
    const failures = result.responses.filter((response) => !response.success);
    for (const failure of failures.slice(0, 3)) {
      logPushError(`FCM send failed (${failure.error?.code || 'unknown'})`, failure.error);
    }
  }
  if (Object.keys(invalidTokenUpdates).length) {
    await userRef.child('pushTokens').update(invalidTokenUpdates).catch((error) => logPushError('could not remove invalid tokens', error));
  }
  console.info(`[PairLink push] ${options.kind || 'notification'} send completed; accepted=${sent}, targets=${entries.length}`);
  return sent;
}

async function getPushProfile(userId) {
  return (await db.ref(`users/${userId}/pushProfile`).get()).val() || {};
}

async function processInvite(recipientId, snapshot) {
  const invite = snapshot.val();
  if (!invite || invite.pushSentAt || invite.to !== recipientId || !invite.from || !invite.roomId) return;
  const action = String(invite.action || '');
  const ageMs = Date.now() - Number(invite.createdAt || 0);
  if (!Number.isFinite(ageMs) || ageMs < -60_000) return;

  if (action === 'call' || action === 'video') {
    if (ageMs > 60_000) return;
    const profile = await getPushProfile(invite.from);
    const sent = await sendPushToUser(recipientId, {
      kind: 'incoming_call',
      callId: invite.callId || snapshot.key,
      roomId: invite.roomId,
      callerId: invite.from,
      callerName: profile.name || invite.fromName || 'PairLink user',
      callerPhoto: compactPhoto(profile.photoDataUrl, invite.fromPhotoDataUrl),
      video: action === 'video' ? 'true' : 'false'
    }, { ttlMs: 45_000, collapseKey: `call-${invite.callId || snapshot.key}`, kind: 'call' });
    if (sent) await markProcessed(snapshot.ref);
    return;
  }

  if (action === 'chat-message-queued' && ageMs <= MAX_PUSH_AGE_MS) {
    const profile = await getPushProfile(invite.from);
    const sent = await sendPushToUser(recipientId, {
      kind: 'incoming_message',
      roomId: invite.roomId,
      senderId: invite.from,
      senderName: invite.fromName || profile.name || 'PairLink user',
      senderPhoto: compactPhoto(profile.photoDataUrl, invite.fromPhotoDataUrl),
      encrypted: 'true',
      queued: 'true'
    }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'queued-message-alert' });
    if (sent) await markProcessed(snapshot.ref);
  }
}

async function processPendingMessage(recipientId, snapshot) {
  const message = snapshot.val();
  if (!message || message.pushSentAt || !message.from || message.to !== recipientId) return;
  const ageMs = Date.now() - Number(message.sentAt || message.createdAt || 0);
  if (Number.isFinite(ageMs) && ageMs > MAX_PUSH_AGE_MS) return;
  const profile = await getPushProfile(message.from);
  const sent = await sendPushToUser(recipientId, {
    kind: 'incoming_message',
    roomId: message.roomId || '',
    messageId: message.id || snapshot.key,
    senderId: message.from,
    senderName: message.fromName || profile.name || 'PairLink user',
    senderPhoto: compactPhoto(profile.photoDataUrl),
    encrypted: 'true'
  }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'encrypted-message-alert' });
  if (sent) await markProcessed(snapshot.ref);
}

async function processRoomEvent(roomId, snapshot) {
  const event = snapshot.val();
  if (!event || event.event !== 'chat-message' || !event.sender || event.pushSentAt) return;
  const ageMs = Date.now() - Number(event.ts || 0);
  if (!Number.isFinite(ageMs) || ageMs < -60_000 || ageMs > MAX_PUSH_AGE_MS) return;
  const recipientId = event.recipientId || (await db.ref(`users/${event.sender}/pairRooms/${roomId}`).get()).val();
  if (!recipientId || recipientId === event.sender) return;
  const profile = await getPushProfile(event.sender);
  const sent = await sendPushToUser(recipientId, {
    kind: 'incoming_message',
    roomId,
    messageId: event.payload?.id || snapshot.key,
    senderId: event.sender,
    senderName: profile.name || 'PairLink user',
    senderPhoto: compactPhoto(profile.photoDataUrl),
    encrypted: 'true'
  }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'encrypted-message-alert' });
  if (sent) await markProcessed(snapshot.ref);
}

function listenForChildAdded(ref, label, handler) {
  ref.on('child_added', (snapshot) => {
    Promise.resolve(handler(snapshot)).catch((error) => logPushError(`${label} handler failed`, error));
  }, (error) => logPushError(`${label} listener failed`, error));
}

function startRenderPushRelay() {
  try {
    const serviceAccount = readServiceAccount();
    if (!serviceAccount) {
      console.warn('[PairLink push] disabled: add FIREBASE_SERVICE_ACCOUNT_JSON to Render environment variables');
      return false;
    }
    if (serviceAccount.project_id !== 'pairlink2') {
      throw new Error('service-account project_id must be pairlink2');
    }
    const app = initializeApp({ credential: cert(serviceAccount), databaseURL: DATABASE_URL }, 'pairlink-render-push');
    db = getDatabase(app);
    messaging = getMessaging(app);

    listenForChildAdded(db.ref('users'), 'user index', (userSnapshot) => {
      const userId = userSnapshot.key;
      if (!userId) return;
      listenForChildAdded(db.ref(`users/${userId}/pairInvites`), `invites ${userId}`, (snapshot) => processInvite(userId, snapshot));
      listenForChildAdded(db.ref(`users/${userId}/pendingMessages`), `pending messages ${userId}`, (snapshot) => processPendingMessage(userId, snapshot));
    });
    listenForChildAdded(db.ref('rooms'), 'room index', (roomSnapshot) => {
      const roomId = roomSnapshot.key;
      if (!roomId) return;
      listenForChildAdded(db.ref(`rooms/${roomId}/events`), `room events ${roomId}`, (snapshot) => processRoomEvent(roomId, snapshot));
    });
    console.info('[PairLink push] Render Firebase event relay started.');
    return true;
  } catch (error) {
    logPushError('could not start Firebase event relay', error);
    return false;
  }
}

module.exports = { startRenderPushRelay };
