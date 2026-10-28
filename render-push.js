const crypto = require('node:crypto');
const { createHash, randomUUID } = crypto;
const { cert, initializeApp } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');
const { getFirestore } = require('firebase-admin/firestore');
const { getMessaging } = require('firebase-admin/messaging');

const DATABASE_URL = process.env.FIREBASE_DATABASE_URL ||
  'https://pairlink2-default-rtdb.asia-southeast1.firebasedatabase.app';
const MAX_PUSH_AGE_MS = 14 * 24 * 60 * 60 * 1000;
const DISPATCH_CLAIM_LEASE_MS = 2 * 60 * 1000;
const INVALID_TOKEN_CODES = new Set([
  'messaging/registration-token-not-registered',
  'messaging/invalid-registration-token'
]);

let db;
let firestore;
let messaging;
const liveCallFallbacks = new Set();
const inMemoryCallPushDispatches = new Map();
const missedCallDispatchBusy = new Set();

function base64UrlDecode(value) {
  const normalized = String(value || '').replace(/-/g, '+').replace(/_/g, '/');
  return Buffer.from(normalized + '='.repeat((4 - normalized.length % 4) % 4), 'base64');
}

function base64UrlEncode(value) {
  return Buffer.from(value).toString('base64').replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
}

function hkdfExtract(salt, input) {
  return crypto.createHmac('sha256', salt).update(input).digest();
}

function hkdfExpand(prk, info, length) {
  const chunks = [];
  let previous = Buffer.alloc(0);
  for (let counter = 1, size = 0; size < length; counter++) {
    previous = crypto.createHmac('sha256', prk).update(previous).update(info).update(Buffer.from([counter])).digest();
    chunks.push(previous);
    size += previous.length;
  }
  return Buffer.concat(chunks).subarray(0, length);
}

function webPushKeys() {
  const privateValue = String(process.env.WEB_PUSH_VAPID_PRIVATE_KEY || '').trim();
  const publicValue = String(process.env.WEB_PUSH_VAPID_PUBLIC_KEY || '').trim();
  if (!privateValue || !publicValue) return null;
  const publicBytes = base64UrlDecode(publicValue);
  if (publicBytes.length !== 65 || publicBytes[0] !== 4) throw new Error('WEB_PUSH_VAPID_PUBLIC_KEY must be a P-256 uncompressed public key.');
  return {
    publicValue,
    privateValue: base64UrlDecode(privateValue),
    x: base64UrlEncode(publicBytes.subarray(1, 33)),
    y: base64UrlEncode(publicBytes.subarray(33, 65))
  };
}

function webPushEncrypt(subscription, payload) {
  const receiverKeyBytes = base64UrlDecode(subscription.keys?.p256dh);
  const authSecret = base64UrlDecode(subscription.keys?.auth);
  if (receiverKeyBytes.length !== 65 || receiverKeyBytes[0] !== 4 || authSecret.length < 16) throw new Error('Invalid Web Push subscription keys.');
  const receiverKey = crypto.createPublicKey({ key: { kty: 'EC', crv: 'P-256', x: base64UrlEncode(receiverKeyBytes.subarray(1, 33)), y: base64UrlEncode(receiverKeyBytes.subarray(33, 65)) }, format: 'jwk' });
  const sender = crypto.generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const senderJwk = sender.publicKey.export({ format: 'jwk' });
  const senderPublic = Buffer.concat([Buffer.from([4]), base64UrlDecode(senderJwk.x), base64UrlDecode(senderJwk.y)]);
  const shared = crypto.diffieHellman({ privateKey: sender.privateKey, publicKey: receiverKey });
  const keyInfo = Buffer.concat([Buffer.from('WebPush: info\0'), receiverKeyBytes, senderPublic]);
  const prkKey = hkdfExtract(authSecret, shared);
  const ikm = hkdfExpand(prkKey, keyInfo, 32);
  const salt = crypto.randomBytes(16);
  const prk = hkdfExtract(salt, ikm);
  const cek = hkdfExpand(prk, Buffer.from('Content-Encoding: aes128gcm\0'), 16);
  const nonce = hkdfExpand(prk, Buffer.from('Content-Encoding: nonce\0'), 12);
  const cipher = crypto.createCipheriv('aes-128-gcm', cek, nonce);
  const encrypted = Buffer.concat([cipher.update(Buffer.concat([Buffer.from(JSON.stringify(payload)), Buffer.from([2])])), cipher.final(), cipher.getAuthTag()]);
  const header = Buffer.alloc(21);
  salt.copy(header, 0);
  header.writeUInt32BE(4096, 16);
  header[20] = senderPublic.length;
  return Buffer.concat([header, senderPublic, encrypted]);
}

function webPushVapidAuthorization(endpoint, keys) {
  const audience = new URL(endpoint).origin;
  const header = base64UrlEncode(JSON.stringify({ typ: 'JWT', alg: 'ES256' }));
  const claims = base64UrlEncode(JSON.stringify({ aud: audience, exp: Math.floor(Date.now() / 1000) + 12 * 60 * 60, sub: process.env.WEB_PUSH_VAPID_SUBJECT || 'mailto:support@pairlink2.web.app' }));
  const unsigned = `${header}.${claims}`;
  const privateKey = crypto.createPrivateKey({ key: { kty: 'EC', crv: 'P-256', x: keys.x, y: keys.y, d: base64UrlEncode(keys.privateValue) }, format: 'jwk' });
  const signature = crypto.sign('sha256', Buffer.from(unsigned), { key: privateKey, dsaEncoding: 'ieee-p1363' });
  return `vapid t=${unsigned}.${base64UrlEncode(signature)}, k=${keys.publicValue}`;
}

async function sendStandardWebPush(subscription, payload, options = {}) {
  const keys = webPushKeys();
  if (!keys) return { skipped: true };
  const endpoint = String(subscription?.endpoint || '');
  if (!/^https:\/\//i.test(endpoint)) throw new Error('Invalid Web Push endpoint.');
  const body = webPushEncrypt(subscription, payload);
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      Authorization: webPushVapidAuthorization(endpoint, keys),
      'Content-Encoding': 'aes128gcm',
      'Content-Type': 'application/octet-stream',
      TTL: String(Math.ceil((options.ttlMs || 60_000) / 1000)),
      Urgency: options.urgency || 'high'
    },
    body
  });
  return { ok: response.ok, status: response.status };
}

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

function getDispatchEventId(payload, options) {
  return String(options.eventId || payload.callId || payload.messageId || payload.reactionId || '');
}

function pushDispatchKey(kind, eventId) {
  return createHash('sha256').update(`${kind}\0${eventId}`).digest('hex');
}

async function claimPushDispatch(userId, payload, options) {
  const eventId = getDispatchEventId(payload, options);
  if (!eventId) return { allowed: true, ref: null, owner: '' };
  const kind = String(options.kind || payload.kind || 'notification');
  const key = pushDispatchKey(kind, eventId);
  const ref = db.ref(`users/${userId}/pushDispatches/${key}`);
  const owner = `${process.pid}:${randomUUID()}`;
  const now = Date.now();
  const result = await ref.transaction((current) => {
    if (current?.status === 'sent' || current?.status === 'attempted') return;
    if (current?.status === 'sending' && now - Number(current.claimedAt || 0) < DISPATCH_CLAIM_LEASE_MS) return;
    return { status: 'sending', claimedAt: now, owner, kind, eventId };
  }, undefined, false);
  return { allowed: result.committed && result.snapshot.val()?.owner === owner, ref, owner };
}

async function finishPushDispatch(claim, accepted, consumeOnFailure = false) {
  if (claim?.memoryKey) {
    if (accepted || consumeOnFailure) inMemoryCallPushDispatches.set(claim.memoryKey, { at: Date.now(), status: accepted ? 'sent' : 'attempted' });
    else inMemoryCallPushDispatches.delete(claim.memoryKey);
    return;
  }
  if (!claim?.ref) return;
  try {
    await claim.ref.transaction((current) => {
      if (!current || current.owner !== claim.owner) return;
      if (accepted) return { ...current, status: 'sent', sentAt: Date.now() };
      if (consumeOnFailure) return { ...current, status: 'attempted', attemptedAt: Date.now() };
      return null;
    }, undefined, false);
  } catch (error) {
    logPushError('could not finish notification dispatch claim', error);
  }
}

function hasFreshForegroundDevice(devices) {
  const now = Date.now();
  return Object.values(devices || {}).some((device) => {
    // Render treats an app as online only while its screen is actually visible.
    // A background RTDB connection or a stale foreground bit must not suppress a push.
    const visibleAt = Number(device?.visibilityUpdatedAt);
    const freshVisibility = Number.isFinite(visibleAt) &&
      now - visibleAt <= 30_000 && visibleAt - now <= 30_000;
    return device?.online === true && device?.foreground === true && freshVisibility;
  });
}

async function receiverIsOnline(userRef) {
  const snapshot = await userRef.child('devices').get();
  return hasFreshForegroundDevice(snapshot.val());
}

async function receiverIsInChatRoom(roomId, userId) {
  if (!roomId || !userId) return false;
  const snapshot = await db.ref(`rooms/${roomId}/users/${userId}`).get();
  const presence = snapshot.val();
  const activeAt = Number(presence?.chatActiveAt);
  const freshChatPresence = Number.isFinite(activeAt) &&
    Date.now() - activeAt <= 30_000 && activeAt - Date.now() <= 30_000;
  return presence?.online === true && presence?.chatActive === true && freshChatPresence;
}

async function sendPushToUser(userId, payload, options = {}) {
  const userRef = db.ref(`users/${userId}`);
  // A backgrounded app may keep its RTDB socket alive, but the native lock
  // screen owns call presentation there. Only an actually foreground app can
  // consume a call invite live without a push notification.
  const [tokensSnapshot, subscriptionsSnapshot, online] = await Promise.all([
    userRef.child('pushTokens').get(),
    userRef.child('webPushSubscriptions').get(),
    options.force ? Promise.resolve(false) : receiverIsOnline(userRef)
  ]);
  // Realtime Database is the live path for foreground devices. For incoming
  // calls, a background connection cannot present the native lock-screen UI.
  if (online && !options.force) {
    console.info(`[PairLink push] receiver online; skipped (${options.kind || 'notification'})`);
    return 0;
  }
  const tokenRecords = tokensSnapshot.val() || {};
  const uniqueTokens = new Map();
  Object.entries(tokenRecords).filter(([, record]) => {
    if (typeof record?.token !== 'string' || !record.token) return false;
    // For calls, an online but backgrounded device also needs the native alert.
    return !options.platform || record.platform === options.platform;
  }).forEach(([key, record]) => {
    if (!uniqueTokens.has(record.token)) uniqueTokens.set(record.token, [key, record]);
  });
  const entries = [...uniqueTokens.values()];
  const subscriptions = Object.entries(subscriptionsSnapshot.val() || {});
  if (!entries.length && !subscriptions.length) {
    console.info(`[PairLink push] no offline registered token (${options.kind || 'notification'})`);
    return 0;
  }

  let claim;
  if (options.persistDispatch === false) {
    const eventId = getDispatchEventId(payload, options);
    const memoryKey = eventId ? `${String(userId)}:${String(options.kind || payload.kind || 'call')}:${eventId}` : '';
    if (memoryKey) {
      const previous = inMemoryCallPushDispatches.get(memoryKey);
      if (previous && Date.now() - previous.at < (options.dedupeMs || 90_000)) return 0;
      inMemoryCallPushDispatches.set(memoryKey, { at: Date.now(), status: 'sending' });
    }
    claim = { allowed: true, memoryKey };
  } else {
    try {
      claim = await claimPushDispatch(userId, payload, options);
    } catch (error) {
      logPushError('could not claim notification dispatch', error);
      return 0;
    }
  }
  if (!claim.allowed) {
    console.info(`[PairLink push] duplicate dispatch suppressed (${options.kind || 'notification'})`);
    return 0;
  }

  // Recheck immediately before FCM dispatch so a device that just returned to
  // the foreground can handle the call live without a competing push card.
  try {
    if (!options.force && await receiverIsOnline(userRef)) {
      await finishPushDispatch(claim, false, Boolean(options.consumeOnAttempt));
      console.info(`[PairLink push] receiver came online; skipped (${options.kind || 'notification'})`);
      return 0;
    }
  } catch (error) {
    await finishPushDispatch(claim, false, Boolean(options.consumeOnAttempt));
    logPushError('presence check failed; push dispatch suppressed', error);
    return 0;
  }

  let sent = 0;
  const invalidTokenUpdates = {};
  // FCM multicast accepts at most 500 registration tokens per request.
  try {
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
  } catch (error) {
    await finishPushDispatch(claim, false, Boolean(options.consumeOnAttempt));
    logPushError(`FCM send attempt failed (${options.kind || 'notification'})`, error);
    return 0;
  }
  const invalidSubscriptionUpdates = {};
  let standardWebPushEnabled = false;
  try { standardWebPushEnabled = Boolean(webPushKeys()); }
  catch (error) { logPushError('Web Push VAPID configuration is invalid', error); }
  if (standardWebPushEnabled) {
    const results = await Promise.all(subscriptions.map(async ([key, record]) => {
      try {
        const result = await sendStandardWebPush(record, payload, options);
        if (result.ok) sent++;
        else if (result.status === 404 || result.status === 410) invalidSubscriptionUpdates[key] = null;
        else logPushError(`Web Push endpoint returned ${result.status}`, new Error(key));
      } catch (error) {
        logPushError('Web Push send failed', error);
      }
    }));
  }
  if (Object.keys(invalidTokenUpdates).length) {
    await userRef.child('pushTokens').update(invalidTokenUpdates).catch((error) => logPushError('could not remove invalid tokens', error));
  }
  if (Object.keys(invalidSubscriptionUpdates).length) {
    await userRef.child('webPushSubscriptions').update(invalidSubscriptionUpdates).catch((error) => logPushError('could not remove expired Web Push subscriptions', error));
  }
  await finishPushDispatch(claim, sent > 0, Boolean(options.consumeOnAttempt));
  console.info(`[PairLink push] ${options.kind || 'notification'} send completed; accepted=${sent}, targets=${entries.length}`);
  return sent;
}

async function getPushProfile(userId) {
  try {
    if (firestore && userId) {
      const snap = await firestore.collection('users').doc(String(userId)).get();
      if (snap.exists) {
        const data = snap.data() || {};
        return {
          name: data.name || data.displayName || 'PairLink user',
          photoDataUrl: data.photoDataUrl || '',
          notificationAvatar: data.notificationAvatar || ''
        };
      }
    }
  } catch (err) {
    console.warn('[PairLink push] Firestore profile lookup fallback:', err?.message || err);
  }
  return {};
}

async function sendRenderCallPush(userId, payload, options = {}) {
  if (!db || !messaging) throw new Error('Render call push is not initialized');
  return sendPushToUser(String(userId || ''), payload, {
    ttlMs: options.ttlMs || 60_000,
    collapseKey: options.collapseKey || `call-${String(payload.callId || '')}`,
    kind: options.kind || String(payload.kind || 'call'),
    eventId: String(options.eventId || payload.callId || ''),
    consumeOnAttempt: true,
    force: true,
    persistDispatch: false,
    dedupeMs: options.dedupeMs
  });
}

async function processPendingMessage(recipientId, snapshot) {
  const message = snapshot.val();
  if (message?.type === 'reaction') return processPendingReaction(recipientId, snapshot);
  if (!message || message.pushSentAt || !message.from || message.to !== recipientId) return;
  const ageMs = Date.now() - Number(message.sentAt || message.createdAt || 0);
  if (Number.isFinite(ageMs) && ageMs > MAX_PUSH_AGE_MS) return;
  if (message.type === 'chat-alert') {
    const profile = message.senderProfile?.name ? message.senderProfile : await getPushProfile(message.from);
    const messageId = String(message.id || message.messageId || snapshot.key);
    await sendPushToUser(recipientId, {
      kind: 'incoming_message',
      roomId: String(message.roomId || ''),
      messageId,
      senderId: String(message.from),
      senderName: String(message.fromName || profile.name || 'PairLink user'),
      senderPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl),
      alertOnly: 'true'
    }, {
      ttlMs: MAX_PUSH_AGE_MS,
      kind: 'online-message-alert',
      eventId: messageId,
      force: true
    });
    await snapshot.ref.remove().catch(() => {});
    return;
  }
  if (message.deliveryMode === 'realtime') {
    if (await receiverIsOnline(db.ref(`users/${recipientId}`))) {
      // The encrypted inbox row remains the actual message delivery path. If
      // the receiver is foreground but not viewing this room, send only a
      // generic FCM alert identifying the sender; never include message data.
      if (await receiverIsInChatRoom(message.roomId, recipientId)) return;
      const profile = message.senderProfile?.name
        ? message.senderProfile
        : await getPushProfile(message.from);
      const messageId = String(message.id || snapshot.key);
      await sendPushToUser(recipientId, {
        kind: 'incoming_message',
        queueId: String(snapshot.key || ''),
        roomId: String(message.roomId || ''),
        messageId,
        senderId: String(message.from),
        senderName: String(message.fromName || profile.name || 'PairLink user'),
        senderPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl),
        alertOnly: 'true'
      }, {
        ttlMs: MAX_PUSH_AGE_MS,
        kind: 'online-message-alert',
        eventId: messageId,
        force: true
      });
      // Do not set pushSentAt here: if the receiver goes offline before the
      // app consumes this inbox row, Render must still be able to move it to
      // pushQueue for the normal offline delivery path. Dispatch claims above
      // deduplicate the alert without changing that inbox state.
      return;
    }
    // The app went offline before consuming its live inbox row. Move the same
    // encrypted item to pushQueue so the existing worker path can wake it.
    const queueRef = db.ref(`pushQueue/${snapshot.key}`);
    await queueRef.transaction((current) => current || { ...message, deliveryMode: null }, undefined, false);
    await snapshot.ref.remove().catch((error) => logPushError('could not move a live inbox row to pushQueue', error));
    return;
  }
  const profile = message.senderProfile?.notificationAvatar
    ? message.senderProfile
    : await getPushProfile(message.from);
  const sent = await sendPushToUser(recipientId, {
    kind: 'incoming_message',
    roomId: message.roomId || '',
    messageId: message.id || snapshot.key,
    senderId: message.from,
    senderName: message.fromName || profile.name || 'PairLink user',
    senderPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl),
      action: 'sync_chat',
    encrypted: 'true'
  }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'encrypted-message-alert', eventId: message.id || snapshot.key });
  if (sent > 0) {
    await markProcessed(snapshot.ref);
  }
}

async function processPendingReaction(recipientId, snapshot) {
  const reaction = snapshot.val();
  if (!reaction || reaction.pushSentAt || reaction.to !== recipientId || !reaction.from || !reaction.roomId || !reaction.emoji) return;
  const ageMs = Date.now() - Number(reaction.sentAt || reaction.createdAt || 0);
  if (!Number.isFinite(ageMs) || ageMs < -60_000 || ageMs > MAX_PUSH_AGE_MS) return;
  const profile = await getPushProfile(reaction.from);
  const sent = await sendPushToUser(recipientId, {
    kind: 'incoming_reaction',
    roomId: reaction.roomId,
    reactionId: reaction.id || snapshot.key,
    messageId: reaction.messageId || '',
    mediaId: reaction.mediaId || '',
    senderId: reaction.from,
    senderName: profile.name || 'PairLink user',
    senderPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl)
  }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'chat-reaction', eventId: reaction.id || snapshot.key });
  if (sent) await markProcessed(snapshot.ref);
}

async function processRoomEvent(roomId, snapshot) {
  const event = snapshot.val();
  if (!event || event.event !== 'chat-message' || !event.sender || event.pushSentAt) return;
  // New clients persist an encrypted pendingMessages copy alongside the room
  // event. That inbox listener sends the single push for this message.
  if (event.durableInbox === true) return;
  const ageMs = Date.now() - Number(event.ts || 0);
  if (!Number.isFinite(ageMs) || ageMs < -60_000 || ageMs > MAX_PUSH_AGE_MS) return;
  const recipientId = event.recipientId;
  if (!recipientId || recipientId === event.sender) return;
  const profile = await getPushProfile(event.sender);
  const sent = await sendPushToUser(recipientId, {
    kind: 'incoming_message',
    roomId,
    messageId: event.payload?.id || snapshot.key,
    senderId: event.sender,
    senderName: profile.name || 'PairLink user',
    senderPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl),
    action: 'sync_chat',
    encrypted: 'true'
  }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'encrypted-message-alert', eventId: event.payload?.id || snapshot.key });
  if (sent) await markProcessed(snapshot.ref);
}

async function enqueueRenderChatReminder(input = {}) {
  if (!db) return false;
  const messageId = String(input.messageId || '').slice(0, 180);
  const from = String(input.from || '').slice(0, 180);
  const to = String(input.to || '').slice(0, 180);
  const roomId = String(input.roomId || '').slice(0, 200);
  if (!messageId || !from || !to || !roomId || from === to) return false;

  const queueRef = db.ref(`pushQueue/chat-alert_${safeKey(messageId)}`);
  const result = await queueRef.transaction((current) => current || {
    id: messageId,
    messageId,
    type: 'chat-alert',
    from,
    to,
    fromName: String(input.fromName || 'PairLink user').slice(0, 120),
    senderProfile: { name: String(input.fromName || 'PairLink user').slice(0, 120), notificationAvatar: compactPhoto(input.senderPhoto) },
    roomId,
    sentAt: Number(input.sentAt) || Date.now(),
    createdAt: Date.now()
  }, undefined, false);
  return Boolean(result.snapshot?.exists());
}

async function enqueueRenderMissedCall(input = {}) {
  if (!db) throw new Error('Render missed-call queue is not initialized');
  const callId = String(input.callId || '');
  const from = String(input.callerId || input.from || '');
  const to = String(input.recipientId || input.to || '');
  if (!callId || !from || !to) throw new Error('Missed-call notification is missing its call or user ID');
  const ref = db.ref(`pushQueue/missed_call_${safeKey(callId)}`);
  const result = await ref.transaction((current) => current || {
    type: 'missed_call',
    notificationKind: 'missed_call',
    callId,
    from,
    to,
    fromName: String(input.callerName || 'PairLink user').slice(0, 120),
    senderPhoto: compactPhoto(input.callerPhoto),
    callType: input.video ? 'video' : 'audio',
    video: input.video ? 'true' : 'false',
    createdAt: Date.now()
  }, undefined, false);
  return Boolean(result.snapshot?.exists());
}

function listenForChildAdded(ref, label, handler) {
  ref.on('child_added', (snapshot) => {
    Promise.resolve(handler(snapshot)).catch((error) => logPushError(`${label} handler failed`, error));
  }, (error) => logPushError(`${label} listener failed`, error));
}

async function processPushQueueItem(snapshot) {
  const item = snapshot.val();
  if (item?.type === 'missed_call' && item.callId) {
    if (item.pushSentAt || missedCallDispatchBusy.has(snapshot.key)) return;
    missedCallDispatchBusy.add(snapshot.key);
    try {
      const sent = await sendPushToUser(String(item.to || ''), {
        kind: 'missed_call',
        queueId: String(snapshot.key || ''),
        callId: String(item.callId),
        callerId: String(item.from || ''),
        callerName: String(item.fromName || 'PairLink user'),
        senderPhoto: compactPhoto(item.senderPhoto),
        callType: String(item.callType || 'audio'),
        video: String(item.video || 'false')
      }, {
        ttlMs: MAX_PUSH_AGE_MS,
        kind: 'missed-call',
        eventId: String(item.callId),
        force: true,
        persistDispatch: false,
        dedupeMs: 5 * 60_000
      });
      if (sent > 0) {
        // Keep the queue row until the receiver's worker acknowledges it,
        // matching encrypted chat notification delivery semantics.
        await snapshot.ref.transaction((current) =>
          current ? { ...current, pushSentAt: Date.now() } : null, undefined, false).catch(() => null);
      } else {
        await snapshot.ref.update({ lastPushAttemptAt: Date.now() }).catch(() => {});
      }
    } finally {
      missedCallDispatchBusy.delete(snapshot.key);
    }
    return;
  }
  if (item && (['call', 'missed_call', 'call-ended'].includes(String(item.type || '')) ||
      String(item.notificationKind || '') === 'missed_call' ||
      ['call', 'video'].includes(String(item.action || '')))) {
    // Call signaling, call invites, and missed-call delivery are Render-owned.
    // Remove stale legacy rows without using them to route or send a call.
    await snapshot.ref.remove().catch(() => {});
    return;
  }
  if (!item || !item.to || !item.from) {
    await snapshot.ref.remove().catch(() => {});
    return;
  }
  // Explicitly silent chat rows should not trigger a push.
  if (item.type === 'chat' && item.suppressPush === true) return;
  const recipientId = String(item.to);
  const eventId = String(item.messageId || snapshot.key);
  const profile = item.senderProfile?.notificationAvatar
    ? item.senderProfile
    : await getPushProfile(item.from);

  try {
    if (item.type === 'chat-alert') {
      if (item.pushSentAt) return;
      const sent = await sendPushToUser(recipientId, {
        kind: 'incoming_message',
        queueId: String(snapshot.key || ''),
        roomId: String(item.roomId || ''),
        messageId: String(item.messageId || eventId),
        senderId: String(item.from),
        senderName: String(item.fromName || profile.name || 'PairLink user'),
        senderPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl),
        alertOnly: 'true'
      }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'online-message-alert', eventId, force: true });
      console.info(`[PairLink push] Render sent a generic chat reminder for recipient ${recipientId} (messageId=${eventId}).`);
      if (sent > 0) {
        // Do not recreate an alert row if the client already deleted it after
        // importing the message. A plain update on a removed RTDB child would
        // resurrect a partial pushQueue record.
        const marked = await snapshot.ref.transaction((current) =>
          current ? { ...current, pushSentAt: Date.now() } : null, undefined, false).catch(() => null);
        if (marked?.snapshot?.exists()) {
          const cleanupTimer = setTimeout(() => snapshot.ref.remove().catch(() => {}), 60_000);
          cleanupTimer.unref?.();
        }
      } else {
        await snapshot.ref.update({ lastPushAttemptAt: Date.now() }).catch(() => {});
      }
    } else if (item.type === 'deletion') {
      if (item.pushSentAt) return;
      await sendPushToUser(recipientId, {
        kind: 'incoming_deletion',
        queueId: String(snapshot.key || eventId),
        roomId: item.roomId || '',
        messageId: eventId,
        deletionKind: item.kind || '',
        targetMessageId: item.messageId || '',
        targetMediaId: item.mediaId || '',
        senderId: item.from,
        senderName: item.fromName || profile.name || 'PairLink user',
        senderPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl),
        action: 'sync_chat'
      }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'deletion-sync', eventId, force: true });
      console.info(`[PairLink push] Render sent DELETION push for recipient ${recipientId} (id=${eventId}).`);
      await snapshot.ref.update({ pushSentAt: Date.now() }).catch(() => {});
    } else {
      if (item.pushSentAt) return; // Already notified, waiting for receiver to pick up
      const latestQueueItem = (await snapshot.ref.get().catch(() => null))?.val();
      if (!latestQueueItem || latestQueueItem.pushSentAt) return;
      const sent = await sendPushToUser(recipientId, {
        kind: 'incoming_message',
        queueId: String(snapshot.key || eventId),
        roomId: item.roomId || '',
        messageId: eventId,
        senderId: item.from,
        senderName: item.fromName || profile.name || 'PairLink user',
        senderPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl),
        action: 'sync_chat',
        encrypted: 'true'
      }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'encrypted-message-alert', eventId, force: true });
      console.info(`[PairLink push] Render sent MESSAGE push notification for recipient ${recipientId} (messageId=${eventId}).`);
      // Mark as notified in pushQueue, DO NOT delete until recipient opens app and downloads it!
      await snapshot.ref.transaction((current) =>
        current ? { ...current, pushSentAt: Date.now() } : null, undefined, false).catch(() => {});
    }
  } catch (error) {
    logPushError(`pushQueue dispatch failed for ${recipientId}`, error);
  }
}

async function dispatchQueuedMissedCalls(userId) {
  if (!db || !messaging) return 0;
  const recipientId = String(userId || '');
  if (!recipientId) return 0;
  const snapshot = await db.ref('pushQueue').orderByChild('type').equalTo('missed_call').get();
  let dispatched = 0;
  const jobs = [];
  snapshot.forEach((child) => {
    const item = child.val();
    if (item?.to === recipientId && item.callId && !item.pushSentAt) jobs.push(child);
  });
  for (const job of jobs) {
    await processPushQueueItem(job);
    const latest = (await job.ref.get().catch(() => null))?.val();
    if (latest?.pushSentAt) dispatched += 1;
  }
  return dispatched;
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
    firestore = getFirestore(app);
    messaging = getMessaging(app);
    let pushQueueChain = Promise.resolve();
    listenForChildAdded(db.ref('pushQueue'), 'pushQueue', (snapshot) => {
      pushQueueChain = pushQueueChain.then(() => processPushQueueItem(snapshot)).catch((err) => {
        logPushError('pushQueue serial processing error', err);
      });
    });
    console.info('[PairLink push] Render is listening only to root pushQueue for push jobs.');
    return true;
  } catch (error) {
    logPushError('could not start Firebase event relay', error);
    return false;
  }
}

module.exports = { startRenderPushRelay, enqueueRenderChatReminder, enqueueRenderMissedCall, dispatchQueuedMissedCalls, sendRenderCallPush };
