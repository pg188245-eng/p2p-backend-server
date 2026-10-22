const { createHash, randomUUID } = require('node:crypto');
const { cert, initializeApp } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');
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

async function sendPushToUser(userId, payload, options = {}) {
  const userRef = db.ref(`users/${userId}`);
  // A backgrounded app may keep its RTDB socket alive, but the native lock
  // screen owns call presentation there. Only an actually foreground app can
  // consume a call invite live without a push notification.
  const [tokensSnapshot, online] = await Promise.all([
    userRef.child('pushTokens').get(),
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
  if (!entries.length) {
    console.info(`[PairLink push] no offline registered token (${options.kind || 'notification'})`);
    return 0;
  }

  let claim;
  try {
    claim = await claimPushDispatch(userId, payload, options);
  } catch (error) {
    logPushError('could not claim notification dispatch', error);
    return 0;
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
  if (Object.keys(invalidTokenUpdates).length) {
    await userRef.child('pushTokens').update(invalidTokenUpdates).catch((error) => logPushError('could not remove invalid tokens', error));
  }
  await finishPushDispatch(claim, sent > 0, Boolean(options.consumeOnAttempt));
  console.info(`[PairLink push] ${options.kind || 'notification'} send completed; accepted=${sent}, targets=${entries.length}`);
  return sent;
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
    roomId,
    sentAt: Number(input.sentAt) || Date.now(),
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
  if (!item || !item.to || !item.from) {
    await snapshot.ref.remove().catch(() => {});
    return;
  }
  const recipientId = String(item.to);
  const eventId = String(item.callId || item.messageId || snapshot.key);
  const isCall = item.type === 'call' || item.action === 'call' || item.action === 'video';
  const senderName = String(item.fromName || item.callerName || 'PairLink user');
  const senderPhoto = compactPhoto(item.fromPhotoDataUrl, item.senderPhoto, item.callerPhoto);

  try {
    if (isCall) {
      const ageMs = Date.now() - Number(item.createdAt || item.sentAt || 0);
      if (!Number.isFinite(ageMs) || ageMs > 60_000 || ageMs < -60_000) {
        await snapshot.ref.remove().catch(() => {});
        return;
      }
      const isVideo = Boolean(item.video || item.action === 'video');
      await sendPushToUser(recipientId, {
        kind: 'incoming_call',
        callId: String(item.callId || eventId),
        callerId: item.from,
        callerName: senderName,
        callerPhoto: senderPhoto,
        video: isVideo ? 'true' : 'false',
        roomId: item.roomId || '',
        action: isVideo ? 'video' : 'call'
      }, { ttlMs: 60_000, kind: 'call-wake', eventId, force: true });
      console.info(`[PairLink push] Render sent CALL push notification for recipient ${recipientId} (callId=${item.callId}).`);
      await snapshot.ref.remove().catch(() => {});
    } else if (item.type === 'chat-alert') {
      if (item.pushSentAt) return;
      const sent = await sendPushToUser(recipientId, {
        kind: 'incoming_message',
        roomId: String(item.roomId || ''),
        messageId: String(item.messageId || eventId),
        senderId: String(item.from),
        senderName,
        senderPhoto,
        alertOnly: 'true'
      }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'online-message-alert', eventId, force: true });
      console.info(`[PairLink push] Render sent a generic chat reminder for recipient ${recipientId} (messageId=${eventId}).`);
      if (sent > 0) {
        await snapshot.ref.update({ pushSentAt: Date.now() }).catch(() => {});
        const cleanupTimer = setTimeout(() => snapshot.ref.remove().catch(() => {}), 60_000);
        cleanupTimer.unref?.();
      } else {
        await snapshot.ref.update({ lastPushAttemptAt: Date.now() }).catch(() => {});
      }
    } else if (item.type === 'missed_call') {
      if (item.pushSentAt) return;
      const sent = await sendPushToUser(recipientId, {
        kind: 'missed_call',
        callId: String(item.callId || eventId),
        roomId: String(item.roomId || ''),
        callerId: String(item.callerId || item.from),
        callerName: senderName,
        callerPhoto: senderPhoto,
        video: item.video === true || item.video === 'true' || item.action === 'video' ? 'true' : 'false'
      }, { ttlMs: 7 * 24 * 60 * 60 * 1000, collapseKey: `missed-${eventId}`, kind: 'missed-call', eventId });
      if (sent > 0) await snapshot.ref.remove().catch(() => {});
    } else if (item.type === 'reaction') {
      if (item.pushSentAt) return;
      const sent = await sendPushToUser(recipientId, {
        kind: 'incoming_reaction',
        roomId: String(item.roomId || ''),
        reactionId: String(item.id || eventId),
        messageId: String(item.messageId || ''),
        mediaId: String(item.mediaId || ''),
        senderId: String(item.from),
        senderName,
        senderPhoto
      }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'chat-reaction', eventId });
      if (sent > 0) await snapshot.ref.update({ pushSentAt: Date.now() }).catch(() => {});
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
        senderName,
        action: 'sync_chat'
      }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'deletion-sync', eventId, force: true });
      console.info(`[PairLink push] Render sent DELETION push for recipient ${recipientId} (id=${eventId}).`);
      await snapshot.ref.update({ pushSentAt: Date.now() }).catch(() => {});
    } else {
      if (item.pushSentAt) return; // Already notified, waiting for receiver to pick up
      const sent = await sendPushToUser(recipientId, {
        kind: 'incoming_message',
        queueId: String(snapshot.key || eventId),
        roomId: item.roomId || '',
        messageId: eventId,
        senderId: item.from,
        senderName,
        senderPhoto,
        action: 'sync_chat',
        encrypted: 'true'
      }, { ttlMs: MAX_PUSH_AGE_MS, kind: 'encrypted-message-alert', eventId, force: true });
      console.info(`[PairLink push] Render sent MESSAGE push notification for recipient ${recipientId} (messageId=${eventId}).`);
      // Mark as notified in pushQueue, DO NOT delete until recipient opens app and downloads it!
      await snapshot.ref.update({ pushSentAt: Date.now() }).catch(() => {});
    }
  } catch (error) {
    logPushError(`pushQueue dispatch failed for ${recipientId}`, error);
  }
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
    let pushQueueChain = Promise.resolve();
    listenForChildAdded(db.ref('pushQueue'), 'pushQueue', (snapshot) => {
      pushQueueChain = pushQueueChain.then(() => processPushQueueItem(snapshot)).catch((err) => {
        logPushError('pushQueue serial processing error', err);
      });
    });

    console.info('[PairLink push] Render is watching only the root pushQueue for push jobs.');
    return true;
  } catch (error) {
    logPushError('could not start Firebase event relay', error);
    return false;
  }
}

module.exports = { startRenderPushRelay, enqueueRenderChatReminder };
