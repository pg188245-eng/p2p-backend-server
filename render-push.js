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

async function routeCallRequest(input = {}) {
  if (!db || !firestore || !messaging) throw new Error('Render call router is not initialized');

  const callerId = String(input.callerId || '').trim();
  const recipientId = String(input.recipientId || '').trim();
  const roomId = String(input.roomId || '').trim();
  const callId = String(input.callId || '').trim();
  if (!callerId || !recipientId || callerId === recipientId || !roomId || !callId ||
      callerId.length > 180 || recipientId.length > 180 || roomId.length > 200 || callId.length > 180) {
    const error = new Error('Invalid call-routing request.');
    error.status = 400;
    throw error;
  }

  // Accept a route request only for the permanent Firestore room belonging to
  // this exact user pair, and only while its Firebase call session is ringing.
  const participants = [callerId, recipientId].sort();
  const pairKey = createHash('sha256').update(participants.join('|')).digest('hex');
  const [pairSnapshot, callSnapshot, activeInviteSnapshot] = await Promise.all([
    firestore.collection('pairRooms').doc(pairKey).get(),
    db.ref(`rooms/${roomId}/callSessions/${callId}`).get(),
    db.ref(`users/${recipientId}/activeCallInvites/${callId}`).get()
  ]);
  const pair = pairSnapshot.exists ? pairSnapshot.data() : null;
  const call = callSnapshot.val();
  const activeInvite = activeInviteSnapshot.val();
  const pairMatches = pair && String(pair.roomId || '') === roomId &&
    (!pair.userA || String(pair.userA) === participants[0]) &&
    (!pair.userB || String(pair.userB) === participants[1]);
  const callMatches = call && call.status === 'ringing' &&
    String(call.callerId || '') === callerId && String(call.recipientId || '') === recipientId;
  const inviteMatches = activeInvite && String(activeInvite.callId || '') === callId &&
    String(activeInvite.from || activeInvite.callerId || '') === callerId &&
    String(activeInvite.to || activeInvite.recipientId || '') === recipientId &&
    String(activeInvite.roomId || '') === roomId;
  if (!pairMatches || !callMatches || !inviteMatches) {
    const error = new Error('Call request did not match its Firebase room and active invite.');
    error.status = 409;
    throw error;
  }

  const inviteId = `call_${callId}`;
  const action = call.kind === 'video' ? 'video' : 'call';
  const invite = {
    id: inviteId,
    type: 'call',
    from: callerId,
    fromName: String(call.callerName || 'PairLink user'),
    ...((activeInvite.fromPhotoDataUrl || activeInvite.callerPhoto || call.callerPhoto)
      ? { fromPhotoDataUrl: String(activeInvite.fromPhotoDataUrl || activeInvite.callerPhoto || call.callerPhoto) }
      : {}),
    to: recipientId,
    roomId,
    action,
    callId,
    createdAt: Number(call.createdAt) || Date.now(),
    sentAt: Date.now(),
    delivery: 'render-routed'
  };

  const queueRef = db.ref(`pushQueue/call_${safeKey(callId)}`);
  const queued = await queueRef.transaction((current) => current || invite, undefined, false);
  console.info(`[PairLink push] Render queued call notification (callId=${callId}).`);
  return { route: queued.snapshot?.exists() ? 'pushQueue' : 'already-queued', callId };
}

async function processInvite(recipientId, snapshot) {
  const invite = snapshot.val();
  if (!invite || invite.pushSentAt || invite.to !== recipientId || !invite.from || !invite.roomId) return;
  const action = String(invite.action || '');
  const ageMs = Date.now() - Number(invite.createdAt || 0);
  if (!Number.isFinite(ageMs) || ageMs < -60_000) return;

  if (action === 'call' || action === 'video') {
    try {
      if (ageMs > 60_000) return;
      const callId = String(invite.callId || snapshot.key);
      // The Firebase room session is authoritative. The invite exists only to
      // request one Render wake-up push for a non-visible receiver.
      const [cancellationSnapshots, roomCallSnapshot] = await Promise.all([
        Promise.all([recipientId, invite.from].map((userId) =>
          db.ref(`users/${userId}/callCancellations/${callId}`).get().catch(() => null))),
        db.ref(`rooms/${invite.roomId}/callSessions/${callId}`).get().catch(() => null)
      ]);
      const roomCall = roomCallSnapshot?.val();
      if (cancellationSnapshots.some((item) => item?.exists()) ||
          !roomCall || roomCall.status !== 'ringing' ||
          String(roomCall.callerId || '') !== String(invite.from) ||
          String(roomCall.recipientId || '') !== recipientId) return;

      const callerProfile = await getPushProfile(invite.from);
      const isVideo = action === 'video' || roomCall.kind === 'video';
      console.info(`[PairLink push] call invite observed (callId=${callId}, recipient=${recipientId}); sending one push attempt.`);
      await sendPushToUser(recipientId, {
        kind: 'incoming_call', callId, callerId: invite.from,
        callerName: String(invite.fromName || roomCall.callerName || callerProfile.name || 'PairLink user'),
        senderPhoto: compactPhoto(invite.fromPhotoDataUrl, callerProfile.notificationAvatar, callerProfile.photoDataUrl),
        roomId: invite.roomId, action: isVideo ? 'video' : 'call', video: isVideo ? 'true' : 'false'
      }, { ttlMs: 45_000, collapseKey: `call-${callId}`, kind: 'call', eventId: callId, consumeOnAttempt: true });
    } finally {
      // Exactly one wake-up attempt per call. All subsequent call control is
      // room-local Firebase signaling, never a second Render invite.
      await snapshot.ref.remove().catch((error) => logPushError('could not remove one-shot call invite', error));
    }
    return;
  }

}

async function processLiveCallInvite(recipientId, snapshot) {
  const invite = snapshot.val();
  if (!invite || !['call', 'video'].includes(String(invite.action || ''))) return;
  try {
    const isForeground = await receiverIsOnline(db.ref(`users/${recipientId}`));
    if (isForeground) {
      watchLiveCallInviteForOfflineFallback(recipientId, snapshot);
      console.info(`[PairLink push] live call invite belongs to foreground receiver; watching visibility for fallback (callId=${invite.callId || snapshot.key}).`);
      return;
    }
  } catch (error) {
    logPushError('could not verify live-call receiver presence; falling back to push', error);
  }
  return processInvite(recipientId, snapshot);
}

function watchLiveCallInviteForOfflineFallback(recipientId, snapshot) {
  const inviteId = String(snapshot.key || '');
  const watcherKey = `${recipientId}:${inviteId}`;
  if (!inviteId || liveCallFallbacks.has(watcherKey)) return;
  liveCallFallbacks.add(watcherKey);

  const inviteRef = db.ref(`users/${recipientId}/liveCallInvites/${inviteId}`);
  const devicesRef = db.ref(`users/${recipientId}/devices`);
  let busy = false;
  let timer = null;
  const cleanup = () => {
    devicesRef.off('value', onDevicesChanged);
    inviteRef.off('value', onInviteChanged);
    if (timer) clearTimeout(timer);
    liveCallFallbacks.delete(watcherKey);
  };
  const onInviteChanged = (current) => {
    if (!current.exists()) cleanup();
  };
  const onDevicesChanged = (devices) => {
    if (busy || hasFreshForegroundDevice(devices.val())) return;
    busy = true;
    inviteRef.get().then((current) => {
      cleanup();
      if (current.exists()) return processInvite(recipientId, current);
      return undefined;
    }).catch((error) => {
      cleanup();
      logPushError('could not fall back from live invite to offline call push', error);
    });
  };

  inviteRef.on('value', onInviteChanged, (error) => {
    cleanup();
    logPushError('live call invite fallback listener failed', error);
  });
  devicesRef.on('value', onDevicesChanged, (error) => {
    cleanup();
    logPushError('live call visibility listener failed', error);
  });
  timer = setTimeout(cleanup, 60_000);
  timer.unref?.();
}

async function processCallCancellation(recipientId, snapshot) {
  const cancellation = snapshot.val();
  if (!cancellation || cancellation.pushSentAt || cancellation.to !== recipientId ||
      !cancellation.from || !cancellation.callId) return;
  const callId = String(cancellation.callId);
  const inviteId = String(cancellation.inviteId || '');
  const callerId = String(cancellation.callerId || '');
  const callRecipientId = String(cancellation.recipientId || '');
  const terminalStatus = String(cancellation.status || 'ended');
  const isRecipientTerminal = Boolean(callRecipientId && recipientId === callRecipientId && cancellation.to === recipientId);
  let callStateSnapshots = [];
  let wasAccepted = false;
  if (isRecipientTerminal && callerId) {
    const participantIds = [...new Set([callerId, callRecipientId])];
    const [acceptanceSnapshots, stateSnapshots] = await Promise.all([
      Promise.all(participantIds.map((userId) => db.ref(`users/${userId}/callAcceptances/${callId}`).get().catch(() => null))),
      Promise.all(participantIds.map((userId) => db.ref(`users/${userId}/callStates/${callId}`).get().catch(() => null)))
    ]);
    callStateSnapshots = stateSnapshots;
    wasAccepted = acceptanceSnapshots.concat(stateSnapshots).some((item) => {
      const state = item?.val();
      return Boolean(state?.acceptedAt || state?.status === 'accepted');
    });
  }
  // The call-ended push removes the lock-screen/browser ringing alert on every
  // device. A missed-call push is a separate, non-ringing notification.
  let missedPushSent = 1;
  if (isRecipientTerminal && callerId && terminalStatus === 'missed' && !wasAccepted) {
    const callerProfile = await getPushProfile(callerId);
    const callState = callStateSnapshots.map((item) => item?.val()).find(Boolean) || {};
    missedPushSent = await sendPushToUser(callRecipientId, {
      kind: 'missed_call',
      callId,
      roomId: String(cancellation.roomId || callState.roomId || ''),
      callerId,
      callerName: String(callerProfile.name || callerId),
      senderPhoto: compactPhoto(callerProfile.notificationAvatar, callerProfile.photoDataUrl),
      video: cancellation.video === true || callState.kind === 'video' ? 'true' : 'false'
    }, {
      ttlMs: 7 * 24 * 60 * 60 * 1000,
      collapseKey: `missed-${callId}`,
      kind: 'missed-call',
      eventId: callId
    });
  }
  let endedPushSent = 1;
  if (isRecipientTerminal && callRecipientId) {
    endedPushSent = await sendPushToUser(callRecipientId, { kind: 'call_ended', callId }, {
      ttlMs: 60_000, collapseKey: `call-ended-${callId}`, kind: 'call-ended', eventId: callId, force: true
    });
  }
  if (missedPushSent > 0 && endedPushSent > 0) {
    const completedDispatches = [
      db.ref(`users/${cancellation.from}/pushDispatches/${pushDispatchKey('call', callId)}`),
      db.ref(`users/${recipientId}/pushDispatches/${pushDispatchKey('call', callId)}`),
      db.ref(`users/${recipientId}/pushDispatches/${pushDispatchKey('call-ended', callId)}`)
    ];
    await Promise.all(completedDispatches.map((ref) => ref.remove().catch((error) =>
      logPushError('could not remove completed call push record', error))));
    // Keep the terminal marker available to live clients for a short grace
    // period. FCM acceptance is not proof that an active WebView consumed the
    // event, and immediate deletion can race its RTDB listener.
    await snapshot.ref.update({ pushSentAt: Date.now() });
    setTimeout(() => snapshot.ref.remove().catch((error) =>
      logPushError('could not expire completed call cancellation', error)), 60_000);
  }
}

async function processPendingMessage(recipientId, snapshot) {
  const message = snapshot.val();
  if (message?.type === 'call') return processPendingCall(recipientId, snapshot);
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

async function processPendingCall(recipientId, snapshot) {
  const call = snapshot.val();
  try {
    if (!call || call.pushSentAt || !call.from || call.to !== recipientId || !call.roomId || !call.callId) return;
    console.info(`[PairLink push] pending call wake row observed (callId=${call.callId}, recipient=${recipientId}); validating and sending one wake push.`);
    const ageMs = Date.now() - Number(call.sentAt || call.createdAt || 0);
    if (!Number.isFinite(ageMs) || ageMs < -60_000 || ageMs > 60_000) return;

    // A pending inbox entry only asks Render to wake the receiver. Firebase's
    // shared room call session remains authoritative for whether it is ringing.
    const roomCall = (await db.ref(`rooms/${call.roomId}/callSessions/${call.callId}`).get()).val();
    if (!roomCall || roomCall.status !== 'ringing' ||
        String(roomCall.callerId || '') !== String(call.from) ||
        String(roomCall.recipientId || '') !== recipientId) return;

    const profile = await getPushProfile(call.from);
    const isVideo = call.video === true || call.action === 'video' || roomCall.kind === 'video';
    await sendPushToUser(recipientId, {
      kind: 'incoming_call', callId: String(call.callId), callerId: String(call.from),
      callerName: String(call.fromName || profile.name || 'PairLink user'),
      senderPhoto: compactPhoto(call.fromPhotoDataUrl, profile.notificationAvatar, profile.photoDataUrl),
      roomId: String(call.roomId), action: isVideo ? 'video' : 'call', video: isVideo ? 'true' : 'false'
    }, {
      ttlMs: 45_000,
      collapseKey: `call-${call.callId}`,
      kind: 'call',
      eventId: String(call.callId),
      consumeOnAttempt: true,
      force: true
    });
  } catch (error) {
    logPushError('one-shot call wake-up failed', error);
  } finally {
    // One inbox row produces at most one attempt, whether FCM accepts or fails.
    // Remove it after the attempt so it cannot produce a second ring.
    await snapshot.ref.remove().catch((error) => logPushError('could not remove one-shot call inbox row', error));
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

  try {
    const profile = item.senderProfile?.notificationAvatar
      ? item.senderProfile
      : await getPushProfile(item.from);

    if (item.type === 'call-ended') {
      await sendPushToUser(recipientId, {
        kind: 'call_ended', callId: String(item.callId || eventId)
      }, { ttlMs: 60_000, collapseKey: `call-ended-${eventId}`, kind: 'call-ended', eventId, force: true });
      await snapshot.ref.remove().catch(() => {});
    } else if (isCall) {
      // Foreground clients consume this durable invite directly from
      // pushQueue, even while viewing another chat. Do not race the live
      // listener with an FCM push and delete the invite underneath it.
      const currentInvite = await snapshot.ref.get().catch(() => null);
      if (!currentInvite?.exists()) return;
      if (await receiverIsOnline(db.ref(`users/${recipientId}`))) return;
      const isVideo = Boolean(item.video || item.action === 'video');
      await sendPushToUser(recipientId, {
        kind: 'incoming_call',
        callId: String(item.callId || eventId),
        callerId: item.from,
        callerName: item.fromName || profile.name || 'PairLink user',
        senderPhoto: compactPhoto(item.fromPhotoDataUrl, item.callerPhoto, profile.notificationAvatar, profile.photoDataUrl),
        video: isVideo ? 'true' : 'false',
        roomId: item.roomId || '',
        action: isVideo ? 'video' : 'call'
      }, { ttlMs: 60_000, kind: 'call-wake', eventId });
      console.info(`[PairLink push] Render sent CALL push notification for recipient ${recipientId} (callId=${item.callId}).`);
      await snapshot.ref.remove().catch(() => {});
    } else if (item.type === 'chat-alert') {
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
    } else if (item.type === 'missed_call') {
      if (item.pushSentAt) return;
      const missedCallId = String(item.callId || eventId);
      const missedRoomId = String(item.roomId || '');
      if (!missedRoomId) {
        await snapshot.ref.remove().catch(() => {});
        return;
      }
      const liveCall = (await db.ref(`rooms/${missedRoomId}/callSessions/${missedCallId}`).get().catch(() => null))?.val();
      const missedRecord = (await db.ref(`users/${recipientId}/missedCalls/${missedCallId}`).get().catch(() => null))?.val();
      const hasCurrentMissedRecord = missedRecord?.status === 'missed' &&
        String(missedRecord.roomId || '') === missedRoomId;
      const hasCurrentTimeoutSession = liveCall?.status === 'missed' && liveCall?.endedBy === 'timeout';
      if (!hasCurrentMissedRecord && !hasCurrentTimeoutSession) {
        // A reject/end may clear a timeout queue item while this dispatcher is
        // already processing its child_added snapshot. Revalidate the canonical
        // room session immediately before producing a missed-call push.
        await snapshot.ref.remove().catch(() => {});
        return;
      }
      const sent = await sendPushToUser(recipientId, {
        kind: 'missed_call',
        callId: missedCallId,
        roomId: missedRoomId,
        callerId: String(item.callerId || item.from),
        callerName: String(item.fromName || profile.name || 'PairLink user'),
        senderPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl),
        video: item.video === true || item.video === 'true' || item.action === 'video' ? 'true' : 'false'
      }, { ttlMs: 7 * 24 * 60 * 60 * 1000, collapseKey: `missed-${eventId}`, kind: 'missed-call', eventId });
      if (sent > 0) await snapshot.ref.remove().catch(() => {});
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

module.exports = { startRenderPushRelay, enqueueRenderChatReminder, routeCallRequest };
