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

async function receiverIsOnline(userRef) {
  const snapshot = await userRef.child('devices').get();
  const now = Date.now();
  return Object.values(snapshot.val() || {}).some((device) => {
    // Render treats an app as online only while its screen is actually visible.
    // A background RTDB connection or a stale foreground bit must not suppress a push.
    const visibleAt = Number(device?.visibilityUpdatedAt);
    const freshVisibility = Number.isFinite(visibleAt) &&
      now - visibleAt <= 30_000 && visibleAt - now <= 30_000;
    return device?.online === true && device?.foreground === true && freshVisibility;
  });
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

      const profile = await getPushProfile(invite.from);
      console.info(`[PairLink push] call wake invite observed (callId=${callId}, recipient=${recipientId}); sending one FCM attempt.`);
      await sendPushToUser(recipientId, {
        kind: 'incoming_call',
        callId,
        inviteId: snapshot.key,
        roomId: invite.roomId,
        callerId: invite.from,
        callerName: profile.name || invite.fromName || 'PairLink user',
        callerPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl, invite.fromPhotoDataUrl),
        video: action === 'video' ? 'true' : 'false'
      }, { ttlMs: 45_000, collapseKey: `call-${callId}`, kind: 'call', eventId: callId, consumeOnAttempt: true });
    } finally {
      // Exactly one wake-up attempt per call. All subsequent call control is
      // room-local Firebase signaling, never a second Render invite.
      await snapshot.ref.remove().catch((error) => logPushError('could not remove one-shot call invite', error));
    }
    return;
  }

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
  // Room session state already ends/rejects the live call on both devices.
  // Render must not send a second call-ended push. It only relays a true
  // missed-call timeout, which Firebase has explicitly marked as "missed".
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
      callerPhoto: compactPhoto(callerProfile.notificationAvatar, callerProfile.photoDataUrl),
      video: cancellation.video === true || callState.kind === 'video' ? 'true' : 'false'
    }, {
      ttlMs: 7 * 24 * 60 * 60 * 1000,
      collapseKey: `missed-${callId}`,
      kind: 'missed-call',
      eventId: callId
    });
  }
  if (missedPushSent > 0) {
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
  if (sent) await markProcessed(snapshot.ref);
}

async function processPendingCall(recipientId, snapshot) {
  const call = snapshot.val();
  try {
    if (!call || call.pushSentAt || !call.from || call.to !== recipientId || !call.roomId || !call.callId) return;
    const ageMs = Date.now() - Number(call.sentAt || call.createdAt || 0);
    if (!Number.isFinite(ageMs) || ageMs < -60_000 || ageMs > 60_000) return;

    // A pending inbox entry only asks Render to wake the receiver. Firebase's
    // shared room call session remains authoritative for whether it is ringing.
    const roomCall = (await db.ref(`rooms/${call.roomId}/callSessions/${call.callId}`).get()).val();
    if (!roomCall || roomCall.status !== 'ringing' ||
        String(roomCall.callerId || '') !== String(call.from) ||
        String(roomCall.recipientId || '') !== recipientId) return;

    const profile = await getPushProfile(call.from);
    const video = call.video === true || call.action === 'video' || call.kind === 'video';
    await sendPushToUser(recipientId, {
      kind: 'incoming_call',
      callId: String(call.callId),
      inviteId: String(call.id || snapshot.key),
      roomId: String(call.roomId),
      callerId: String(call.from),
      callerName: String(call.fromName || profile.name || 'PairLink user'),
      callerPhoto: compactPhoto(profile.notificationAvatar, profile.photoDataUrl, call.fromPhotoDataUrl),
      video: video ? 'true' : 'false'
    }, {
      ttlMs: 45_000,
      collapseKey: `call-${call.callId}`,
      kind: 'call',
      eventId: String(call.callId),
      consumeOnAttempt: true
    });
  } catch (error) {
    logPushError('one-shot call wake-up failed', error);
  } finally {
    // One inbox row produces at most one attempt, whether FCM accepts or fails.
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

async function recordPushDeliveryReceipt(input = {}) {
  if (!db) throw new Error('Firebase Admin push relay is not initialized');
  const recipientId = String(input.recipientId || '').slice(0, 180);
  const senderId = String(input.senderId || '').slice(0, 180);
  const roomId = String(input.roomId || '').slice(0, 200);
  const messageId = String(input.messageId || '').slice(0, 180);
  const deviceToken = String(input.deviceToken || '');
  if (!recipientId || !senderId || !roomId || !messageId || !deviceToken || deviceToken.length > 4096) return false;

  const recipientRef = db.ref(`users/${recipientId}`);
  const [tokensSnapshot, pendingSnapshot] = await Promise.all([
    recipientRef.child('pushTokens').get(),
    recipientRef.child(`pendingMessages/${messageId}`).get()
  ]);
  const tokenIsRegistered = Object.values(tokensSnapshot.val() || {}).some((record) => record?.token === deviceToken);
  if (!tokenIsRegistered) return false;

  const pendingMessage = pendingSnapshot.val();
  const pendingMatches = pendingMessage?.from === senderId && pendingMessage?.roomId === roomId && pendingMessage?.to === recipientId;
  if (!pendingMatches) return false;

  const receiptRef = db.ref(`users/${senderId}/messageReceipts/${messageId}`);
  const result = await receiptRef.transaction((current) => {
    if (current?.status === 'read' || current?.status === 'delivered') return current;
    return { messageId, roomId, status: 'delivered', updatedAt: Date.now() };
  });
  return result.committed;
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
    const watchedUsers = new Set();
    const watchedRooms = new Set();

    listenForChildAdded(db.ref('users'), 'user index', (userSnapshot) => {
      const userId = userSnapshot.key;
      if (!userId || watchedUsers.has(userId)) return;
      watchedUsers.add(userId);
      console.info(`[PairLink push] monitoring call/message inboxes for user ${userId}.`);
      listenForChildAdded(db.ref(`users/${userId}/pairInvites`), `invites ${userId}`, (snapshot) => processInvite(userId, snapshot));
      listenForChildAdded(db.ref(`users/${userId}/callCancellations`), `call cancellations ${userId}`, (snapshot) => processCallCancellation(userId, snapshot));
      listenForChildAdded(db.ref(`users/${userId}/pendingMessages`), `pending messages ${userId}`, (snapshot) => processPendingMessage(userId, snapshot));
    });
    listenForChildAdded(db.ref('rooms'), 'room index', (roomSnapshot) => {
      const roomId = roomSnapshot.key;
      if (!roomId || watchedRooms.has(roomId)) return;
      watchedRooms.add(roomId);
      listenForChildAdded(db.ref(`rooms/${roomId}/events`), `room events ${roomId}`, (snapshot) => processRoomEvent(roomId, snapshot));
    });
    console.info('[PairLink push] Render Firebase event relay started.');
    return true;
  } catch (error) {
    logPushError('could not start Firebase event relay', error);
    return false;
  }
}

module.exports = { startRenderPushRelay, recordPushDeliveryReceipt };
