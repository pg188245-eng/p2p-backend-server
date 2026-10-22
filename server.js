const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { startRenderPushRelay } = require('./render-push');

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

  function touchRoomActivity(roomName) {
    if (roomsStore[roomName]) {
      roomsStore[roomName].lastActivityAt = Date.now();
    }
  }

  function handleJoinRoom(data) {
    try {
      const roomName = (typeof data === 'object' && data !== null) ? data.room : data;
      const userId = (typeof data === 'object' && data !== null && data.userId) ? data.userId : socket.id;

      if (!roomName) return;
      socket.data.pairlinkUserId = String(userId);

      const existingRoom = roomsStore[roomName];
      if (existingRoom && Date.now() - existingRoom.lastActivityAt >= ROOM_EXPIRY_MS) {
        delete roomsStore[roomName];
      }

      if (!roomsStore[roomName]) {
        roomsStore[roomName] = { allowedUsers: new Set([userId]), lastActivityAt: Date.now() };
      } else if (!roomsStore[roomName].allowedUsers.has(userId)) {
        if (roomsStore[roomName].allowedUsers.size >= 2) {
          socket.emit('room_locked_error', { 
            message: 'This room is locked for its two registered users. A third user cannot join.' 
          });
          return;
        } else {
          roomsStore[roomName].allowedUsers.add(userId);
        }
      }

      socket.join(roomName);
      currentRoom = roomName;
      socket.emit('room_joined', { room: roomName });

      const room = io.sockets.adapter.rooms.get(roomName);
      const socketsInRoom = room ? Array.from(room) : [];

      if (socketsInRoom.length >= 2) {
        io.to(roomName).emit('user_status', { online: true });
        
        const s1 = socketsInRoom[socketsInRoom.length - 2];
        const s2 = socketsInRoom[socketsInRoom.length - 1];
        
        io.to(s1).emit('peer-ready', { polite: false });
        io.to(s2).emit('peer-ready', { polite: true });
      } else {
        socket.emit('user_status', { online: false });
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
    socket.join(accountRelayRoom(userId));
    socket.emit('user_relay_joined', { userId });
  });

  // --- SAFE BROADCAST HANDLERS (Crash Safe) ---
  socket.on('signal', (data) => {
    try {
      if (currentRoom) {
        touchRoomActivity(currentRoom);
        socket.to(currentRoom).emit('signal', data);
      }
    } catch (err) { console.error("Signal error:", err); }
  });
  
  socket.on('chat-message', (data, acknowledge) => {
    try {
      const room = (typeof data === 'object' && data?.room) ? data.room : currentRoom;
      if (room) {
        touchRoomActivity(room);
        const recipientId = String(data?.recipientId || data?.encrypted?.recipient || '');
        if (!recipientId) {
          socket.to(room).emit('chat-message', data);
          if (typeof acknowledge === 'function') acknowledge({ routed: true, transport: 'room' });
          return;
        }
        const roomSocketIds = io.sockets.adapter.rooms.get(room) || new Set();
        const recipientInRoom = [...roomSocketIds].some((socketId) =>
          socketId !== socket.id && io.sockets.sockets.get(socketId)?.data?.pairlinkUserId === recipientId
        );
        if (recipientInRoom) {
          socket.to(room).emit('chat-message', data);
          if (typeof acknowledge === 'function') acknowledge({ routed: true, transport: 'room' });
          return;
        }
        const recipientRelayRoom = accountRelayRoom(recipientId);
        const recipientSocketIds = io.sockets.adapter.rooms.get(recipientRelayRoom);
        if (!recipientSocketIds?.size) {
          if (typeof acknowledge === 'function') acknowledge({ routed: false, transport: 'unavailable' });
          return;
        }
        io.to(recipientRelayRoom).timeout(10_000).emit('user-chat-message', {
          id: String(data?.id || ''),
          roomId: String(room),
          from: String(socket.data.pairlinkUserId || data?.senderId || data?.encrypted?.sender || ''),
          to: recipientId,
          fromName: String(data?.senderName || 'PairLink user'),
          sentAt: Number(data?.sentAt) || Date.now(),
          envelope: data
        }, (error, responses) => {
          if (typeof acknowledge === 'function') {
            acknowledge({ routed: !error && (responses || []).some((response) => response?.stored === true), transport: 'user' });
          }
        });
      }
    } catch (err) { console.error("Chat message error:", err); }
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
