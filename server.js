const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { startRenderPushRelay, recordPushDeliveryReceipt, routeCallRequest } = require('./render-push');

const app = express();
app.use(cors());
app.use(express.json({ limit: '8kb' }));

const server = http.createServer(app);

const io = new Server(server, {
  // 12MB limit ko badha kar 50MB kar rahe hain taaki HD photos/large base64 par disconnect na ho
  maxHttpBufferSize: 50 * 1024 * 1024,
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

app.get('/', (req, res) => {
  res.send('PairLink2 Backend is running!');
});

app.get('/healthz', (_req, res) => {
  res.status(200).json({ status: 'ok' });
});

app.post('/api/push/receipt', async (req, res) => {
  try {
    const recorded = await recordPushDeliveryReceipt(req.body);
    if (!recorded) return res.status(403).json({ ok: false, error: 'Push receipt could not be verified.' });
    return res.status(200).json({ ok: true });
  } catch (error) {
    console.error('[PairLink push] receipt endpoint failed:', error?.message || error);
    return res.status(503).json({ ok: false, error: 'Push receipt service is unavailable.' });
  }
});

app.post('/api/calls/route', async (req, res) => {
  try {
    const result = await routeCallRequest(req.body);
    return res.status(200).json({ ok: true, ...result });
  } catch (error) {
    const status = Number.isInteger(error?.status) ? error.status : 503;
    console.error('[PairLink call] Render routing request failed:', error?.message || error);
    return res.status(status).json({ ok: false, error: error?.message || 'Call routing is unavailable.' });
  }
});

// TURN credentials are supplied as short-lived ICE server credentials through
// the Render environment. Keep them out of the Firebase-hosted JavaScript.
app.get('/api/ice-servers', (req, res) => {
  res.set('Cache-Control', 'no-store');
  try {
    const parsed = JSON.parse(process.env.ICE_SERVERS || '[]');
    const servers = Array.isArray(parsed) ? parsed : parsed?.iceServers;
    const turnServers = Array.isArray(servers) ? servers.filter((entry) => {
      const urls = Array.isArray(entry?.urls) ? entry.urls : [entry?.urls];
      return urls.some((url) => typeof url === 'string' && /^turns?:/i.test(url)) &&
        typeof entry.username === 'string' && typeof entry.credential === 'string';
    }) : [];
    res.json({ iceServers: turnServers });
  } catch (error) {
    console.error('ICE_SERVERS must be valid JSON:', error.message);
    res.status(503).json({ iceServers: [] });
  }
});

// Locked Rooms Store
const roomsStore = {};
function destroyRoom(roomName, notify = true) {
  if (!roomName) return;
  const room = io.sockets.adapter.rooms.get(roomName);
  if (room && notify) io.to(roomName).emit('room_reset_kick');
  // Delete adapter membership too; deleting only roomsStore leaves stale
  // sockets that make the next join look like a second peer.
  if (room) io.in(roomName).socketsLeave(roomName);
  delete roomsStore[roomName];
  console.log(`Room "${roomName}" reset and removed.`);
}

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);
  let currentRoom = null;
  socket.data.pairlinkFileRooms = new Set();
  socket.data.pairlinkFileRoomUsers = new Map();

  function handleJoinRoom(data) {
    try {
      const roomName = (typeof data === 'object' && data !== null) ? data.room : data;
      const userId = (typeof data === 'object' && data !== null && data.userId) ? data.userId : socket.id;

      if (!roomName) return;
      socket.data.pairlinkUserId = String(userId);

      if (!roomsStore[roomName]) {
        roomsStore[roomName] = { allowedUsers: new Set([userId]) };
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

    } catch (err) {
      console.error("Error in handleJoinRoom:", err);
    }
  }

  socket.on('join_room', (data) => handleJoinRoom(data));
  socket.on('rejoin_room', (data) => handleJoinRoom(data));

  // Keep the app's active signaling room separate from background media
  // subscriptions. A client can receive files for saved conversations while
  // its UI is on Home, without joining the chat/call signaling flow.
  socket.on('join_file_rooms', (data) => {
    const userId = String(data?.userId || '');
    const rooms = Array.isArray(data?.rooms) ? data.rooms.slice(0, 100) : [];
    if (!userId) return;
    socket.data.pairlinkUserId = userId;
    const joined = [];
    for (const item of rooms) {
      const roomName = String(item?.roomId || item?.room || '');
      if (!roomName || roomName === currentRoom || roomName.length > 200) continue;
      const roomData = roomsStore[roomName];
      if (roomData && !roomData.allowedUsers.has(userId) && roomData.allowedUsers.size >= 2) {
        socket.emit('file_room_rejected', { room: roomName });
        continue;
      }
      if (!roomsStore[roomName]) {
        roomsStore[roomName] = { allowedUsers: new Set([userId]) };
      } else {
        roomsStore[roomName].allowedUsers.add(userId);
      }
      socket.join(roomName);
      socket.data.pairlinkFileRooms.add(roomName);
      socket.data.pairlinkFileRoomUsers.set(roomName, userId);
      joined.push(roomName);
      const roomSockets = io.sockets.adapter.rooms.get(roomName);
      if (roomSockets && roomSockets.size >= 2) {
        io.to(roomName).emit('user_status', { online: true });
        io.to(roomName).emit('peer-ready', { polite: false, userId });
      }
    }
    socket.emit('file_rooms_joined', { rooms: joined });
  });

  socket.on('leave_file_rooms', (data) => {
    const rooms = Array.isArray(data?.rooms) ? data.rooms : [];
    for (const value of rooms) {
      const roomName = String(value || '');
      if (!socket.data.pairlinkFileRooms.has(roomName)) continue;
      socket.leave(roomName);
      socket.data.pairlinkFileRooms.delete(roomName);
      socket.data.pairlinkFileRoomUsers.delete(roomName);
      const roomSockets = io.sockets.adapter.rooms.get(roomName);
      if (!roomSockets || roomSockets.size < 2) {
        socket.to(roomName).emit('user_status', { online: false });
        socket.to(roomName).emit('peer-left');
      }
    }
  });

  socket.on('file-transfer', (data) => {
    try {
      const requestedRoom = String(data?.room || currentRoom || '');
      const isCurrentRoom = requestedRoom && requestedRoom === currentRoom;
      const isSubscribedFileRoom = requestedRoom && socket.data.pairlinkFileRooms.has(requestedRoom);
      if (!requestedRoom || (!isCurrentRoom && !isSubscribedFileRoom)) return;
      socket.to(requestedRoom).emit('file-transfer', { ...data, room: requestedRoom });
    } catch (err) { console.error("File transfer error:", err); }
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

        // Socket.IO may still report the disconnecting socket in the room.
        if (!room || room.size <= 1) {
          delete roomsStore[currentRoom];
          console.log(`Room "${currentRoom}" empty. Lock deleted!`);
        }
      }
      for (const roomName of socket.data.pairlinkFileRooms || []) {
        const room = io.sockets.adapter.rooms.get(roomName);
        if (!room || room.size < 2) {
          socket.to(roomName).emit('user_status', { online: false });
          socket.to(roomName).emit('peer-left');
        }
      }
    } catch (err) { console.error("Disconnect error:", err); }
  });
});

// GLOBAL CRASH PREVENTER (Isse server kabhi crash nahi hoga)
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
