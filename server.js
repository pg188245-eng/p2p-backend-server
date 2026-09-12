const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
  // Chat media is sent as base64 over Socket.IO. Keep this below the
  // app's 8 MB file limit while allowing normal photos and documents.
  maxHttpBufferSize: 12 * 1024 * 1024,
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

app.get('/', (req, res) => {
  res.send('PairLink2 Backend is running!');
});

// Locked Rooms Store: Tracks permanent user IDs allowed in each room
const roomsStore = {};
const ROOM_EXPIRY_MS = 3 * 24 * 60 * 60 * 1000;

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

  // Strict Room Locking & Joining Logic
  function handleJoinRoom(data) {
    const roomName = typeof data === 'object' ? data.room : data;
    const userId = (typeof data === 'object' && data.userId) ? data.userId : socket.id;

    if (!roomName) return;

    const existingRoom = roomsStore[roomName];
    if (existingRoom && Date.now() - existingRoom.lastActivityAt >= ROOM_EXPIRY_MS) {
      delete roomsStore[roomName];
    }

    // 1. Check & Apply Room Lock Logic
    if (!roomsStore[roomName]) {
      // Pehla User: Room create hua
      roomsStore[roomName] = { allowedUsers: new Set([userId]), lastActivityAt: Date.now() };
    } else if (!roomsStore[roomName].allowedUsers.has(userId)) {
      // Agar naya user enter karne ki koshish kar raha hai
      if (roomsStore[roomName].allowedUsers.size >= 2) {
        // Room me pehle se 2 locked users maujood hain -> Reject 3rd User
        socket.emit('room_locked_error', { 
          message: 'This room is locked for its two registered users. A third user cannot join.' 
        });
        return;
      } else {
        // Doosra User: Register karo aur room ko lock kar do
        roomsStore[roomName].allowedUsers.add(userId);
      }
    }

    // 2. Allow Joining
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

    roomsStore[roomName].lastActivityAt = Date.now();
  }

  // Manual Room Join
  socket.on('join_room', (data) => {
    handleJoinRoom(data);
  });

  // Auto-Rejoin Logic
  socket.on('rejoin_room', (data) => {
    handleJoinRoom(data);
  });

  // Data Broadcast Handlers
  socket.on('signal', (data) => {
    if (currentRoom) {
      socket.to(currentRoom).emit('signal', data);
    }
  });
  
  socket.on('chat-message', (data) => {
    if (currentRoom) {
      socket.to(currentRoom).emit('chat-message', data);
    }
  });
  
  socket.on('file-transfer', (data) => {
    if (currentRoom) {
      socket.to(currentRoom).emit('file-transfer', data);
    }
  });
  
  socket.on('reset_room', () => {
    if (currentRoom) {
      delete roomsStore[currentRoom];
      io.to(currentRoom).emit('room_reset_kick');
    }
  });

  // --- NAYA CODE: Manual Leave Room (Taki spot khali ho aur naya room ban sake) ---
  socket.on('leave_room', (data) => {
    if (currentRoom) {
      const userId = data.userId;
      
      // Socket ko room se bahar nikalo
      socket.leave(currentRoom);
      const remainingAfterLeave = io.sockets.adapter.rooms.get(currentRoom);
      // A browser refresh can create the replacement socket before the old
      // socket finishes leaving. Do not announce offline while two live
      // sockets are still present in the room.
      if (!remainingAfterLeave || remainingAfterLeave.size < 2) {
        socket.to(currentRoom).emit('user_status', { online: false });
        socket.to(currentRoom).emit('peer-left');
      }

      // Room ke lock (Set) mein se sirf IS user ki ID remove karo
      if (roomsStore[currentRoom] && userId) {
        roomsStore[currentRoom].allowedUsers.delete(userId);
        
        // Agar room mein koi allowed user nahi bacha, toh room ka lock poora delete kar do
        if (roomsStore[currentRoom].allowedUsers.size === 0) {
          delete roomsStore[currentRoom];
          console.log(`Room "${currentRoom}" ke sabhi users leave kar gaye. Lock completely deleted!`);
        } else {
          console.log(`User ${userId} left room "${currentRoom}". 1 spot free!`);
        }
      }

      currentRoom = null;
    }
  });
  // ----------------------------------------------------------------------------------

  // Disconnect Handling & Lock Cleanup
  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
    if (currentRoom) {
      // Jab dono users disconnect ho jayen (Active Sockets = 0), toh Room Lock Delete kar do
      const room = io.sockets.adapter.rooms.get(currentRoom);
      // Ignore stale disconnects caused by a refresh when the replacement
      // socket and its partner are still connected.
      if (!room || room.size < 2) {
        socket.to(currentRoom).emit('user_status', { online: false });
        socket.to(currentRoom).emit('peer-left');
      }
      if (!room || room.size === 0) {
        delete roomsStore[currentRoom];
        console.log(`Room "${currentRoom}" khali ho gaya. Lock deleted!`);
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
