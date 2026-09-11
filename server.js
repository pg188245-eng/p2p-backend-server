const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

app.get('/', (req, res) => {
  res.send('PairLink2 Backend is running!');
});

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);
  let currentRoom = null;

  // Manual Room Join
  socket.on('join_room', (roomName) => {
    const room = io.sockets.adapter.rooms.get(roomName);
    const numClients = room ? room.size : 0;

    if (numClients >= 2) {
      socket.emit('room_full', { room: roomName });
      return;
    }

    socket.join(roomName);
    currentRoom = roomName;
    socket.emit('room_joined', { room: roomName });

    const updatedRoom = io.sockets.adapter.rooms.get(roomName);
    const count = updatedRoom ? updatedRoom.size : 0;

    if (count === 2) {
      io.to(roomName).emit('user_status', { online: true });
      const socketsInRoom = Array.from(updatedRoom);
      io.to(socketsInRoom[0]).emit('peer-ready', { polite: false });
      io.to(socketsInRoom[1]).emit('peer-ready', { polite: true });
    } else {
      socket.emit('user_status', { online: false });
    }
  });

  // Auto-Rejoin Logic (localStorage)
  socket.on('rejoin_room', (roomName) => {
    const room = io.sockets.adapter.rooms.get(roomName);
    const numClients = room ? room.size : 0;

    // Room khali hone par expired popup
    if (numClients === 0) {
      socket.emit('room_expired');
      return;
    }

    // Room full hone par alert
    if (numClients >= 2) {
      socket.emit('room_full', { room: roomName });
      return;
    }

    // Single user hone par direct rejoin
    socket.join(roomName);
    currentRoom = roomName;
    socket.emit('room_joined', { room: roomName });

    const updatedRoom = io.sockets.adapter.rooms.get(roomName);
    if (updatedRoom && updatedRoom.size === 2) {
      io.to(roomName).emit('user_status', { online: true });
      const socketsInRoom = Array.from(updatedRoom);
      io.to(socketsInRoom[0]).emit('peer-ready', { polite: false });
      io.to(socketsInRoom[1]).emit('peer-ready', { polite: true });
    }
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
      io.to(currentRoom).emit('room_reset_kick');
    }
  });

  // Disconnect Handling
  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
    if (currentRoom) {
      socket.to(currentRoom).emit('user_status', { online: false });
      socket.to(currentRoom).emit('peer-left');
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
