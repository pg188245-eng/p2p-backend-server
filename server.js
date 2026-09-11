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
  res.send('PairLink2 Multi-Room Backend is running!');
});

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);

  // User jab room name submit kare
  socket.on('join_room', (roomName) => {
    if (!roomName) return;

    const room = roomName.trim().toLowerCase();

    // Check room active users count
    const clientsInRoom = io.sockets.adapter.rooms.get(room);
    const numClients = clientsInRoom ? clientsInRoom.size : 0;

    // Room mein 2 log pehle se hain
    if (numClients >= 2) {
      socket.emit('room_full', { room });
      return;
    }

    socket.join(room);
    socket.currentRoom = room;

    console.log(`User ${socket.id} joined room: "${room}" (Total: ${numClients + 1})`);

    // Pehla user connected
    if (numClients === 0) {
      socket.emit('room_joined', { room, isInitiator: true });
      socket.emit('user_status', { online: false });
    } 
    // Dusra user connected
    else if (numClients === 1) {
      socket.emit('room_joined', { room, isInitiator: false });
      
      io.in(room).emit('user_status', { online: true });

      // Perfect Negotiation roles
      socket.emit('peer-ready', { polite: true });
      socket.to(room).emit('peer-ready', { polite: false });
    }
  });

  // Relay signals exclusively to the same room
  socket.on('signal', (data) => {
    if (socket.currentRoom) {
      socket.to(socket.currentRoom).emit('signal', data);
    }
  });

  socket.on('chat-message', (data) => {
    if (socket.currentRoom) {
      socket.to(socket.currentRoom).emit('chat-message', data);
    }
  });

  socket.on('file-transfer', (data) => {
    if (socket.currentRoom) {
      socket.to(socket.currentRoom).emit('file-transfer', data);
    }
  });

  socket.on('reset_room', () => {
    if (socket.currentRoom) {
      io.in(socket.currentRoom).emit('room_reset_kick');
    }
  });

  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
    if (socket.currentRoom) {
      socket.to(socket.currentRoom).emit('user_status', { online: false });
      socket.to(socket.currentRoom).emit('peer-left');
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
