const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

// Static files serve करे खातिर
app.use(express.static(path.join(__dirname, 'public')));

// Root route
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;

// Socket.io Signaling & Events
io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);

  // 2 गो से बेसी यूजर अइला पर Room Full भेजल जाई
  const activeSockets = io.sockets.sockets.size;
  if (activeSockets > 2) {
    socket.emit('room-full');
    socket.disconnect(true);
    console.log(`Connection rejected for ${socket.id}: Room is full`);
    return;
  }

  // Chat message relay
  socket.on('chat-message', (data) => {
    socket.broadcast.emit('chat-message', data);
  });

  // WebRTC Offer relay
  socket.on('offer', (offer) => {
    socket.broadcast.emit('offer', offer);
  });

  // WebRTC Answer relay
  socket.on('answer', (answer) => {
    socket.broadcast.emit('answer', answer);
  });

  // ICE Candidate relay
  socket.on('ice-candidate', (candidate) => {
    socket.broadcast.emit('ice-candidate', candidate);
  });

  // End Call signal
  socket.on('end-call', () => {
    socket.broadcast.emit('end-call');
  });

  // Disconnect handling
  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
    socket.broadcast.emit('end-call');
  });
});

server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
