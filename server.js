const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors()); // Cross-Origin Resource Sharing allow karne ke liye

const server = http.createServer(app);

// CORS settings taake aapka Firebase frontend is backend se baat kar sakay
const io = new Server(server, {
  cors: {
    origin: "*", // Production mein isay apne Firebase URL se replace kar sakte hain
    methods: ["GET", "POST"]
  }
});

// Render server ko zinda rakhne aur check karne ke liye basic route
app.get('/', (req, res) => {
  res.send('PairLink2 Backend is running!');
});

io.on('connection', (socket) => {
  console.log(`User connected: ${socket.id}`);
  const activeSockets = io.sockets.sockets.size;

  // Sirf 2 users ko allow karein
  if (activeSockets > 2) {
    socket.emit('room_full'); 
    socket.disconnect(true);
    return;
  }

  // Pehle user ko batayein ke dusra user aa gaya hai
  socket.broadcast.emit('user_status', { online: true });

  // Roles assign karein jab dono users connect ho jayein
  if (activeSockets === 2) {
    socket.emit('user_status', { online: true });
    socket.emit('peer-ready', { polite: true });
    socket.broadcast.emit('peer-ready', { polite: false });
  }

  // WebRTC aur Chat ka data ek dusre ko bhejna
  socket.on('signal', (data) => { 
    socket.broadcast.emit('signal', data); 
  });
  
  socket.on('chat-message', (data) => { 
    socket.broadcast.emit('chat-message', data); 
  });
  
  socket.on('file-transfer', (data) => { 
    socket.broadcast.emit('file-transfer', data); 
  });
  
  socket.on('reset_room', () => { 
    io.emit('room_reset_kick'); 
  });

  // User ke disconnect hone par handle karna
  socket.on('disconnect', () => {
    console.log(`User disconnected: ${socket.id}`);
    socket.broadcast.emit('user_status', { online: false });
    socket.broadcast.emit('peer-left');
  });
});

// Port Render khud assign karta hai, warna 3000 use hoga
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
