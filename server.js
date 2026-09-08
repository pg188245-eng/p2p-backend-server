const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*", // Firebase domain se request allow karne ke liye
    methods: ["GET", "POST"]
  }
});

app.get("/", (req, res) => {
  res.send("P2P Backend Server Active!");
});

const rooms = {};

io.on("connection", (socket) => {
  console.log("User Connected:", socket.id);

  // Room Join & Presence logic
  socket.on("join-room", (roomId) => {
    socket.join(roomId);

    if (!rooms[roomId]) {
      rooms[roomId] = [];
    }
    rooms[roomId].push(socket.id);

    // Jab doosra user room join kare toh Online status emit karein
    if (rooms[roomId].length >= 2) {
      io.to(roomId).emit("user-connected", {
        status: "Online",
        users: rooms[roomId]
      });
    }
  });

  // WebRTC Signaling (Call / Media)
  socket.on("offer", (data) => socket.to(data.roomId).emit("offer", data));
  socket.on("answer", (data) => socket.to(data.roomId).emit("answer", data));
  socket.on("ice-candidate", (data) => socket.to(data.roomId).emit("ice-candidate", data));

  // Chat message relay
  socket.on("send-message", (data) => socket.to(data.roomId).emit("receive-message", data));

  // Disconnect & Offline Status
  socket.on("disconnect", () => {
    for (const roomId in rooms) {
      const index = rooms[roomId].indexOf(socket.id);
      if (index !== -1) {
        rooms[roomId].splice(index, 1);
        io.to(roomId).emit("user-disconnected", { status: "Offline" });
        if (rooms[roomId].length === 0) delete rooms[roomId];
      }
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));