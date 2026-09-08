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

// Do users ko track karne ke liye array
let activeUsers = [];

io.on("connection", (socket) => {
  console.log("User Connected:", socket.id);

  // Agar room me pehle se 2 log hain toh teesre ko block karein
  if (activeUsers.length >= 2) {
    socket.emit("room_full");
    socket.disconnect(true);
    return;
  }

  activeUsers.push(socket.id);

  // Jab 2 users connect ho jayein toh dono ko Online status aur WebRTC role assign karein
  if (activeUsers.length === 2) {
    io.emit("user_status", { online: true });

    // Polite / Impolite peer assignment (WebRTC collision se bachne ke liye)
    io.to(activeUsers[0]).emit("peer-ready", { polite: false });
    io.to(activeUsers[1]).emit("peer-ready", { polite: true });
  } else {
    // Sirf 1 user hone par Offline status dikhayenge
    socket.emit("user_status", { online: false });
  }

  // 1. WebRTC Signaling relay (Offer, Answer, ICE Candidates)
  socket.on("signal", (data) => {
    socket.broadcast.emit("signal", data);
  });

  // 2. Chat messaging relay
  socket.on("chat-message", (msg) => {
    socket.broadcast.emit("chat-message", msg);
  });

  // 3. Media / File transfer relay
  socket.on("file-transfer", (mediaData) => {
    socket.broadcast.emit("file-transfer", mediaData);
  });

  // Disconnect hone par cleanup
  socket.on("disconnect", () => {
    console.log("User Disconnected:", socket.id);
    activeUsers = activeUsers.filter((id) => id !== socket.id);

    // Bacha hua user wapas offline status dekhega
    socket.broadcast.emit("peer-left");
    socket.broadcast.emit("user_status", { online: false });
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
