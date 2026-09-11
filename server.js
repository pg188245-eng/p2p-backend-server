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
    res.send('PairLink Backend Server is running...');
});

io.on('connection', (socket) => {
    console.log('User connected:', socket.id);

    // Password validation aur Dynamic Room logic[cite: 7]
    socket.on('join-room', (password) => {
        if (!password || typeof password !== 'string') return;
        const cleanPass = password.trim();
        if (!cleanPass) return;

        const roomId = `room_${cleanPass}`;
        const currentRoom = io.sockets.adapter.rooms.get(roomId);
        const numClients = currentRoom ? currentRoom.size : 0;

        // 1. CHANCE: Room nahi hai (0 users) -> Naya Room Banao
        if (numClients === 0) {
            socket.join(roomId);
            socket.roomId = roomId;
            socket.userPassword = cleanPass;
            socket.isPolite = false; // Pehla user (Impolite)[cite: 7]

            socket.emit('room_created', {
                roomId: roomId,
                password: cleanPass,
                isPolite: false
            });
            console.log(`Naya room bana pass '${cleanPass}' ke sath: ${socket.id}`);
        } 
        // 2. CHANCE: Room pehle se hai aur 1 banda hai -> Join Karo
        else if (numClients === 1) {
            socket.join(roomId);
            socket.roomId = roomId;
            socket.userPassword = cleanPass;
            socket.isPolite = true; // Doosra user (Polite)[cite: 7]

            socket.emit('room_joined', {
                roomId: roomId,
                password: cleanPass,
                isPolite: true
            });

            // Dono users ko connection start karne ka signal bhejo
            io.to(roomId).emit('user_connected', { numClients: 2 });
            console.log(`User ${socket.id} joined room '${cleanPass}'`);
        } 
        // 3. CHANCE: Room full hai (2 log pehle se hain) -> Block Karo[cite: 7]
        else {
            socket.emit('room_full', 'Yeh room full ho chuka hai! Kisi aur password se try karein.');
            console.log(`Room '${cleanPass}' full hai. ${socket.id} reject hua.`);
        }
    });

    // WebRTC Signaling Data Routing[cite: 7]
    socket.on('signal', (data) => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit('signal', data);
        }
    });

    // Disconnect Handler: User ke nikalte hi room khali kar do
    socket.on('disconnect', () => {
        if (socket.roomId) {
            // Room me majood doosre user ko disconnect alert bhejo
            socket.to(socket.roomId).emit('user_disconnected');
            // Socket ko room se leave karwa do
            socket.leave(socket.roomId);
        }
        console.log('User disconnected:', socket.id);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
