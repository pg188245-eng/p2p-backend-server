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

    // Dynamic Password Room Join Logic[cite: 7]
    socket.on('join-room', (password) => {
        if (!password || typeof password !== 'string') return;
        const cleanPass = password.trim();
        if (!cleanPass) return;

        const roomId = `room_${cleanPass}`;
        const currentRoom = io.sockets.adapter.rooms.get(roomId);
        const numClients = currentRoom ? currentRoom.size : 0;

        // 1. Room nahi hai -> Create new room[cite: 7]
        if (numClients === 0) {
            socket.join(roomId);
            socket.roomId = roomId;
            socket.userPassword = cleanPass;
            socket.isPolite = false; // Master User[cite: 7]

            socket.emit('room_created', {
                roomId: roomId,
                password: cleanPass,
                isPolite: false
            });
            console.log(`Room created with password '${cleanPass}': ${socket.id}`);
        } 
        // 2. Room me 1 banda hai -> Join existing room[cite: 7]
        else if (numClients === 1) {
            socket.join(roomId);
            socket.roomId = roomId;
            socket.userPassword = cleanPass;
            socket.isPolite = true; // Polite User[cite: 7]

            socket.emit('room_joined', {
                roomId: roomId,
                password: cleanPass,
                isPolite: true
            });

            // Room me dono users ko connectivity ka signal bhejo
            io.to(roomId).emit('user_connected', { numClients: 2 });
            console.log(`User ${socket.id} joined room '${cleanPass}'`);
        } 
        // 3. Room full hai (2 users pehle se hain) -> Reject[cite: 7]
        else {
            socket.emit('room_full', 'Yeh room full ho chuka hai! Kisi aur password se try karein.');
            console.log(`Rejected ${socket.id} from full room '${cleanPass}'`);
        }
    });

    // WebRTC Signaling Data Exchange[cite: 7]
    socket.on('signal', (data) => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit('signal', data);
        }
    });

    // Chat Message Relay
    socket.on('chat_message', (data) => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit('chat_message', data);
        }
    });

    // Disconnect Handler: User ke nikalte hi room dismantle kar do
    socket.on('disconnect', () => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit('user_disconnected');
            socket.leave(socket.roomId);
        }
        console.log('User disconnected:', socket.id);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
