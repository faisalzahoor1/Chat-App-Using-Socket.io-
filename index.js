import express from 'express'
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import http from 'http'
import { Server } from 'socket.io'

const messages = [];
const lastSeen = new Map();
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const app = express()
const server = http.createServer(app)

const io = new Server(server, {
    connectionStateRecovery: {}
})

app.use(express.static(join(__dirname, "public")))

app.get("/", (req, res) => {
    return res.sendFile(join(__dirname, "index.html"))
})

io.on('connection', (socket) => {
    socket.on('chat message', (msg) => {
        const messageWithId = `${socket.id}: ${msg}`;
        messages.push(messageWithId);
        io.emit('chat message', messageWithId);
    });


});

server.listen(3000, () => {
    console.log(`Server is running on port 3000`)
})