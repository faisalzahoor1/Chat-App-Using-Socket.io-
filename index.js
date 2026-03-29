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

import { open } from 'sqlite';
import sqlite3 from 'sqlite3'

const db = await open({
    filename: 'chat.db',
    driver: sqlite3.Database
});
await db.exec(`
    CREATE TABLE If NOT EXISTS messages(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content TEXT);
    `
);
const io = new Server(server, {
    connectionStateRecovery: {}
})

app.use(express.static(join(__dirname, "public")))

app.get("/", (req, res) => {
    return res.sendFile(join(__dirname, "index.html"))
})

io.on('connection', async(socket) => {
    socket.on('chat message', async(msg) => {
        let result;
        try {
            result = await db.run('INSERT INTO messages (content) VALUES (?)', msg);
        } catch (error) {
            console.log(error);
            return;
        }
        io.emit('chat message', msg, result.lastID)
        // const messageWithId = `${socket.id}: ${msg}`;
        // messages.push(messageWithId);
        // io.emit('chat message', messageWithId);
    });
    if (!socket.recovered) {
        try {
            await db.each('SELECT id, content FROM messages WHERE id > ?', [socket.handshake.auth.serverOffset || 0], (_err, row)=>{
                socket.emit('chat message', row.content, row.id)
            })
        } catch (error) {
            return;
        }
    }

});

server.listen(3000, () => {
    console.log(`Server is running on port 3000`)
})