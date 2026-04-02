import express from 'express'
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import http from 'http'
import { Server } from 'socket.io'
import { open } from 'sqlite';
import sqlite3 from 'sqlite3'
import { availableParallelism } from 'os';
import cluster from 'cluster';
import { createAdapter, setupPrimary } from '@socket.io/cluster-adapter';

const messages = [];
const lastSeen = new Map();
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

if (cluster.isPrimary) {
    const numCPUs = availableParallelism();

    for (let index = 0; index < numCPUs; index++) {
        cluster.fork({
            PORT: 3000 + index,
        })
    }
    setupPrimary();
} else {
    const app = express()
    const server = http.createServer(app)
    const io = new Server(server, {
        connectionStateRecovery: {},
        adapter: createAdapter(),
    })

    app.use(express.static(join(__dirname, "public")))

    app.get("/", (req, res) => {
        return res.sendFile(join(__dirname, "index.html"))
    })

    io.on('connection', async (socket) => {


        socket.on('private message', async (toNickname, message, callback) => {
            try {
                const sockets = await io.fetchSockets();

                // find target user socket
                const target = sockets.find(s => s.data.nickname === toNickname);

                if (!target) {
                    return callback && callback("User not found");
                }

                // send ONLY to that user
                io.to(target.id).emit('private message', `${socket.data.nickname} (private): ${message}`);

                callback && callback("OK");

            } catch (err) {
                console.log(err);
                callback && callback("error");
            }
        });

        async function sendOnlineUsers() {
            const sockets = await io.fetchSockets();

            const users = sockets
                .map(s => s.data.nickname)
                .filter(Boolean);

            io.emit('online users', users)
        }
        socket.on('set nickname', (nickname) => {
            socket.data.nickname = nickname;
            sendOnlineUsers();
        })


        socket.on('typing', (nickname) => {
            socket.broadcast.emit('typing', nickname)
        })
        socket.on('stop typing', (nickname) => {
            socket.broadcast.emit('stop typing', nickname)
        })
        socket.on('chat message', async (msg, clientoffset, nickname, callback) => {
            let result;
            try {
                result = await db.run('INSERT INTO messages (content, client_offset) VALUES (?, ? )', [`${nickname}: ${msg}`, clientoffset]);
            } catch (error) {
                console.log(error);
                return callback("error");
            }
            socket.broadcast.emit('chat message', `${nickname}: ${msg}`, result.lastID)
            if (callback) callback("OK");
            // const messageWithId = `${socket.id}: ${msg}`;
            // messages.push(messageWithId);
            // io.emit('chat message', messageWithId);
        });


        if (!socket.recovered) {
            try {
                await db.each('SELECT id, content FROM messages WHERE id > ?', [socket.handshake.auth.serverOffset || 0], (_err, row) => {
                    socket.emit('chat message', row.content, row.id)
                })
            } catch (error) {
                return;
            }
        }

    });

    const db = await open({
        filename: 'chat.db',
        driver: sqlite3.Database
    });
    await db.exec(`
    CREATE TABLE If NOT EXISTS messages(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_offset TEXT UNIQUE,
    content TEXT);
    `
    );

    const port = process.env.PORT;
    server.listen(port, () => {
        console.log(`Server is running on port ${port}`)
    });

}


