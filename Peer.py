import asyncio
import os
import signal
import sys
import threading
import time

from protocol import encode, readFrame, HELLO, PING, PONG, MSG, ACK

class Peer:
    def __init__(self, _id, _address):
        self.id = _id
        self.address = _address
        self.connections = {} #All active connections
        self.state = { i: "DEAD" for i in self.otherPeers} #Possible states: DEAD, SUSPECT, ALIVE


        #Dictionary that maps other peers to the last time this peer heard from them
        self.lastPong = dict.fromkeys(self.otherPeers, 0.0)

        self.outbox = {peerID: {} for peerID in self.otherPeers}
        self.seen = set()
        #Terminate peer event
        self.terminate = asyncio.Event()

    @property
    def otherPeers(self):
        return [p for p in self.address if p != self.id]
    
    def sendFrame(self, peerID, ftype, payload):
        if writer := self.connections.get(peerID):
            writer.write(encode(ftype, payload))

    async def dial(self, peerID):
        delay = 0.2
        while True:
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(*self.address[peerID]), 3)
            except (OSError, asyncio.TimeoutError):
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5)
                continue
            delay = 0.2
            await self.link(reader, writer, expect=peerID)
            await asyncio.sleep(0.2)

    async def link(self, reader, writer, expect=None):

        writer.write(encode(HELLO, {"id": self.id}))

        try:
            ftype, h = await asyncio.wait_for(readFrame(reader), 5)
            peerID = h["id"]
            wrongPeer = peerID != expect if expect else peerID > self.id
            if ftype != HELLO or peerID not in self.otherPeers or wrongPeer:
                raise ValueError("Unexpected peer")
        except Exception:
            writer.close()
            return
        
        if staleConnection := self.connections.get(peerID):
            staleConnection.close()
        self.connections[peerID] = writer
        self.hear(peerID)

        for i in list(self.outbox[peerID].values()):
            self.sendFrame(peerID, MSG, i)

        try:
            while True:
                ftype, h = await readFrame(reader)
                self.hear(peerID)
                self.handleMessage(peerID, ftype, h)
        except (asyncio.IncompleteReadError, ConnectionError, OSError, ValueError, KeyError):
            pass
        finally:
            writer.close()
            if self.connections.get(peerID) is writer:
                del self.connections[peerID]
                if self.state[peerID] == "ALIVE":
                    self.setState(peerID, "SUSPECT")


    def hear(self, peerID):
        self.lastPong[peerID] = time.monotonic()
        self.setState(peerID, "ALIVE")

    def setState(self, peerID, _state):
        self.state[peerID] = _state

    def handleMessage(self, peerID, ftype, payload):
        if ftype == PING:
            self.sendFrame(peerID, PONG, {})
        elif ftype == MSG:
            if payload["id"] not in self.seen:
                self.seen.add(payload["id"])
                print(f"New message from {peerID}: {payload['text']}")
            self.sendFrame(peerID, ACK, {"id": payload["id"]})
        elif ftype == ACK:
            self.outbox[peerID].pop(payload["id"], None)

    def sendMessage(self, peerID, text):
        msg = {"id": os.urandom(6).hex(), "text": text}
        self.outbox[peerID][msg["id"]] = msg
        self.sendFrame(peerID, MSG, msg)

    def CLIcontrols(self, txt):
        command, _, rest = txt.partition(" ")
        if command == "/peers":
            for peerID in self.otherPeers:
                status = "connected" if peerID in self.connections else "disconnected"
                print(f"{peerID}: {self.state[peerID]} {status}")
        elif command == "/message":
            recepient, _, message = rest.partition(" ")
            if recepient.isdigit() and int(recepient) in self.outbox and message:
                self.sendMessage(int(recepient), message)
            else:
                print("Invalid arguements for /message")
        elif command == "/quit":
            self.terminate.set()
        elif txt:
            print("Unknown command")
            print("Commands available: /peers, /message(<peerID> <message>), /quit")

    async def CLI(self):
        loop, inputQueue = asyncio.get_running_loop(), asyncio.Queue()

        def readSTDin():
            try:
                for line in sys.stdin:
                    loop.call_soon_threadsafe(inputQueue.put_nowait, line.strip())
            finally:
                loop.call_soon_threadsafe(inputQueue.put_nowait, None)
        threading.Thread(target=readSTDin, daemon=True).start()
        while (text := await inputQueue.get()) is not None:
            try:
                self.CLIcontrols(text)
            except Exception as e:
                print(f"Error: {e}")
        if sys.stdin and sys.stdin.isatty():
            self.terminate.set()
        else:
            print("stdin is closed. Ctrl+C to exit.")

    async def detector(self, heartbeat, suspect, dead):
        while not self.terminate.is_set():
            await asyncio.sleep(heartbeat)
            now = time.monotonic()
            for peerID in self.otherPeers:
                self.sendFrame(peerID, PING, {})
                silence = now - self.lastPong[peerID]
                if self.state[peerID] == "ALIVE" and silence > suspect:
                    self.setState(peerID, "SUSPECT")
                if silence > dead:
                    self.setState(peerID, "DEAD")

    async def run(self, heartbeat, suspect, dead):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.terminate.set) #Register signal handler for terminate event
        server = await asyncio.start_server(self.link, *self.address[self.id])

        tasks = [asyncio.create_task(self.dial(peerID)) for peerID in self.otherPeers if peerID > self.id] #Every peer connects only to peers with a higher ID
        tasks.append(asyncio.create_task(self.detector(heartbeat, suspect, dead)))
        tasks.append(asyncio.create_task(self.CLI()))

        await self.terminate.wait() #Wait for terminate event
        server.close()
        for t in tasks:
            t.cancel()
        for value in self.connections.values():
            value.close()
        
