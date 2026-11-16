import json
import struct

#FTYPE 
HELLO, PING, PONG, MSG, ACK = range(1, 6)
#Hello - initiate handshake
#Ping - check if peer is alive
#Pong - confirm peer is alive
#MSG - Send a message
#ACK - Acknowledge receivement of a message

def encode(ftype, payload):
    data = json.dumps(payload).encode()
    return struct.pack(">BI", ftype, len(data)) + data

async def readFrame(reader):
    ftype, n = struct.unpack(">BI", await reader.readexactly(5))
    if n > 1 << 20:
        raise ValueError("Frame too large")
    return ftype, json.loads(await reader.readexactly(n))