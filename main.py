import argparse
import asyncio
import sys

from Peer import Peer

HOST = "127.0.0.1"
BASE_PORT = 9000

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("id", type=int)
    #p.add_argument("--address", type=str, default=HOST, required=True)
    #p.add_argument("--peers", type=str, required=True)
    p.add_argument("-n", type=int,default=4)
    p.add_argument("--heartbeat", default=1, type=float)
    p.add_argument("--suspect", type=float, default=3)
    p.add_argument("--dead", type=float, default=6)
    args = p.parse_args()

    if not 1<=args.id<=args.n:
        p.error(f"ID must be in range 1 -{args.n}")
    addrs = {peerID: (HOST, BASE_PORT + peerID) for peerID in range(1, args.n + 1)}

    try:
        await Peer(args.id, addrs).run(args.heartbeat, args.suspect, args.dead)
    except OSError:
        print("Failed to start peer. Address is in use.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
