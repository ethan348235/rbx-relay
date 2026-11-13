# relay.py — render.com free web service: websocket relay, same protocol.
# First binary frame = [2-byte len][domain], then raw bytes both ways.
# Allowlist fail-closed. Roblox TLS stays end-to-end through this pipe.
import asyncio
import os

import websockets

ALLOW = (".roblox.com", ".rbxcdn.com")
MAX_NAME = 253


def allowed(domain: str) -> bool:
    d = (domain or "").lower()
    if not d or len(d) > MAX_NAME or "/" in d or ":" in d:
        return False
    return any(d == s[1:] or d.endswith(s) for s in ALLOW)


async def handle(ws):
    try:
        msg = await ws.recv()
    except Exception:
        return
    if not isinstance(msg, (bytes, bytearray)):
        await ws.close()
        return
    b = bytes(msg)
    if len(b) < 3:
        await ws.close()
        return
    n = (b[0] << 8) | b[1]
    if n < 4 or n > MAX_NAME or len(b) < 2 + n:
        await ws.close()
        return
    domain = b[2:2 + n].decode("ascii", "ignore").strip().lower()
    if not allowed(domain):
        await ws.close()
        return
    try:
        reader, writer = await asyncio.open_connection(domain, 443)
    except Exception:
        await ws.close()
        return

    async def ws_to_tcp():
        try:
            async for data in ws:
                if isinstance(data, (bytes, bytearray)):
                    writer.write(bytes(data))
                    await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def tcp_to_ws():
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                await ws.send(data)
        except Exception:
            pass
        finally:
            try:
                await ws.close()
            except Exception:
                pass

    await asyncio.gather(ws_to_tcp(), tcp_to_ws())


async def health_check(path, headers):
    # answer render's plain-GET health pings, never eat websocket upgrades
    if path == "/" and headers.get("Upgrade", "").lower() != "websocket":
        return (200, [("Content-Type", "text/plain")], b"relay alive\n")
    return None


async def main():
    port = int(os.environ.get("PORT", "10000"))
    async with websockets.serve(
        handle, "0.0.0.0", port,
        max_size=None, ping_interval=20, ping_timeout=120,
        process_request=health_check,
    ):
        print(f"[relay] listening 0.0.0.0:{port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
