"""The stalled client must leave every byte after the handshake in the kernel."""

import asyncio
import socket

import pytest

from wslive.rawws import open_stalled

PAYLOAD = b"\x82" + bytes(20_000)  # looks like the start of a frame; content is irrelevant


async def _fake_server(response_head: bytes) -> tuple[asyncio.Server, int]:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.readuntil(b"\r\n\r\n")
        writer.write(response_head + PAYLOAD)
        await writer.drain()
        await asyncio.sleep(5)
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


def test_bytes_after_the_handshake_stay_in_the_kernel() -> None:
    async def main() -> bytes:
        server, port = await _fake_server(
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n"
        )
        async with server:
            stalled = await open_stalled("127.0.0.1", port, "/ws/x")
            await asyncio.sleep(0.3)
            stalled.sock.setblocking(True)
            waiting = stalled.sock.recv(len(PAYLOAD) + 1, socket.MSG_PEEK)
            await stalled.close()
            return waiting

    waiting = asyncio.run(main())
    # Peeking sees everything after the 101 headers: the client consumed none of it.
    assert waiting[:1] == b"\x82"
    assert len(waiting) == len(PAYLOAD)


def test_a_non_101_response_is_refused() -> None:
    async def main() -> None:
        server, port = await _fake_server(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
        async with server:
            await open_stalled("127.0.0.1", port, "/ws/x")

    with pytest.raises(ConnectionError, match="404"):
        asyncio.run(main())
