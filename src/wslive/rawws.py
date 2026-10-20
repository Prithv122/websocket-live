"""A WebSocket client that completes the handshake and then never reads.

This is the "stalled consumer" of the benchmark. A library client would keep draining the TCP
socket into its own queue; this one leaves every byte in the kernel, so the server's sends back
up exactly as they would behind a frozen browser tab. `rcvbuf` shrinks the receive buffer before
connecting, which makes the stall bite sooner.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import os
import socket
from dataclasses import dataclass


@dataclass
class StalledSocket:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    async def close(self) -> None:
        self.writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await self.writer.wait_closed()


async def open_stalled(
    host: str, port: int, path: str, *, rcvbuf: int | None = None, timeout: float = 5.0
) -> StalledSocket:
    """Open a WebSocket to ws://host:port/path, verify the 101 response, and stop reading."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if rcvbuf is not None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    sock.setblocking(False)
    loop = asyncio.get_running_loop()
    try:
        await asyncio.wait_for(loop.sock_connect(sock, (host, port)), timeout)
    except BaseException:
        sock.close()
        raise
    reader, writer = await asyncio.open_connection(sock=sock)
    key = base64.b64encode(os.urandom(16)).decode()
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    writer.write(request.encode())
    await writer.drain()
    head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout)
    status = head.split(b"\r\n", 1)[0]
    if b" 101 " not in status + b" ":
        writer.close()
        raise ConnectionError(f"handshake failed: {status.decode(errors='replace')}")
    return StalledSocket(reader, writer)
