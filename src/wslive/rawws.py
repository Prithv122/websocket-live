"""A WebSocket client that completes the handshake and then never reads.

This is the "stalled consumer" of the benchmark. The socket is never wrapped in an asyncio
transport: a StreamReader would keep pulling up to 128 KiB into user space before pausing (and on
the Windows proactor loop one 64 KiB receive stays posted even then), which would hide the stall
from the server. Here the handshake reads the response one byte at a time, so nothing past the
101 headers leaves the kernel, and afterwards nothing reads at all. `rcvbuf` shrinks the kernel
receive buffer before connecting, which makes the stall bite sooner.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import os
import socket
from dataclasses import dataclass

MAX_RESPONSE_HEAD = 8192


@dataclass
class StalledSocket:
    sock: socket.socket

    async def close(self) -> None:
        with contextlib.suppress(OSError):
            self.sock.close()


async def _read_head(loop: asyncio.AbstractEventLoop, sock: socket.socket) -> bytes:
    head = bytearray()
    while not head.endswith(b"\r\n\r\n"):
        if len(head) >= MAX_RESPONSE_HEAD:
            raise ConnectionError("handshake response headers too long")
        byte = await loop.sock_recv(sock, 1)
        if not byte:
            raise ConnectionError("connection closed during handshake")
        head += byte
    return bytes(head)


async def open_stalled(
    host: str, port: int, path: str, *, rcvbuf: int | None = None, timeout: float = 5.0
) -> StalledSocket:
    """Open a WebSocket to ws://host:port/path, verify the 101 response, and stop reading."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if rcvbuf is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        sock.setblocking(False)
        loop = asyncio.get_running_loop()
        await asyncio.wait_for(loop.sock_connect(sock, (host, port)), timeout)
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
        await asyncio.wait_for(loop.sock_sendall(sock, request.encode()), timeout)
        head = await asyncio.wait_for(_read_head(loop, sock), timeout)
        status = head.split(b"\r\n", 1)[0]
        if status.split(b" ")[1:2] != [b"101"]:
            raise ConnectionError(f"handshake failed: {status.decode(errors='replace')}")
    except BaseException:
        sock.close()
        raise
    return StalledSocket(sock)
