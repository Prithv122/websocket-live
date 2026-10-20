"""Wire models shared by the server, the load generator and the tests (see PROTOCOL.md)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

PROTOCOL_VERSION = 1
ZONES: tuple[str, ...] = ("north", "south", "east", "west", "central", "airport")
STATUSES: tuple[str, ...] = ("placed", "picked", "delivered", "cancelled")
CHANNEL_PATTERN = r"^[a-z0-9_-]{1,32}$"

EnvelopeType = Literal["event", "snapshot", "heartbeat", "gap"]


def iso_ms(epoch: float) -> str:
    """Format Unix seconds as ISO-8601 UTC with milliseconds, e.g. 2026-10-20T12:34:56.789Z."""
    return datetime.fromtimestamp(epoch, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_iso_ms(text: str) -> float:
    """Inverse of iso_ms: ISO-8601 UTC string to Unix seconds."""
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


class Envelope(BaseModel):
    """One server-to-client message."""

    model_config = ConfigDict(frozen=True)

    v: Literal[1] = PROTOCOL_VERSION
    channel: str = Field(pattern=CHANNEL_PATTERN)
    seq: int = Field(ge=0)
    ts: str
    type: EnvelopeType
    data: dict[str, Any] = Field(default_factory=dict)

    def to_json(self) -> str:
        return self.model_dump_json()


class OrderEvent(BaseModel):
    """Payload of an `event` on the orders channel. Extra fields (t_sched, pad) are allowed."""

    model_config = ConfigDict(extra="allow")

    order_id: str
    zone: Literal["north", "south", "east", "west", "central", "airport"]
    amount: float = Field(gt=0)
    status: Literal["placed", "picked", "delivered", "cancelled"]


class PublishRequest(BaseModel):
    """Body of POST /publish/{channel}."""

    type: str = Field(default="event", min_length=1, max_length=32)
    data: dict[str, Any] = Field(default_factory=dict)
