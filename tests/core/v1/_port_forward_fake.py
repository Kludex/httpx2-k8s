from __future__ import annotations

import struct


def spdy_control(frame_type: int, payload: bytes, *, flags: int = 0, version: int = 3) -> bytes:
    return struct.pack(">HHI", 0x8000 | version, frame_type, flags << 24 | len(payload)) + payload


def spdy_reply(stream_id: int) -> bytes:
    return spdy_control(2, struct.pack(">I", stream_id))


def spdy_data(stream_id: int, payload: bytes = b"", *, fin: bool = False) -> bytes:
    flags = 1 if fin else 0
    return struct.pack(">II", stream_id, flags << 24 | len(payload)) + payload


def spdy_stream_id(frame: bytes) -> int:
    first_word = struct.unpack(">I", frame[:4])[0]
    if first_word & 0x80000000:
        return struct.unpack(">I", frame[8:12])[0]
    return first_word


def spdy_frame_type(frame: bytes) -> int | None:
    first_word = struct.unpack(">I", frame[:4])[0]
    return first_word & 0xFFFF if first_word & 0x80000000 else None


def spdy_flags(frame: bytes) -> int:
    return struct.unpack(">I", frame[4:8])[0] >> 24


def spdy_payload(frame: bytes) -> bytes:
    length = struct.unpack(">I", frame[4:8])[0] & 0xFFFFFF
    return frame[8 : 8 + length]
