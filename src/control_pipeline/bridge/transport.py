"""transport.py — Transport abstractions for the McuBridgeWorker.

Provides:
- ``ByteTransport`` – structural Protocol defining the async byte-level
  transport interface.
- ``MockMcuTransport`` – Profile C in-process queue-pair transport for
  standalone dev/test (paired with :class:`MockMcu`).
- ``PipeTransport`` – Profile B COBS-framed transport over a
  ``StreamReader``/``StreamWriter`` pair (pty, socketpair, or similar).

Nothing is imported from E:\\edge\\src.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ByteTransport(Protocol):
    """Structural interface for a bidirectional raw-frame transport.

    Each :meth:`recv` call returns exactly one raw frame's worth of bytes.
    Framing is transport-specific: ``MockMcuTransport`` uses queue entries
    (already framed), while ``PipeTransport`` uses COBS with a ``0x00``
    delimiter.
    """

    async def send(self, data: bytes) -> None:
        """Transmit *data* (one complete frame) to the remote side."""
        ...

    async def recv(self) -> bytes:
        """Block until one complete frame is available and return its bytes."""
        ...

    async def open(self) -> None:
        """Initialise the transport (open file descriptors, bind queues, …)."""
        ...

    async def close(self) -> None:
        """Tear down the transport gracefully."""
        ...


# ---------------------------------------------------------------------------
# Profile C – in-process asyncio.Queue pair
# ---------------------------------------------------------------------------


class MockMcuTransport:
    """Profile C transport for testing/demo.

    Uses two :class:`asyncio.Queue` instances to simulate a bidirectional
    UART link between the MPU (McuBridgeWorker) and the MCU (MockMcu).

    Queues
    ------
    ``mpu_to_mcu``
        MPU-side writes here (via :meth:`send`); MCU-side reads from here.
    ``mcu_to_mpu``
        MCU-side writes here; MPU-side reads from here (via :meth:`recv`).

    Usage
    -----
    ::

        mpu_transport, mcu_transport = MockMcuTransport.linked_pair()
        worker = McuBridgeWorker(mpu_transport, ...)
        mock   = MockMcu(mcu_transport)
    """

    def __init__(
        self,
        mpu_to_mcu: asyncio.Queue[bytes],
        mcu_to_mpu: asyncio.Queue[bytes],
    ) -> None:
        self.mpu_to_mcu: asyncio.Queue[bytes] = mpu_to_mcu
        self.mcu_to_mpu: asyncio.Queue[bytes] = mcu_to_mpu
        self._closed: bool = False

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @staticmethod
    def linked_pair() -> tuple[MockMcuTransport, MockMcuTransport]:
        """Return *(mpu_side, mcu_side)* – a matched transport pair.

        The *mpu_side* instance is passed to :class:`McuBridgeWorker`; the
        *mcu_side* instance is passed to :class:`MockMcu`.
        """
        a_to_b: asyncio.Queue[bytes] = asyncio.Queue()
        b_to_a: asyncio.Queue[bytes] = asyncio.Queue()
        mpu_side = MockMcuTransport(mpu_to_mcu=a_to_b, mcu_to_mpu=b_to_a)
        mcu_side = MockMcuTransport(mpu_to_mcu=b_to_a, mcu_to_mpu=a_to_b)
        return mpu_side, mcu_side

    # ------------------------------------------------------------------
    # ByteTransport implementation
    # ------------------------------------------------------------------

    async def open(self) -> None:  # noqa: D401
        """No-op – queues are already initialised."""
        self._closed = False

    async def close(self) -> None:
        """Mark the transport closed; pending :meth:`recv` calls will raise."""
        self._closed = True

    async def send(self, data: bytes) -> None:
        """Enqueue *data* for the remote side to consume."""
        if self._closed:
            raise RuntimeError("MockMcuTransport is closed")
        await self.mpu_to_mcu.put(data)

    async def recv(self) -> bytes:
        """Block until the remote side enqueues a frame and return it."""
        if self._closed:
            raise RuntimeError("MockMcuTransport is closed")
        # We loop so that a close() that races with a pending get wakes us.
        while True:
            try:
                return await asyncio.wait_for(self.mcu_to_mpu.get(), timeout=0.1)
            except asyncio.TimeoutError:
                if self._closed:
                    raise RuntimeError("MockMcuTransport closed while waiting")


# ---------------------------------------------------------------------------
# COBS helpers
# ---------------------------------------------------------------------------


def cobs_encode(data: bytes) -> bytes:
    """Encode *data* with Consistent Overhead Byte Stuffing (COBS).

    Returns the encoded bytes **without** the trailing ``0x00`` delimiter so
    that callers can append it themselves (facilitating streaming).

    Algorithm
    ---------
    Scan *data* for zero bytes; replace each run of non-zero bytes plus the
    following zero (or end-of-input) with a length byte followed by the
    non-zero payload bytes.  The length byte encodes the distance to the
    *next* zero-byte (or end of output), capped at 254 so that ``0x00``
    never appears in the output.
    """
    output = bytearray()
    code_index = 0          # index of the pending "distance" byte in output
    output.append(0x00)     # placeholder for the first code byte
    code = 1                # distance to next zero / end-of-block

    for byte in data:
        if byte == 0x00:
            output[code_index] = code
            code_index = len(output)
            output.append(0x00)  # placeholder
            code = 1
        else:
            output.append(byte)
            code += 1
            if code == 0xFF:
                # Maximum block length reached – emit block without consuming zero
                output[code_index] = code
                code_index = len(output)
                output.append(0x00)  # placeholder
                code = 1

    output[code_index] = code
    return bytes(output)


def cobs_decode(data: bytes) -> bytes | None:
    """Decode a COBS-encoded frame (without the trailing ``0x00`` delimiter).

    Returns the original data, or ``None`` if *data* is malformed (e.g. a
    code byte references beyond the end of the buffer).
    """
    output = bytearray()
    index = 0

    while index < len(data):
        code = data[index]
        if code == 0:
            # Zero must not appear inside a valid COBS frame
            return None
        index += 1
        end = index + code - 1
        if end > len(data):
            return None
        for i in range(index, end):
            output.append(data[i])
        index = end
        if code < 0xFF and index < len(data):
            output.append(0x00)

    return bytes(output)


# ---------------------------------------------------------------------------
# Profile B – StreamReader/StreamWriter with COBS framing
# ---------------------------------------------------------------------------


class PipeTransport:
    """Profile B transport using a COBS-framed ``asyncio`` stream pair.

    Suitable for a pty pair, a ``socketpair()``, or any OS-level
    bidirectional byte channel.  Uses ``0x00`` as the COBS frame delimiter.

    Parameters
    ----------
    reader:
        An :class:`asyncio.StreamReader` connected to the remote end.
    writer:
        An :class:`asyncio.StreamWriter` connected to the remote end.
    max_frame_bytes:
        Maximum decoded frame size (bytes).  Frames that decode to more than
        this are discarded to prevent memory exhaustion.  Default 4096.
    """

    DELIMITER = b"\x00"

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        max_frame_bytes: int = 4096,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._max_frame_bytes = max_frame_bytes
        self._closed = False

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def from_socketpair() -> tuple[PipeTransport, PipeTransport]:
        """Create a matched pair backed by an in-process ``socketpair``.

        Returns *(side_a, side_b)*.  Useful for integration tests.
        """
        import socket

        sock_a, sock_b = socket.socketpair()
        loop = asyncio.get_event_loop()

        ra, wa = await asyncio.open_connection(sock=sock_a)
        rb, wb = await asyncio.open_connection(sock=sock_b)
        return PipeTransport(ra, wa), PipeTransport(rb, wb)

    # ------------------------------------------------------------------
    # ByteTransport implementation
    # ------------------------------------------------------------------

    async def open(self) -> None:
        """No-op – streams are provided at construction time."""
        self._closed = False

    async def close(self) -> None:
        """Close the writer; the reader will naturally drain."""
        self._closed = True
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass

    async def send(self, data: bytes) -> None:
        """COBS-encode *data* and transmit it followed by the ``0x00`` delimiter."""
        if self._closed:
            raise RuntimeError("PipeTransport is closed")
        encoded = cobs_encode(data) + self.DELIMITER
        self._writer.write(encoded)
        await self._writer.drain()

    async def recv(self) -> bytes:
        """Read bytes until a ``0x00`` delimiter and COBS-decode the payload.

        Raises
        ------
        RuntimeError
            If the transport is closed or the connection is lost.
        ValueError
            If the received frame is malformed COBS.
        """
        if self._closed:
            raise RuntimeError("PipeTransport is closed")
        while True:
            encoded = await self._reader.readuntil(self.DELIMITER)
            # Strip the trailing 0x00 delimiter
            encoded = encoded[:-1]
            if not encoded:
                # Empty delimited segment – skip (inter-frame gap)
                continue
            decoded = cobs_decode(encoded)
            if decoded is None:
                raise ValueError(f"Malformed COBS frame ({len(encoded)} bytes)")
            if len(decoded) > self._max_frame_bytes:
                raise ValueError(
                    f"Decoded frame too large: {len(decoded)} > {self._max_frame_bytes}"
                )
            return decoded
