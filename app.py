"""Eagle Lenz stream test.

Streams two Pocket Option assets (default EURHUF_otc and EURUSD_otc) on 15 second
candles through ONE SSID and pushes every tick to browsers over a WebSocket.

Why two assets: the broker streams about two symbols per session (the chart symbol
plus one more). A third stream starves and ends in
"Subscription stream timed out after 60s". Keep this at two per SSID.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import random
import time
from contextlib import aclosing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from aiohttp import WSMsgType, web

from BinaryOptionsToolsV2.pocketoption.asynchronous import PocketOptionAsync

from feed import CandleFeed

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("eagle-lenz")


# --------------------------------------------------------------------------- config
def _float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def _backoff() -> list[float]:
    raw = os.environ.get("BACKOFF_SECONDS", "5,10,20,40,60,120")
    try:
        values = [float(x) for x in raw.split(",") if x.strip()]
    except ValueError:
        values = []
    return values or [5.0, 10.0, 20.0, 40.0, 60.0, 120.0]


SSID = os.environ.get("POCKET_OPTION_SSID", "").strip()
ACCESS_KEY = os.environ.get("ACCESS_KEY", "").strip()
PORT = _int("PORT", 8080)

PERIOD = _int("PERIOD_SECONDS", 15)
CLOSED_CANDLES = _int("CLOSED_CANDLES", 99)
MAX_STREAMS = 2  # broker ceiling per session, see module docstring

ASSETS = [a.strip() for a in os.environ.get("ASSETS", "EURHUF_otc,EURUSD_otc").split(",") if a.strip()]
if len(ASSETS) > MAX_STREAMS:
    log.warning("Only %d live streams work per SSID; using %s", MAX_STREAMS, ASSETS[:MAX_STREAMS])
    ASSETS = ASSETS[:MAX_STREAMS]

STALE_AFTER = _float("STALE_AFTER_SECONDS", 5.0)  # badge turns to "Stale"
STALL_SECONDS = _float("STALL_SECONDS", 30.0)  # no tick this long -> reconnect the SSID
FIRST_UPDATE_TIMEOUT = _float("FIRST_UPDATE_TIMEOUT", 90.0)  # subscribe + first tick
CONNECT_TIMEOUT = _float("CONNECT_TIMEOUT", 60.0)
CONFIRM_SECONDS = _float("CONFIRM_SECONDS", 15.0)
VERIFY_BUCKETS = _int("VERIFY_BUCKETS", 3)  # complete live candles to check history against
CANDLE_SOURCE = os.environ.get("CANDLE_SOURCE", "aligned").strip().lower()  # aligned | ticks
USE_HISTORY = os.environ.get("USE_HISTORY", "1").strip().lower() not in ("0", "false", "no", "off")
AUTH_RETRY_SECONDS = _float("AUTH_RETRY_SECONDS", 300.0)
SEND_TIMEOUT = 2.0
BACKOFF = _backoff()

STATIC_DIR = Path(__file__).parent / "static"


def display_name(symbol: str) -> str:
    """EURHUF_otc -> EUR/HUF OTC."""
    otc = symbol.lower().endswith("_otc")
    base = symbol[:-4] if otc else symbol
    if len(base) == 6 and base.isalpha():
        base = f"{base[:3]}/{base[3:]}"
    return f"{base} OTC" if otc else base


def scrub(text: str) -> str:
    """Keep the SSID out of anything that reaches logs or the browser."""
    if SSID and len(SSID) > 8:
        text = text.replace(SSID, "***")
    return text[:160]


# --------------------------------------------------------------------------- state
@dataclass
class AssetState:
    symbol: str
    name: str
    closed: list = field(default_factory=list)
    forming: Optional[dict] = None
    status: str = "CONNECTING"
    detail: str = ""
    ticks: int = 0
    last_tick_wall: float = 0.0  # unix seconds when the last tick reached the server
    history_source: str = "none"
    cross: str = ""

    @property
    def price(self) -> Optional[float]:
        if self.forming:
            return self.forming["close"]
        if self.closed:
            return self.closed[-1]["close"]
        return None

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "price": self.price,
            "ticks": self.ticks,
            "last_tick_ms": int(self.last_tick_wall * 1000) if self.last_tick_wall else None,
            "closed": self.closed,
            "forming": self.forming,
        }


class Hub:
    """Holds the latest state per asset and fans messages out to connected browsers."""

    def __init__(self) -> None:
        self.assets: dict[str, AssetState] = {s: AssetState(s, display_name(s)) for s in ASSETS}
        self.clients: set[web.WebSocketResponse] = set()
        self.service: dict[str, Any] = {"status": "STARTING", "detail": ""}

    # -- messages
    def snapshot_message(self, symbols: Optional[list[str]] = None) -> dict:
        chosen = [self.assets[s] for s in (symbols or self.assets.keys())]
        return {
            "type": "snapshot",
            "server_ms": int(time.time() * 1000),
            "period": PERIOD,
            "max_rows": CLOSED_CANDLES,
            "service": self.service,
            "assets": [a.to_dict() for a in chosen],
        }

    async def broadcast(self, message: dict) -> None:
        if not self.clients:
            return
        text = json.dumps(message, separators=(",", ":"))

        async def send(ws: web.WebSocketResponse) -> None:
            try:
                await asyncio.wait_for(ws.send_str(text), SEND_TIMEOUT)
            except Exception:  # slow or dead browser: drop it, it will reconnect
                self.clients.discard(ws)
                try:
                    await ws.close()
                except Exception:
                    pass

        await asyncio.gather(*(send(ws) for ws in list(self.clients)))

    # -- status
    async def set_service(self, status: str, detail: str = "") -> None:
        if self.service["status"] == status and self.service["detail"] == detail:
            return
        self.service = {"status": status, "detail": detail}
        await self.broadcast({"type": "service", **self.service})

    async def set_status(self, symbol: str, status: str, detail: str = "") -> None:
        state = self.assets[symbol]
        if state.status == status and state.detail == detail:
            return
        state.status, state.detail = status, detail
        await self.broadcast({"type": "status", "symbol": symbol, "status": status, "detail": detail})

    async def set_all_status(self, status: str, detail: str = "") -> None:
        for symbol in self.assets:
            await self.set_status(symbol, status, detail)

    # -- data
    async def apply_update(self, state: AssetState, closed: list, forming: Optional[dict], event: str, info: dict) -> None:
        previous_last = state.closed[-1]["time"] if state.closed else None
        state.closed = closed
        state.forming = forming
        state.last_tick_wall = time.time()
        state.history_source = info.get("source", "none")
        state.cross = info.get("cross", "")

        if event in ("seed", "history"):
            # First data, or verified history was just merged: resync the whole chart.
            if event == "seed":
                state.ticks = 0
            state.status, state.detail = "LIVE", info.get("detail", "")
            await self.broadcast(self.snapshot_message([state.symbol]))
            return

        if event == "note":
            await self.set_status(state.symbol, "LIVE", info.get("detail", ""))
            return

        state.ticks += 1
        if state.status != "LIVE":
            await self.set_status(state.symbol, "LIVE", state.detail)

        # A candle just closed: tell the browser its final values.
        if closed and (previous_last is None or closed[-1]["time"] > previous_last):
            for candle in closed:
                if previous_last is None or candle["time"] > previous_last:
                    await self.broadcast({"type": "candle", "symbol": state.symbol, "candle": candle})

        await self.broadcast(
            {
                "type": "tick",
                "symbol": state.symbol,
                "price": state.price,
                "forming": forming,
                "n": state.ticks,
                "ts": int(state.last_tick_wall * 1000),
            }
        )


# --------------------------------------------------------------------------- streaming
class StreamStalled(Exception):
    pass


async def stream_asset(client: PocketOptionAsync, hub: Hub, symbol: str, lock: asyncio.Lock) -> None:
    """Stream one asset: live candles from ticks, plus history that passed verification."""
    state = hub.assets[symbol]
    await hub.set_status(symbol, "CONNECTING", "subscribing")

    feed = CandleFeed(client, symbol, PERIOD, CLOSED_CANDLES, lock, verify_buckets=VERIFY_BUCKETS,
                      source=CANDLE_SOURCE, use_history=USE_HISTORY)
    generator = feed.stream()
    first = True
    async with aclosing(generator):
        updates = generator.__aiter__()
        while True:
            timeout = FIRST_UPDATE_TIMEOUT if first else STALL_SECONDS
            try:
                closed, forming, event = await asyncio.wait_for(updates.__anext__(), timeout)
            except asyncio.TimeoutError:
                raise StreamStalled(f"{symbol}: no data for {timeout:.0f}s") from None
            except StopAsyncIteration:
                raise StreamStalled(f"{symbol}: stream ended") from None
            await hub.apply_update(state, closed, forming, event, feed.info)
            first = False


async def wait_confirmed(client: PocketOptionAsync, seconds: float) -> bool:
    """True once the broker has accepted the session."""
    deadline = time.monotonic() + seconds
    while True:
        try:
            if client.is_ssid_valid():
                return True
        except Exception:
            return True  # cannot tell: do not treat as an auth failure
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(0.5)


async def shutdown_client(client: Optional[PocketOptionAsync]) -> None:
    if client is None:
        return
    try:
        await asyncio.wait_for(client.shutdown(), 10)
    except Exception:
        pass


async def supervisor(hub: Hub) -> None:
    """Own the broker connection. Any failure reconnects the whole SSID with backoff.

    The server ignores unsubscribe requests, so resubscribing one asset does not help;
    a fresh connection does.
    """
    if not SSID:
        await hub.set_service("SSID_MISSING", "Set the POCKET_OPTION_SSID variable in Railway, then redeploy.")
        await hub.set_all_status("OFFLINE", "no SSID configured")
        log.error("POCKET_OPTION_SSID is not set")
        return

    attempt = 0
    while True:
        client: Optional[PocketOptionAsync] = None
        started = time.monotonic()
        confirmed = False
        reason = ""
        try:
            await hub.set_service("CONNECTING", "")
            await hub.set_all_status("CONNECTING", "connecting to broker")
            client = PocketOptionAsync(SSID)
            await asyncio.wait_for(client.wait_for_assets(), CONNECT_TIMEOUT)

            confirmed = await wait_confirmed(client, CONFIRM_SECONDS)
            if not confirmed:
                log.warning("Broker has not confirmed the session yet; streaming anyway")
            await hub.set_service("RUNNING", "" if confirmed else "session not confirmed by broker yet")

            lock = asyncio.Lock()  # one subscribe/history call at a time on this connection
            tasks = [asyncio.create_task(stream_asset(client, hub, symbol, lock)) for symbol in hub.assets]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                error = task.exception()
                if error:
                    raise error
            raise RuntimeError("all streams ended")
        except asyncio.CancelledError:
            await shutdown_client(client)
            raise
        except Exception as error:  # noqa: BLE001 - every failure leads to a reconnect
            reason = scrub(f"{type(error).__name__}: {error}")
            log.warning("Stream failure: %s", reason)

        await shutdown_client(client)

        if not confirmed:
            # The broker never accepted this session: retrying quickly only risks a flag.
            delay = AUTH_RETRY_SECONDS
            await hub.set_service(
                "AUTH_FAILED",
                "The broker did not confirm this SSID. Replace POCKET_OPTION_SSID and redeploy. "
                f"Next try in {delay:.0f}s.",
            )
        else:
            if time.monotonic() - started > 300:
                attempt = 0  # it ran fine for a while, so start the backoff over
            delay = BACKOFF[min(attempt, len(BACKOFF) - 1)] * random.uniform(1.0, 1.25)
            attempt += 1
            await hub.set_service("RECONNECTING", f"retry in {delay:.0f}s")
        await hub.set_all_status("RECONNECTING", f"{reason} (retry in {delay:.0f}s)")
        await asyncio.sleep(delay)


async def monitor(hub: Hub) -> None:
    """Mark an asset Stale as soon as ticks pause, well before the reconnect threshold."""
    while True:
        await asyncio.sleep(1.0)
        now = time.time()
        for state in hub.assets.values():
            if state.status == "LIVE" and state.last_tick_wall and now - state.last_tick_wall > STALE_AFTER:
                await hub.set_status(state.symbol, "STALE", f"no tick for {now - state.last_tick_wall:.0f}s")


# --------------------------------------------------------------------------- web
def authorized(request: web.Request) -> bool:
    if not ACCESS_KEY:
        return True
    supplied = request.query.get("key", "")
    return hmac.compare_digest(supplied.encode(), ACCESS_KEY.encode())


async def index(request: web.Request) -> web.StreamResponse:
    if not authorized(request):
        return web.Response(status=401, text="Unauthorized. Open the page with ?key=YOUR_ACCESS_KEY")
    return web.FileResponse(
        STATIC_DIR / "index.html",
        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
    )


async def health(request: web.Request) -> web.Response:
    hub: Hub = request.app["hub"]
    now = time.time()
    return web.json_response(
        {
            "ok": True,
            "service": hub.service["status"],
            "assets": {
                symbol: {
                    "status": state.status,
                    "history": state.history_source,
                    "cross_check": state.cross,
                    "tick_age_s": round(now - state.last_tick_wall, 1) if state.last_tick_wall else None,
                }
                for symbol, state in hub.assets.items()
            },
        }
    )


async def ws_handler(request: web.Request) -> web.StreamResponse:
    if not authorized(request):
        return web.Response(status=401, text="Unauthorized")
    hub: Hub = request.app["hub"]
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=4096)
    await ws.prepare(request)
    try:
        await ws.send_str(json.dumps(hub.snapshot_message(), separators=(",", ":")))
        hub.clients.add(ws)
        async for message in ws:  # browsers send nothing we need; this keeps the socket open
            if message.type == WSMsgType.ERROR:
                break
    finally:
        hub.clients.discard(ws)
    return ws


async def background(app: web.Application):
    hub: Hub = app["hub"]
    tasks = [asyncio.create_task(supervisor(hub)), asyncio.create_task(monitor(hub))]
    log.info("Streaming %s on %ss candles (%d closed + live), candle source=%s, history=%s",
             list(hub.assets), PERIOD, CLOSED_CANDLES, CANDLE_SOURCE, USE_HISTORY)
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def create_app() -> web.Application:
    app = web.Application()
    app["hub"] = Hub()
    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/health", health)
    app.cleanup_ctx.append(background)
    return app


if __name__ == "__main__":
    if not ACCESS_KEY:
        log.warning("ACCESS_KEY is not set: anyone with the URL can watch the stream")
    web.run_app(create_app(), host="0.0.0.0", port=PORT, access_log=None)
