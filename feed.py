"""Candle feed that only trusts history it can verify against live ticks.

Why this exists
---------------
In BinaryOptionsToolsV2 0.2.18, get_candles_live(), candles(), get_candles() and
candles_many() all backfill the same way: they compile candles from raw tick pages
(loadHistoryPeriod). get_candles_live only asks for the broker's own chart history
(changeSymbol -> updateHistoryNewFast, what the broker chart draws) when the tick
pages returned too FEW candles. If the tick pages return enough candles that are stale
or misplaced, they are merged in unchecked and the chart history is never requested.

This module:
  1. builds candles from live ticks (these have proven correct),
  2. fetches history from the chart feed first, then the tick pages,
  3. accepts a history source only if it agrees with the live candles, trying a
     clock-shift correction (multiples of 15 minutes) when it is offset,
  4. otherwise keeps building from live ticks only and says so.

Closed-candle source (the developer's suggestion):
  source="aligned" also opens subscribe_ticks(asset, interval=period, aligned=True),
  the library's own clock-aligned candle builder, and uses its finished candles as the
  authority. It shares the asset's single broker stream. Our tick-built candles are kept
  as a cross-check, and the agreement is reported. Both skip the first window, because
  joining mid-window gives a partial candle.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Optional

log = logging.getLogger("eagle-lenz.feed")

SHIFT_STEP = 900  # clock errors are whole timezone steps; try multiples of 15 minutes
MAX_SHIFT = 4 * 3600
MIN_HISTORY = 5
FIRST_TICK_TIMEOUT = 45.0
CHART_TIMEOUT = 25.0
TICKS_TIMEOUT = 20.0
HISTORY_ATTEMPTS = 3
HISTORY_RETRY_SECONDS = 45.0


# --------------------------------------------------------------------------- helpers
def candle_time(raw: dict) -> int:
    """Start time in Unix seconds. The engine uses 'timestamp', wrappers use 'time';
    some paths send milliseconds."""
    value = int(float(raw.get("timestamp", raw.get("time", 0))))
    return value // 1000 if value > 10_000_000_000 else value


def normalize(payload: Any) -> list[dict]:
    """Turn a JSON string or list of engine candles into sorted, de-duplicated
    {'time','open','high','low','close'} dicts. Malformed candles are dropped."""
    if isinstance(payload, (str, bytes)):
        try:
            payload = json.loads(payload)
        except ValueError:
            return []
    if not isinstance(payload, list):
        return []
    by_time: dict[int, dict] = {}
    for raw in payload:
        try:
            candle = {
                "time": candle_time(raw),
                "open": float(raw["open"]),
                "high": float(raw["high"]),
                "low": float(raw["low"]),
                "close": float(raw["close"]),
            }
        except (KeyError, TypeError, ValueError, AttributeError):
            continue
        values = (candle["open"], candle["high"], candle["low"], candle["close"])
        if candle["time"] <= 0 or not all(math.isfinite(v) and v > 0 for v in values):
            continue
        by_time[candle["time"]] = candle
    return [by_time[t] for t in sorted(by_time)]


def median_range(candles: list[dict]) -> float:
    ranges = [c["high"] - c["low"] for c in candles]
    return statistics.median(ranges) if ranges else 0.0


# --------------------------------------------------------------------------- assessment
@dataclass
class Assessment:
    ok: bool
    reason: str = ""
    mode: str = ""  # "overlap" (checked against live candles) or "anchor" (price/age only)
    shift: int = 0  # seconds added to the source's timestamps
    compared: int = 0
    matched: int = 0
    age: Optional[float] = None  # seconds between newest source candle and the live clock
    candles: list = field(default_factory=list)  # source candles after the shift
    rows: list = field(default_factory=list)  # overlap table, for the probe
    trimmed: int = 0  # older candles dropped because the chain of candles broke
    break_note: str = ""  # where and why it broke
    spacing: dict = field(default_factory=dict)  # spacing/alignment stats, for the probe

    def to_dict(self) -> dict:
        data = {k: v for k, v in self.__dict__.items() if k not in ("candles", "rows")}
        data["candles"] = len(self.candles)
        return data


def _overlap(live: list[dict], by_time: dict[int, dict], shift: int, tol: float):
    rows, matched = [], 0
    for lc in live:
        hc = by_time.get(lc["time"] - shift)
        if hc is None:
            continue
        diff = max(abs(hc[k] - lc[k]) for k in ("open", "high", "low", "close"))
        ok = diff <= tol
        matched += ok
        rows.append(
            {
                "time": lc["time"],
                "live": [lc["open"], lc["high"], lc["low"], lc["close"]],
                "source": [hc["open"], hc["high"], hc["low"], hc["close"]],
                "max_diff": diff,
                "match": ok,
            }
        )
    return rows, matched


def _shifts() -> list[int]:
    steps = MAX_SHIFT // SHIFT_STEP
    others = [k * SHIFT_STEP for k in range(-steps, steps + 1) if k]
    return [0] + sorted(others, key=lambda s: (abs(s), s))


def spacing_stats(candles: list[dict], period: int, last: int = 60) -> dict:
    """How the candle timestamps are spaced. Reveals wrong-granularity history
    (for example minute candles returned for a 15s request)."""
    tail = candles[-last:]
    diffs = [b["time"] - a["time"] for a, b in zip(tail, tail[1:])]
    exact = sum(1 for d in diffs if d == period)
    aligned = sum(1 for c in tail if c["time"] % period == 0)
    histogram: dict[int, int] = {}
    for d in diffs:
        histogram[d] = histogram.get(d, 0) + 1
    common = sorted(histogram.items(), key=lambda kv: -kv[1])[:4]
    return {
        "exact_fraction": exact / len(diffs) if diffs else 0.0,
        "aligned_fraction": aligned / len(tail) if tail else 0.0,
        "common_spacings": common,
    }


def contiguous_tail(candles: list[dict], period: int, jump_tol: float) -> tuple[list[dict], str]:
    """Walk back from the newest candle and keep the unbroken chain only.

    A break is a time gap of more than two periods or a price jump between one candle's
    close and the next candle's open. Everything older than the first break is untrusted:
    that is where stale blocks get stitched on.
    """
    if not candles:
        return [], ""
    start = len(candles) - 1
    note = ""
    for i in range(len(candles) - 1, 0, -1):
        newer, older = candles[i], candles[i - 1]
        gap = newer["time"] - older["time"]
        jump = abs(newer["open"] - older["close"])
        if gap > 2 * period:
            note = f"time gap of {gap}s before {newer['time']}"
            break
        if jump > jump_tol:
            note = f"price jump of {jump:.6g} before {newer['time']}"
            break
        start = i - 1
    return candles[start:], note


def assess_history(
    history: list[dict],
    live_full: list[dict],
    period: int,
    ref_time: int,
    ref_price: float,
) -> Assessment:
    """Decide whether `history` describes the same market the live ticks show.

    live_full: live-built candles that were fully observed (not the one we joined mid-way).
    ref_time / ref_price: newest live tick, in the same clock as the live candles.
    """
    if len(history) < MIN_HISTORY:
        return Assessment(False, f"only {len(history)} candles returned")

    by_time = {c["time"]: c for c in history}
    newest = history[-1]["time"]
    hist_range = median_range(history[-30:]) or 1e-6 * ref_price
    chosen: Optional[Assessment] = None

    if live_full:
        tol = max(3 * median_range(live_full), 5e-5 * ref_price)
        for shift in _shifts():
            rows, matched = _overlap(live_full, by_time, shift, tol)
            covered = len(rows) / len(live_full)  # history must contain the live window, not a sample of it
            if rows and covered >= 0.8 and matched >= min(2, len(rows)) and matched / len(rows) >= 0.6:
                chosen = Assessment(True, mode="overlap", shift=shift, compared=len(rows), matched=matched, rows=rows)
                break
        if chosen is None:
            rows, matched = _overlap(live_full, by_time, 0, tol)
            if rows and len(rows) < 0.8 * len(live_full):
                reason = f"history holds only {len(rows)} of {len(live_full)} live candle times (wrong granularity?)"
            elif rows:
                reason = f"{matched}/{len(rows)} overlapping candles match the live ticks"
            else:
                reason = f"history ends {ref_time - newest:.0f}s before the live data and does not contain it"
            return Assessment(False, reason, mode="overlap", compared=len(rows), matched=matched,
                              age=ref_time - newest, rows=rows)
    else:
        tol = max(12 * hist_range, 3e-4 * ref_price)
        for shift in _shifts():
            age = ref_time - (newest + shift)
            last_close = history[-1]["close"]
            if -period <= age <= 8 * period and abs(last_close - ref_price) <= tol:
                chosen = Assessment(True, mode="anchor", shift=shift, age=age)
                break
        if chosen is None:
            age = ref_time - newest
            return Assessment(
                False,
                f"newest candle is {age:.0f}s old and {abs(history[-1]['close'] - ref_price):.6g} away from the live price",
                mode="anchor", age=age,
            )

    corrected = [{**c, "time": c["time"] + chosen.shift} for c in history]
    chosen.age = ref_time - corrected[-1]["time"]

    stats = spacing_stats(corrected, period)
    chosen.spacing = stats
    if stats["exact_fraction"] < 0.85 or stats["aligned_fraction"] < 0.9:
        chosen.ok = False
        chosen.reason = (f"candles are not {period}s apart (common spacings {stats['common_spacings']}), "
                         f"{stats['aligned_fraction']:.0%} aligned to the period")
        return chosen

    # Only trust the unbroken chain that leads up to the verified live edge.
    jump_tol = max(10 * hist_range, 5e-4 * ref_price)
    tail, note = contiguous_tail(corrected, period, jump_tol)
    chosen.trimmed = len(corrected) - len(tail)
    chosen.break_note = note
    if len(tail) < MIN_HISTORY:
        chosen.ok = False
        chosen.reason = f"only {len(tail)} unbroken candles reach the live data ({note or 'too few'})"
        return chosen
    chosen.candles = tail
    return chosen


# --------------------------------------------------------------------------- feed
class CandleFeed:
    """Streams (closed_candles, forming_candle, event) for one asset.

    event is "seed" (first tick), "tick", "history" (verified history was merged,
    resend the whole chart) or "note" (only self.info['detail'] changed).
    """

    def __init__(self, client, symbol: str, period: int, max_rows: int, lock: asyncio.Lock,
                 verify_buckets: int = 2, source: str = "aligned", use_history: bool = True) -> None:
        self.client = client
        self.symbol = symbol
        self.period = period
        self.max_rows = max_rows
        self.lock = lock  # serialises subscribe and history calls across assets
        self.verify_buckets = verify_buckets
        self.source = source if source in ("aligned", "ticks") else "aligned"
        self.use_history = use_history
        self.closed: deque = deque(maxlen=max_rows)  # tick-built (and merged history) candles
        self.forming: Optional[dict] = None
        self.live_full: list[dict] = []  # tick-built, fully observed candles
        self.aligned: dict[int, dict] = {}  # library-built, fully observed candles
        self.first_bucket: Optional[int] = None
        self.last_tick: Optional[tuple[int, float]] = None
        self.history_applied = False
        self.cross = {"compared": 0, "matched": 0}
        self._checked: set[int] = set()
        self.info: dict = {"source": "none", "detail": "Waiting for the first tick", "history": 0,
                           "shift": 0, "cross": ""}

    # -- live candles
    def _ingest(self, ts: int, price: float) -> None:
        self.last_tick = (ts, price)
        bucket = (ts // self.period) * self.period
        if self.first_bucket is None:
            self.first_bucket = bucket
        current = self.forming
        if current is None or bucket > current["time"]:
            if current is not None:
                self.closed.append(dict(current))
                if current["time"] > self.first_bucket:  # the first bucket was joined mid-way
                    self.live_full.append(dict(current))
                    del self.live_full[:-400]
                    self._cross_check(current["time"])
            self.forming = {"time": bucket, "open": price, "high": price, "low": price, "close": price}
        elif bucket == current["time"]:
            current["high"] = max(current["high"], price)
            current["low"] = min(current["low"], price)
            current["close"] = price
        # ticks older than the forming bucket are ignored

    def _ingest_aligned(self, candle: dict) -> None:
        # The first window the session joined is partial; the library still emits it.
        if self.first_bucket is None or candle["time"] <= self.first_bucket:
            return
        self.aligned[candle["time"]] = candle
        for stale in sorted(self.aligned)[:-400]:
            del self.aligned[stale]
        self._cross_check(candle["time"])

    def _cross_check(self, t: int) -> None:
        """Compare the library's aligned candle with ours once both exist for bucket t."""
        if t in self._checked or t not in self.aligned:
            return
        own = next((c for c in reversed(self.live_full) if c["time"] == t), None)
        if own is None:
            return
        self._checked.add(t)
        theirs = self.aligned[t]
        tol = 1e-5 * own["close"]
        diff = max(abs(own[k] - theirs[k]) for k in ("open", "high", "low", "close"))
        self.cross["compared"] += 1
        if diff <= tol:
            self.cross["matched"] += 1
        else:
            log.info("%s aligned candle differs from tick-built at %s: aligned=%s ticks=%s",
                     self.symbol, t, theirs, own)
        self.info["cross"] = f"{self.cross['matched']}/{self.cross['compared']}"

    def _reference(self) -> list[dict]:
        """Complete live candles that history is verified against."""
        if self.source == "aligned" and self.aligned:
            return [self.aligned[t] for t in sorted(self.aligned)]
        return list(self.live_full)

    def _view_closed(self) -> list[dict]:
        """Closed candles for display: library-built where available, never a partial one."""
        out = []
        for candle in self.closed:
            if not self.history_applied and candle["time"] == self.first_bucket:
                continue  # joined mid-window: partial, and nothing verified to replace it
            out.append(self.aligned.get(candle["time"], candle) if self.source == "aligned" else candle)
        return out

    async def _read_ticks(self, subscription, queue: asyncio.Queue) -> None:
        try:
            async for tick in subscription:
                ts = candle_time(tick) or int(time.time())
                try:
                    price = float(tick.get("close", tick.get("price")))
                except (TypeError, ValueError):
                    continue
                if math.isfinite(price) and price > 0:
                    queue.put_nowait(("tick", ts, price))
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - reported to the consumer
            queue.put_nowait(("error", error))
        else:
            queue.put_nowait(("end",))

    async def _read_aligned(self, subscription, queue: asyncio.Queue) -> None:
        try:
            async for raw in subscription:
                candles = normalize([raw])
                if candles:
                    queue.put_nowait(("aligned", candles[0]))
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            queue.put_nowait(("note", f"Aligned stream stopped ({type(error).__name__}); using tick-built candles"))
            self.source = "ticks"

    # -- history
    async def _fetch_chart(self):
        return await self.client.client.history(self.symbol, self.period)

    async def _fetch_ticks(self):
        seconds = (self.max_rows + 10) * self.period
        return await self.client.client.compile_candles(self.symbol, self.period, seconds)

    async def _load_history(self, queue: asyncio.Queue) -> None:
        sources = [("chart", self._fetch_chart, CHART_TIMEOUT), ("ticks", self._fetch_ticks, TICKS_TIMEOUT)]
        need = self.verify_buckets if self.period <= 60 else 0
        try:
            for attempt in range(HISTORY_ATTEMPTS):
                # Wait until enough complete live candles exist to compare against.
                deadline = time.monotonic() + (need + 2) * self.period + 15
                while len(self._reference()) < need and time.monotonic() < deadline:
                    await asyncio.sleep(0.5)

                reasons = []
                for name, fetch, timeout in sources:
                    try:
                        async with self.lock:
                            payload = await asyncio.wait_for(fetch(), timeout)
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:  # noqa: BLE001
                        reasons.append(f"{name} {type(error).__name__}")
                        continue
                    candles = normalize(payload)
                    if not self.last_tick:
                        reasons.append(f"{name} no live tick yet")
                        continue
                    ref_time, ref_price = self.last_tick
                    result = assess_history(candles, self._reference(), self.period, ref_time, ref_price)
                    if result.ok:
                        result.reason = name  # remember which source won
                        queue.put_nowait(("history", result))
                        return
                    reasons.append(f"{name}: {result.reason}")
                    log.info("%s history from %s rejected: %s", self.symbol, name, result.reason)

                queue.put_nowait(("note", ("History not verified (" + "; ".join(reasons) + "). Building from live ticks.")[:200]))
                if attempt < HISTORY_ATTEMPTS - 1:
                    await asyncio.sleep(HISTORY_RETRY_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001
            log.warning("%s history task failed: %s", self.symbol, error)
            queue.put_nowait(("note", f"History loader failed: {type(error).__name__}"))

    def _apply_history(self, result: Assessment) -> None:
        cut = (self.first_bucket or 0) + self.period  # first bucket we saw completely
        history = [c for c in result.candles if c["time"] < cut]
        live = [c for c in self.closed if c["time"] >= cut]
        self.closed = deque((history + live)[-self.max_rows:], maxlen=self.max_rows)
        self.history_applied = True
        how = (f"verified against {result.matched}/{result.compared} complete live candles"
               if result.mode == "overlap" else "checked by age and price only")
        fix = f", clock corrected by {result.shift:+d}s" if result.shift else ""
        if result.trimmed:
            fix += f", dropped {result.trimmed} older candles after a break ({result.break_note})"
        self.info.update({
            "source": result.reason,
            "detail": f"History: {result.reason} feed, {len(history)} candles, {how}{fix}",
            "history": len(history),
            "shift": result.shift,
        })

    # -- stream
    async def stream(self):
        queue: asyncio.Queue = asyncio.Queue()
        subscription = aligned_sub = None
        reader = aligned_reader = history_task = None
        try:
            async with self.lock:
                subscription = await self.client.subscribe_symbol(self.symbol)
                reader = asyncio.create_task(self._read_ticks(subscription, queue))
                if self.source == "aligned":
                    aligned_sub = await self.client.subscribe_ticks(
                        self.symbol, interval=timedelta(seconds=self.period), aligned=True)
                    aligned_reader = asyncio.create_task(self._read_aligned(aligned_sub, queue))
                item = await asyncio.wait_for(queue.get(), FIRST_TICK_TIMEOUT)
            if self.use_history:
                history_task = asyncio.create_task(self._load_history(queue))
                self.info["detail"] = "Building from live ticks while history is verified"
            else:
                self.info["detail"] = "History off. The chart fills as candles close."
            first = True
            while True:
                if item is None:
                    item = await queue.get()
                current, item = item, None
                kind = current[0]
                if kind == "tick":
                    self._ingest(current[1], current[2])
                    event = "seed" if first else "tick"
                elif kind == "aligned":
                    self._ingest_aligned(current[1])
                    continue
                elif kind == "history":
                    self._apply_history(current[1])
                    event = "history"
                elif kind == "note":
                    self.info["detail"] = current[1]
                    event = "note"
                elif kind == "error":
                    raise current[1]
                else:
                    raise ConnectionError("tick stream ended")
                first = False
                yield self._view_closed(), (dict(self.forming) if self.forming else None), event
        finally:
            tasks = [t for t in (history_task, aligned_reader, reader) if t is not None]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for sub in (aligned_sub, subscription):
                if sub is not None:
                    try:
                        await asyncio.wait_for(sub.unsubscribe(), 5)
                    except Exception:  # noqa: BLE001 - teardown must not mask the real error
                        pass
