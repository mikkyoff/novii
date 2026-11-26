"""Find out which candle-history source matches the live market.

Run:   POCKET_OPTION_SSID='42["auth",{...}]' python history_probe.py [ASSET] [PERIOD]
       python history_probe.py EURHUF_otc 15

What it does
  1. Listens to live ticks for COLLECT_SECONDS (default 75) and builds candles from them,
     and also records the library's own clock-aligned candles for the same window.
  2. Fetches history from every source the library offers.
  3. Compares each source with the live candles for the same timestamps, tries clock shifts
     (multiples of 15 minutes) and prints a verdict.
  4. Writes history_probe_report.json, ready to attach to a bug report.

Use a machine with a correct clock (NTP). Takes about two minutes. It opens at most two
streams, both on one asset.
"""
from __future__ import annotations

import asyncio
import json
import os
import platform
import sys
import time
import warnings
from datetime import datetime, timedelta, timezone
from importlib import metadata

from BinaryOptionsToolsV2.pocketoption.asynchronous import PocketOptionAsync

from feed import assess_history, candle_time, median_range, normalize, spacing_stats

warnings.filterwarnings("ignore", category=DeprecationWarning)

ASSET = sys.argv[1] if len(sys.argv) > 1 else "EURHUF_otc"
PERIOD = int(sys.argv[2]) if len(sys.argv) > 2 else 15
COLLECT = float(os.environ.get("COLLECT_SECONDS", "75"))
SSID = os.environ.get("POCKET_OPTION_SSID", "").strip()
LAGOS = timezone(timedelta(hours=1))


def library_version() -> str:
    try:
        return metadata.version("BinaryOptionsToolsV2")
    except metadata.PackageNotFoundError:
        return "unknown"


def clock(t: float) -> str:
    return datetime.fromtimestamp(t, LAGOS).strftime("%H:%M:%S")


def build_candles(ticks: list[tuple[int, float]], period: int) -> list[dict]:
    out: list[dict] = []
    for ts, price in ticks:
        bucket = (ts // period) * period
        if out and out[-1]["time"] == bucket:
            c = out[-1]
            c["high"], c["low"], c["close"] = max(c["high"], price), min(c["low"], price), price
        elif not out or bucket > out[-1]["time"]:
            out.append({"time": bucket, "open": price, "high": price, "low": price, "close": price})
    return out


async def timed(label: str, awaitable, timeout: float):
    started = time.monotonic()
    try:
        value = await asyncio.wait_for(awaitable, timeout)
        return value, time.monotonic() - started, None
    except Exception as error:  # noqa: BLE001
        return None, time.monotonic() - started, f"{type(error).__name__}: {error}"


async def main() -> None:
    if not SSID:
        sys.exit("Set POCKET_OPTION_SSID first.")
    report: dict = {
        "environment": {
            "BinaryOptionsToolsV2": library_version(),
            "python": platform.python_version(),
            "asset": ASSET,
            "period": PERIOD,
            "machine_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    }
    print(f"BinaryOptionsToolsV2 {report['environment']['BinaryOptionsToolsV2']}, "
          f"Python {report['environment']['python']}, {ASSET}, {PERIOD}s candles")

    async with PocketOptionAsync(SSID) as client:
        ticks: list[tuple[int, float]] = []
        raw_ticks: list[dict] = []
        aligned_raw: list[dict] = []

        raw_sub = await client.subscribe_ticks(ASSET)
        aligned_sub = await client.subscribe_ticks(ASSET, interval=timedelta(seconds=PERIOD), aligned=True)

        async def read_ticks():
            async for tick in raw_sub:
                if len(raw_ticks) < 5:
                    raw_ticks.append(tick)
                ticks.append((candle_time(tick), float(tick.get("close", tick.get("price")))))

        async def read_aligned():
            async for candle in aligned_sub:
                aligned_raw.append(candle)

        readers = [asyncio.create_task(read_ticks()), asyncio.create_task(read_aligned())]
        print(f"Collecting live ticks for {COLLECT:.0f}s ...")
        await asyncio.sleep(COLLECT)
        if not ticks:
            sys.exit("No ticks arrived. Is the asset open, and is the SSID valid?")

        ref_time, ref_price = ticks[-1]
        skew = ref_time - time.time()
        first_bucket = (ticks[0][0] // PERIOD) * PERIOD
        built = build_candles(ticks, PERIOD)
        live_full = [c for c in built[:-1] if c["time"] > first_bucket]  # drop partial first and forming
        aligned = [c for c in normalize(aligned_raw) if c["time"] > first_bucket]
        print(f"  {len(ticks)} ticks, {len(live_full)} complete tick-built candles, "
              f"{len(aligned)} complete library-aligned candles")
        print(f"  tick clock minus this machine's clock: {skew:+.1f}s "
              f"({'looks like a whole-hour offset' if abs(skew) > 600 else 'clocks agree'})")

        report["live"] = {
            "ticks": len(ticks),
            "tick_clock_minus_machine_s": round(skew, 1),
            "raw_tick_samples": raw_ticks,
            "tick_built_candles": live_full,
            "library_aligned_candles": aligned,
        }

        # Do the two live builders agree with each other?
        by_time = {c["time"]: c for c in aligned}
        agree = [(c["time"], max(abs(c[k] - by_time[c["time"]][k]) for k in ("open", "high", "low", "close")))
                 for c in live_full if c["time"] in by_time]
        if agree:
            worst = max(d for _, d in agree)
            print(f"  tick-built vs library-aligned: {len(agree)} shared candles, worst difference {worst:.6g}")
        reference = aligned if len(aligned) >= 2 else live_full
        print(f"  verifying history against {len(reference)} complete live candles "
              f"({'library-aligned' if reference is aligned else 'tick-built'})\n")

        # One request at a time: the broker drops sockets that get several at once.
        sources = {
            "chart_history (client.history, changeSymbol feed)": lambda: client.client.history(ASSET, PERIOD),
            "tick_pages (compile_candles, loadHistoryPeriod)": lambda: client.client.compile_candles(
                ASSET, PERIOD, 110 * PERIOD),
        }
        report["sources"] = {}
        for name, make_call in sources.items():
            payload, seconds, error = await timed(name, make_call(), 40)
            result = {"fetch_seconds": round(seconds, 1), "error": error}
            print(f"== {name}")
            if error:
                print(f"   FAILED after {seconds:.1f}s: {error}\n")
                report["sources"][name] = result
                continue
            candles = normalize(payload)
            raw_count = len(json.loads(payload)) if isinstance(payload, str) else len(payload or [])
            print(f"   {len(candles)} usable candles of {raw_count} returned, fetched in {seconds:.1f}s")
            if candles:
                age = ref_time - candles[-1]["time"]
                print(f"   first {clock(candles[0]['time'])}  last {clock(candles[-1]['time'])} "
                      f"(newest is {age:.0f}s before the live clock)")
                print(f"   median candle range {median_range(candles[-30:]):.6g}, last close {candles[-1]['close']}, "
                      f"live price {ref_price}")
            if candles:
                sp = spacing_stats(candles, PERIOD)
                print(f"   spacing: {sp['exact_fraction']:.0%} of neighbours are exactly {PERIOD}s apart, "
                      f"{sp['aligned_fraction']:.0%} aligned to {PERIOD}s; most common gaps {sp['common_spacings']}")
            verdict = assess_history(candles, reference, PERIOD, ref_time, ref_price)
            if verdict.ok:
                if verdict.trimmed:
                    print(f"   NOTE: only the newest {len(verdict.candles)} candles form an unbroken chain; "
                          f"{verdict.trimmed} older ones dropped ({verdict.break_note})")
                fix = f", needs a clock shift of {verdict.shift:+d}s" if verdict.shift else ""
                print(f"   VERDICT: matches the market ({verdict.matched}/{verdict.compared} overlapping candles){fix}")
            else:
                print(f"   VERDICT: REJECTED, {verdict.reason}")
            for row in verdict.rows[:6]:
                flag = "ok  " if row["match"] else "DIFF"
                print(f"     {flag} {clock(row['time'])} live C={row['live'][3]:.6g}  source C={row['source'][3]:.6g}  "
                      f"max diff {row['max_diff']:.6g}")
            print()
            result.update(
                usable=len(candles), returned=raw_count, verdict=verdict.to_dict(),
                first_three=candles[:3], last_three=candles[-3:], overlap_rows=verdict.rows,
            )
            report["sources"][name] = result

        # What get_candles_live / candles / get_candles / candles_many would have used.
        gen = client.get_candles_live(ASSET, PERIOD, hours=1.0, max_rows=99)
        seed, seconds, error = await timed("get_candles_live", anext(gen), 60)
        try:
            await gen.aclose()
        except Exception:  # noqa: BLE001
            pass
        print("== get_candles_live seed (same path as candles(), get_candles(), candles_many())")
        if error:
            print(f"   FAILED after {seconds:.1f}s: {error}\n")
            report["sources"]["get_candles_live_seed"] = {"error": error}
        else:
            closed = normalize(seed[0])
            verdict = assess_history(closed, reference, PERIOD, ref_time, ref_price)
            state = "matches the market" if verdict.ok else f"REJECTED, {verdict.reason}"
            print(f"   {len(closed)} candles, newest {ref_time - closed[-1]['time'] if closed else 'n/a'}s old: {state}\n")
            report["sources"]["get_candles_live_seed"] = {
                "usable": len(closed), "verdict": verdict.to_dict(), "first_three": closed[:3], "last_three": closed[-3:],
            }

        for task in readers:
            task.cancel()
        await asyncio.gather(*readers, return_exceptions=True)

    good = [n for n, r in report["sources"].items() if r.get("verdict", {}).get("ok") and not n.startswith("get_candles_live")]
    print("RECOMMENDATION")
    if good:
        print(f"  Use: {good[0]}. Run the app with CANDLE_SOURCE=aligned (default) and USE_HISTORY=1.")
    else:
        print("  No history source matches the live market. Run the app with USE_HISTORY=0 so the chart")
        print("  fills from live candles, and send history_probe_report.json to the developer.")
    report["recommendation"] = good[0] if good else "none"
    with open("history_probe_report.json", "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, default=str)
    print("\nSaved history_probe_report.json")


if __name__ == "__main__":
    asyncio.run(main())
