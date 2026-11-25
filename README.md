# Eagle Lenz stream test

Streams `EURHUF_otc` and `EURUSD_otc` on 15 second candles through one Pocket Option SSID.
Each chart shows the last 99 closed candles plus the live candle, updated on every tick.

## Deploy on Railway

1. Push this folder to a GitHub repo (the `Dockerfile` is at the repo root).
2. Railway: New Project, Deploy from GitHub repo, pick the repo. It builds the Dockerfile.
3. Service Variables:

   | Variable | Value |
   |---|---|
   | `POCKET_OPTION_SSID` | your full SSID string, e.g. `42["auth",{...}]` (required) |
   | `ACCESS_KEY` | any long random string (recommended) |

4. Settings, Networking, Generate Domain. Open `https://YOUR-DOMAIN/?key=YOUR_ACCESS_KEY`.

Keep the service at **1 replica**. A second replica opens a second session with the same SSID.
Do not run the same SSID locally while it is deployed.

## Optional variables

| Variable | Default | Meaning |
|---|---|---|
| `ASSETS` | `EURHUF_otc,EURUSD_otc` | Max 2 (broker streams about 2 symbols per session) |
| `PERIOD_SECONDS` | `15` | Candle size |
| `CLOSED_CANDLES` | `99` | Closed candles kept on the chart |
| `STALE_AFTER_SECONDS` | `5` | Badge turns to Stale |
| `STALL_SECONDS` | `30` | No tick this long: reconnect the SSID |
| `BACKOFF_SECONDS` | `5,10,20,40,60,120` | Reconnect delays (jitter added) |
| `AUTH_RETRY_SECONDS` | `300` | Wait if the broker never confirmed the SSID |
| `CANDLE_SOURCE` | `aligned` | `aligned`: closed candles from the library's clock-aligned subscription (the developer's suggestion). `ticks`: our own tick-built candles |
| `USE_HISTORY` | `1` | `0` skips history entirely: the chart starts empty and fills as candles close |
| `VERIFY_BUCKETS` | `3` | Complete live candles history must match before it is trusted |

## Candle history

The chart never trusts history blindly. It builds candles from live ticks, then fetches the broker
chart history and the tick-page history, and only merges a source whose candles match the live
candles for the same timestamps (it also tries clock shifts in 15 minute steps). The line under each
chart says what happened, for example `History: chart feed, 99 candles, verified against 3/3 complete
live candles`, or `History not verified (...). Building from live ticks.`

The first candle after a start is hidden unless verified history replaces it, because joining
mid-candle gives a partial candle.

To see which history source is right, run the probe once (about two minutes):

    POCKET_OPTION_SSID='42["auth",{...}]' python history_probe.py EURHUF_otc 15

It prints a verdict per source and writes `history_probe_report.json` for the developer. On Railway,
set a temporary custom start command `python history_probe.py EURHUF_otc 15` and read the deploy logs
(the report file is not kept; copy the printed output).

## Notes

- Reconnects restart the whole session, because the server ignores unsubscribe requests.
- If the broker never confirms the SSID, the page shows a red banner and retries only every 5 minutes.
- `/health` returns status only (no prices, no SSID) for Railway's healthcheck.
- Run locally: `pip install -r requirements.txt`, set the variables, `python app.py`.
