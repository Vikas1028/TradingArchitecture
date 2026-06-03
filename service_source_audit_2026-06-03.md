# Service Source Audit

Date: 2026-06-03
Repo: `/Users/vikasbhandekar/Desktop/TradingArchitecture`
Branch: `dev`
Commit: `01a5e2b7b1a4f9d22838a0cf0cbf33a6e157dd11`

## Summary

There are three service states in this workspace:

1. Full source present in working tree and tracked in Git `HEAD`
2. Source was tracked in Git `HEAD` but was missing from the working tree and has now been restored
3. Binary/config only in the working tree, with no tracked source in Git `HEAD`

No recoverable source copies were found in `~/.Trash`.

## Restored From Git HEAD

These services had tracked source in Git `HEAD`, but the files were missing from the working tree. They were restored to their correct repo paths using `git archive HEAD <service> | tar -xk`.

- `go_feed`
- `go_ltp`
- `marketdata`
- `market_price`
- `paper_engine`
- `first_candle_strategy`
- `trading_dashboard`
- `volatile_strategy`
- `news_stratergy`
- `vwap_strategy`
- `ldrb_strategy`

Examples of restored files:

- `/Users/vikasbhandekar/Desktop/TradingArchitecture/paper_engine/cmd/paper_engine/main.go`
- `/Users/vikasbhandekar/Desktop/TradingArchitecture/paper_engine/internal/engine/processor.go`
- `/Users/vikasbhandekar/Desktop/TradingArchitecture/go_feed/kafka/kafka.go`
- `/Users/vikasbhandekar/Desktop/TradingArchitecture/trading_dashboard/cmd/main.go`

## Full Source Present Now

These services now have substantial source in the working tree and tracked files in Git `HEAD`.

- `go_feed`
- `go_ltp`
- `marketdata`
- `market_price`
- `paper_engine`
- `first_candle_strategy`
- `vwap_strategy`
- `ldrb_strategy`
- `volatile_strategy`
- `news_stratergy`
- `trading_dashboard`
- `python_feed`

## Local Source Present But Not Tracked In Git HEAD

These services have local source files in the working tree, but `git ls-tree -r HEAD <service>` returns `0`. That means they exist locally, but are not present in the visible repo history of this clone.

- `trend_recognition`
- `real_signal_router`
- `real_engine_broker_sync`
- `strategy_signal_archive`
- `s2_fallback_selector`
- `trading_dashboard_wrapper`

## Binary Or Config Only In Working Tree

These services do not have usable implementation source in the working tree, and Git `HEAD` also has `0` tracked files for them.

- `real_engine`
- `openmarketvolatility`
- `fast_movers_stratergy`
- `chart_view`
- `filters`
- `go_option_feed`

What is present for these services is mostly:

- config loader(s)
- plist / start / stop scripts
- built binaries
- logs / release artifacts

## Service Inventory Snapshot

Format: `service | working_tree_go | working_tree_go_mod | working_tree_py | git_head_files`

- `chart_view | 1 | 0 | 0 | 0`
- `fast_movers_stratergy | 1 | 0 | 0 | 0`
- `filters | 0 | 0 | 0 | 0`
- `first_candle_strategy | 11 | 1 | 0 | 21`
- `go_feed | 15 | 1 | 0 | 23`
- `go_ltp | 16 | 1 | 0 | 24`
- `go_option_feed | 1 | 0 | 0 | 0`
- `ldrb_strategy | 11 | 1 | 0 | 26`
- `market_price | 8 | 1 | 0 | 12`
- `marketdata | 11 | 1 | 0 | 21`
- `marketdata_go_feed | 11 | 1 | 0 | 20`
- `news_stratergy | 12 | 1 | 0 | 17`
- `openmarketvolatility | 1 | 0 | 0 | 0`
- `paper_engine | 16 | 1 | 0 | 26`
- `python_feed | 0 | 0 | 21 | 26`
- `real_engine | 1 | 0 | 0 | 0`
- `real_engine_broker_sync | 1 | 1 | 0 | 0`
- `real_signal_router | 1 | 1 | 0 | 0`
- `s2_fallback_selector | 1 | 1 | 0 | 0`
- `strategy_signal_archive | 1 | 1 | 0 | 0`
- `trading_dashboard | 7 | 1 | 0 | 13`
- `trading_dashboard_wrapper | 1 | 1 | 0 | 0`
- `trend_recognition | 3 | 1 | 0 | 0`
- `volatile_strategy | 11 | 1 | 0 | 15`
- `vwap_strategy | 12 | 1 | 0 | 22`

## Lineage / Copy Analysis

### `paper_engine` vs `real_engine`

Conclusion:

- `real_engine` is not just the `paper_engine` binary renamed.
- `real_engine` is a sibling or fork in the same architecture family.
- Its implementation source existed when the binary was built, but that source is missing from this repo snapshot.

Evidence:

- `paper_engine` source is present and includes:
  - `internal/engine/processor.go`
  - `internal/kafka/*`
  - `internal/dashboard/store.go`
- `real_engine` binary embeds source paths for its own distinct packages:
  - `/Users/vikasbhandekar/Desktop/TradingArchitecture/real_engine/internal/dhan/client.go`
  - `/Users/vikasbhandekar/Desktop/TradingArchitecture/real_engine/internal/engine/processor.go`
  - `/Users/vikasbhandekar/Desktop/TradingArchitecture/real_engine/internal/postgres/trades_store.go`
  - `/Users/vikasbhandekar/Desktop/TradingArchitecture/real_engine/cmd/real_engine/main.go`
  - `/Users/vikasbhandekar/Desktop/TradingArchitecture/real_engine/cmd/real_engine/trades_api.go`
- `real_engine` binary exports functions that do not exist in `paper_engine`, especially broker execution pieces:
  - `real_engine/internal/dhan.(*Client).PlaceMarketOrder`
  - `real_engine/internal/dhan.(*Client).fetchOrderDetails`
  - `real_engine/internal/engine.(*Engine).OnTick`
  - `real_engine/internal/engine.(*Engine).ForceEODExits`
  - `real_engine/internal/engine.(*Engine).RestoreTradeHistory`
- Shared structure suggests the same design lineage:
  - `internal/engine`
  - `internal/kafka`
  - `internal/dashboard`
  - `internal/config`
  - event/trade-store patterns

Operational interpretation:

- `real_engine` was very likely created by copying or forking `paper_engine` and then adding:
  - broker integration
  - postgres trade store
  - live order/fill handling
  - tick-driven exit logic

### `marketdata` vs `marketdata_go_feed`

Conclusion:

- These are almost certainly copy/fork siblings.

Evidence:

- Large overlap in tree layout and filenames:
  - `cmd/marketdata/main.go`
  - `common/constants.go`
  - `internal/candle/builder.go`
  - `internal/kafka/consumer.go`
  - `internal/kafka/producer.go`
  - `internal/metrics/metrics.go`

### `go_feed` vs `go_ltp`

Conclusion:

- These are sibling services built from a near-common codebase.

Evidence:

- Strong file overlap:
  - `cmd/init.go`
  - `cmd/main.go`
  - `common/*`
  - `websocket/*`
  - `kafka/kafka.go`
  - `postgres/postgres.go`
- Primary divergence is service behavior:
  - `go_feed` is websocket event feed
  - `go_ltp` includes `ltp/poller.go`

### `trading_dashboard` vs `trading_dashboard_wrapper`

Conclusion:

- `trading_dashboard_wrapper` is not a copy of `trading_dashboard`.
- It is a thin wrapper/proxy around the dashboard service.

Evidence:

- Shared overlap is minimal.
- Wrapper has its own single entrypoint:
  - `/Users/vikasbhandekar/Desktop/TradingArchitecture/trading_dashboard_wrapper/main.go`
- Main dashboard has its own internal web and source packages:
  - `/Users/vikasbhandekar/Desktop/TradingArchitecture/trading_dashboard/internal/web/server.go`
  - `/Users/vikasbhandekar/Desktop/TradingArchitecture/trading_dashboard/internal/source/poller.go`

### Other Binary-Only Services

Binary inspection shows these were also standalone services with their own missing implementation trees, not just renamed binaries:

- `openmarketvolatility`
  - embedded packages: `internal/strategy`, `internal/dashboard`, `internal/jetstream`
- `fast_movers_stratergy`
  - embedded packages: `internal/strategy`, `internal/dashboard`, `internal/jetstream`
- `chart_view`
  - embedded packages: `internal/http`, `internal/config`

`go_option_feed` appears to have a very incomplete local folder and may have been assembled differently; it needs separate recovery from another repo copy or backup.

## Bottom Line

1. A large part of the working tree had tracked source missing. That has now been restored where Git `HEAD` still had it.
2. `real_engine`, `openmarketvolatility`, `fast_movers_stratergy`, `chart_view`, `filters`, and `go_option_feed` are still source-incomplete because the visible Git history in this clone does not contain their implementation files.
3. `real_engine` is not the same thing as `paper_engine`; it is a fork/sibling with its own missing source tree, and the binary itself proves that source once existed under `/Users/vikasbhandekar/Desktop/TradingArchitecture/real_engine/...`.
