# Label Enricher

Label Enricher is a local Windows app for processing shipping labels and adding warehouse-friendly overlay text such as internal labels, locations, quantities, totals, and overflow notes.

It is designed for teams that print marketplace labels in batches and need fast in-house picking context without relying on marketplace APIs or browser automation.

## What It Does

- Processes eBay and Amazon label batches locally
- Reads order data from uploaded marketplace exports when available
- Matches label files to item records in `items.csv`
- Prints overlay text into configured label margins or onto follow-up pages
- Supports manual-entry batches for one-off labels or incomplete order data
- Keeps unprocessed and needs-review queues for anything that needs confirmation
- Lets you tune layout settings with a live preview in the Settings page

## Core Areas In The App

- **Dashboard**: upload, process, reprocess, and open combined output PDFs
- **Unprocessed Queue**: resolve unmatched labels and save variation choices
- **Items Database**: edit `items.csv`, review auto-added items, and manage staged imports
- **Manual Entry**: build labels directly when you already have the PDF and want to enter order details yourself
- **Layout & Settings**: configure line builder, spacing, overflow behavior, summary fallback pages, sorting, and preview options

## First-Time Setup

1. Install Python 3.11 or newer on Windows.
2. During install, enable **Add Python to PATH**.
3. Open this app folder.
4. Run `SETUP_FIRST_TIME.bat`.
5. When setup finishes, run `START_LABEL_ENRICHER.bat`.
6. Your browser should open automatically to the app.

## Starting And Stopping

- Start with `START_LABEL_ENRICHER.bat`
- Stop with `STOP_LABEL_ENRICHER.bat`

The start script automatically looks for an open local port and uses the first free one from:

- `8080`
- `8081`
- `8082`
- `8083`
- `8090`
- `9000`
- `10080`

The app runs only on `127.0.0.1`, so it stays local to that computer.

## Fresh Install Data Expectations

This repo is set up so fresh installs bring their own business data.

Not included by default:

- `items.csv`
- `config.yaml`
- `label_location_hints.csv`
- incoming customer label files

What that means:

- `config.yaml` is created automatically on first run if missing
- each install should import or create its own `items.csv`
- optional hint/location files should be created locally if used
- incoming and processed batch data stay local to the machine

## Typical Batch Workflow

1. Upload label PDFs or ZIPs on the Dashboard.
2. Upload the matching marketplace order export when available.
3. Process the batch.
4. Review any labels sent to **Unprocessed** or **Needs Review**.
5. Reprocess if needed after saving item or variation changes.
6. Open the combined PDF once the batch is clean.

If layout settings or `items.csv` change after a batch is processed, the app can prompt you to reprocess before opening the combined PDF so the output stays in sync.

## Manual Entry Workflow

Manual Entry is for cases where you already have the exact label PDF and want to key in the matching order/item details yourself.

Current manual-entry features include:

- label ZIP/PDF staging
- platform auto-detection from the selected label
- multi-label view
- multi-item sections within a label card
- item lookup suggestions based on item number, SKU, or ASIN
- autofill for internal label and location when a match is found

Manual entry is separate from the main batch queues so you can work on manual labels without clearing unrelated dashboard items first.

## Overlay And Overflow Features

The overlay system supports several label-layout behaviors:

- configurable line builder fields and ordering
- compact/secondary overflow regions
- spill text into other margins when space runs out
- `CONT BELOW` warnings when text continues into another region
- optional summary fallback page for larger overflows
- per-layout live preview from Settings

## Item Database Notes

The app is built around `items.csv`.

Typical uses:

- maintain item number, SKU, ASIN, internal label, and location data
- review auto-added items that need cleanup
- import replacement data through the staged import flow
- use hints and item matching to reduce manual corrections over time

## Optional Native Launcher

If you want a Windows EXE launcher for the app, run:

```powershell
BUILD_NATIVE_LAUNCHER.bat
```

That builds a small native launcher and copies the finished EXE into the app folder.

## Troubleshooting

- If the browser does not open automatically, check the console output for the chosen local port and open that `http://127.0.0.1:<port>` address manually.
- If a combined PDF looks outdated after item or layout edits, reprocess the latest batch before opening it.
- If labels do not match automatically, check the **Unprocessed** queue or use **Manual Entry**.
- If fresh installs look empty, that is expected until local `items.csv` and config data are added.

## Privacy And Security

Label Enricher is intended to run locally on Windows.

- no marketplace APIs
- no browser automation
- no cloud sync required for core use
- customer label files and processed output can stay on the local machine
