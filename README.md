# Label Enricher (Windows Local App)

Label Enricher processes shipping labels from eBay and Amazon and adds warehouse info (location, internal SKU/label, quantity, total paid) in a safe overlay area or backside page.

## V1 Constraints
- No marketplace APIs
- No browser automation
- Local-only processing on Windows

## First-Time Setup (Non-Technical)
1. Install Python 3.11+ (Windows x64) from [python.org](https://www.python.org/downloads/windows/).
2. In Python installer, check **Add Python to PATH**.
3. Open the app folder.
4. Double-click `SETUP_FIRST_TIME.bat`.
5. After setup, double-click `START_LABEL_ENRICHER.bat`.
6. Open [http://127.0.0.1:8081](http://127.0.0.1:8081)

## Daily Start
- Double-click `START_LABEL_ENRICHER.bat`
- Open [http://127.0.0.1:8081](http://127.0.0.1:8081)

## If You Restart the PC
- The app does not auto-run by default.
- Just double-click `START_LABEL_ENRICHER.bat` again.

## Updating Packages Later
- Double-click `UPDATE_DEPENDENCIES.bat`

## Why localhost can conflict
- `localhost` means “this same PC only”.
- Port numbers are like room numbers.
- If another app is using `8080`, your app must use another port (we use `8081` in the start script).

## What To Put In `incoming/batch`
- Label PDFs or ZIP of label PDFs
- eBay OrdersReport CSV (if processing eBay labels)
- Amazon order report TXT (optional, but recommended for totals)

## Daily Workflow
1. Upload/drop files into dashboard (or place files directly in `incoming/batch`).
2. Confirm readiness cards (labels, eBay CSV, Amazon TXT).
3. Click **Process Batch**.
4. Open outputs in `processed/batch_YYYYMMDD_HHMMSS/output_pdfs/`.
5. If needed, resolve unmatched labels in **Resolve Match** page.

## Amazon Overlap Rule (Important)
- App extracts order IDs from Amazon label ZIP filenames.
- If Amazon `.txt` has extra rows, app only keeps rows whose `order-id` appears in ZIP labels.
- This allows multiple batches/day without overlap errors.

## Item Database (`items.csv`)
Two options:
- Edit in Excel directly
- Use **Item Database** page in UI

The app auto-adds new item IDs with `needs_review=1`.

## Troubleshooting
- **Missing orders data**: add eBay OrdersReport CSV and/or Amazon Order Report TXT.
- **Multiple CSV confusion**: keep one eBay OrdersReport CSV per batch.
- **Ambiguous matches**: check **Resolve Match** queue.
- **PDF text extraction issues**: label may still match by filename order ID (Amazon) or manual resolve.

## Purging Archives
Dashboard includes **Purge Old Archives** by retention days. This deletes old `processed/batch_*` folders.

## Build Portable EXE (PyInstaller)
Install dependencies first, then:

```powershell
pyinstaller --noconfirm --onefile --name LabelEnricher ^
  --add-data "templates;templates" ^
  --add-data "static;static" ^
  app/main.py
```

### Portable Folder Layout

```text
LabelEnricher_Portable/
  LabelEnricher.exe
  config.yaml
  items.csv
  incoming/
    batch/
  processed/
  logs/
```

## Security Note
V1 is local-only and does not use APIs or browser login automation.

## Margin Tuning (New)
Use **Settings** to fine-tune placement and text layout:
- `Edge Inset X`: smaller pushes text box closer to left/right edge.
- `Edge Inset Y`: smaller pushes box closer to top/bottom edge.
- `Margin Box Width/Height`: controls overlay area size.
- `Text Align`: left/center/right.
- `Wrap Mode`:
  - `truncate` = one-line cut with `...`
  - `word` = wrap by words
  - `char` = wrap by characters
- `Max Lines`: hard line cap before backside overflow rule.

Suggested starting values for “way on the edge” right margin:
- `Placement Preset`: `right_margin`
- `Edge Inset X`: `3`
- `Edge Inset Y`: `18`
- `Margin Box Width`: `200`
- `Text Align`: `left`
- `Wrap Mode`: `word`

