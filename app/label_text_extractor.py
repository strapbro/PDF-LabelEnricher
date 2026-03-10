from __future__ import annotations

import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import fitz
from pypdf import PdfReader


ZIP_RE = re.compile(r"\b(\d{5})(?:-?(\d{4}))?\b")
TRACKING_USPS_RE = re.compile(r"\b(9[0-9]{19,25})\b")
TRACKING_UPS_RE = re.compile(r"\b1Z[0-9A-Z]{16}\b", re.IGNORECASE)
TRACKING_FEDEX_RE = re.compile(r"\b(\d{12}|\d{15}|\d{20}|\d{22})\b")
AMZ_ORDER_RE = re.compile(r"\b\d{3}-\d{7}-\d{7}\b")
EBAY_ORDER_RE = re.compile(r"\b\d{2}-\d{5}-\d{5}\b")

BUSINESS_WORDS = {"group", "llc", "inc", "company", "corp", "corporation", "goods", "supply", "supplies", "logistics", "shipping"}


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _extract_text(pdf_path: Path) -> tuple[str, str]:
    page_text = ""
    words_text = ""
    try:
        with fitz.open(str(pdf_path)) as doc:
            if doc.page_count > 0:
                page = doc[0]
                # Try multiple rotation matrices because carrier labels are often
                # stored at +/-90 and plain extraction can miss core fields.
                text_candidates: list[str] = []
                word_candidates: list[str] = []
                for rot in (0, 90, 180, 270):
                    try:
                        m = fitz.Matrix(1, 1).prerotate(rot)
                        tp = page.get_textpage(matrix=m)
                        t = page.get_text("text", textpage=tp) or ""
                        if t.strip():
                            text_candidates.append(t)

                        words = page.get_text("words", textpage=tp) or []
                        ws = " ".join(str(w[4]) for w in words if len(w) > 4 and str(w[4]).strip())
                        if ws.strip():
                            word_candidates.append(ws)
                    except Exception:
                        continue

                page_text = max(text_candidates, key=len) if text_candidates else ""
                words_text = _normalize_space(max(word_candidates, key=len)) if word_candidates else ""
    except Exception:
        pass

    try:
        reader = PdfReader(str(pdf_path))
        if reader.pages and not page_text:
            page_text = reader.pages[0].extract_text() or ""
    except Exception:
        pass

    return page_text, words_text


def _looks_like_ebay_label_pdf(pdf_path: Path) -> bool:
    return "ebay-label" in (pdf_path.name or "").lower()


def _ocr_windows_image_lines(image_path: Path) -> list[str]:
    ps_script = f"""
Add-Type -AssemblyName System.Runtime.WindowsRuntime
$null = [Windows.Storage.StorageFile, Windows.Storage, ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.BitmapDecoder, Windows.Graphics.Imaging, ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.SoftwareBitmap, Windows.Graphics.Imaging, ContentType=WindowsRuntime]
$null = [Windows.Media.Ocr.OcrEngine, Windows.Media.Ocr, ContentType=WindowsRuntime]
function Await($op, [Type]$resultType) {{
    $asTask = [System.WindowsRuntimeSystemExtensions].GetMethods() | Where-Object {{ $_.Name -eq 'AsTask' -and $_.IsGenericMethod -and $_.GetParameters().Count -eq 1 }} | Select-Object -First 1
    $netTask = $asTask.MakeGenericMethod($resultType).Invoke($null, @($op))
    $netTask.Wait(-1) | Out-Null
    return $netTask.Result
}}
$path = '{str(image_path).replace("'", "''")}'
$file = Await ([Windows.Storage.StorageFile]::GetFileFromPathAsync($path)) ([Windows.Storage.StorageFile])
$stream = Await ($file.OpenAsync([Windows.Storage.FileAccessMode]::Read)) ([Windows.Storage.Streams.IRandomAccessStream])
$decoder = Await ([Windows.Graphics.Imaging.BitmapDecoder]::CreateAsync($stream)) ([Windows.Graphics.Imaging.BitmapDecoder])
$bitmap = Await ($decoder.GetSoftwareBitmapAsync()) ([Windows.Graphics.Imaging.SoftwareBitmap])
$engine = [Windows.Media.Ocr.OcrEngine]::TryCreateFromUserProfileLanguages()
$result = Await ($engine.RecognizeAsync($bitmap)) ([Windows.Media.Ocr.OcrResult])
($result.Lines | ForEach-Object {{ $_.Text }}) -join "`n"
"""
    try:
        proc = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_script],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except Exception:
        return []
    if proc.returncode != 0:
        return []
    return [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]


def _ocr_ebay_ups_recipient_block(pdf_path: Path) -> list[str]:
    try:
        with fitz.open(str(pdf_path)) as doc:
            if doc.page_count <= 0:
                return []
            page = doc[0]
            rect = page.rect
            # Tuned from the provided eBay UPS sample: sender + recipient live in
            # the upper-right panel of the source label orientation.
            clip = fitz.Rect(rect.width * 0.60, rect.height * 0.03, rect.width * 0.98, rect.height * 0.52)
            with tempfile.TemporaryDirectory(prefix="label_ocr_") as tmpdir:
                out_path = Path(tmpdir) / "ebay_ups_recipient.png"
                pix = page.get_pixmap(matrix=fitz.Matrix(4, 4).prerotate(270), clip=clip, alpha=False)
                pix.save(str(out_path))
                return _ocr_windows_image_lines(out_path)
    except Exception:
        return []


def _extract_tracking(full_text: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9]", "", full_text).upper()

    m = TRACKING_UPS_RE.search(compact)
    if m:
        return m.group(0).upper()

    m = TRACKING_USPS_RE.search(compact)
    if m:
        return m.group(1)

    for m in TRACKING_FEDEX_RE.finditer(compact):
        val = m.group(1)
        if len(val) >= 12:
            return val

    return ""


def _detect_carrier(full_text: str, tracking_number: str) -> str:
    text = (full_text or "").lower()
    tracking = re.sub(r"[^A-Za-z0-9]", "", (tracking_number or "").upper())

    if tracking.startswith("1Z"):
        return "ups"
    if tracking.startswith(("92", "93", "94", "95", "96")) and len(tracking) >= 20:
        return "usps"
    if tracking.isdigit() and len(tracking) in {12, 15, 20, 22}:
        return "fedex"

    if "ups" in text or "united parcel service" in text:
        return "ups"
    if "fedex" in text or "federal express" in text:
        return "fedex"
    if "usps" in text or "united states postal service" in text or "priority mail" in text:
        return "usps"
    return ""


def _candidate_lines(text: str, words_text: str) -> list[str]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if words_text:
        # Keep a coarse words fallback in case text lines are badly rotated,
        # but avoid single-token OCR scraps that can look like a city name.
        lines.append(_normalize_space(words_text))
    return lines


def _is_noise_line(line: str) -> bool:
    ll = line.lower()
    noise_substrings = [
        "ship to",
        "from",
        "tracking",
        "usps",
        "ups",
        "fedex",
        "us postage",
        "postage",
        "paid",
        "pitney",
        "amazon",
        "ebay",
        "commprice",
        "no surcharge",
        "surcharge",
        "weight",
        "priority mail",
        "ground",
        "label",
    ]
    if any(t in ll for t in noise_substrings):
        return True
    return bool(re.search(r"\b(?:lb|lbs|oz)\b", ll))


def _extract_shipto_block(lines: list[str]) -> list[str]:
    anchors = ["ship to", "shipto", "to:", "deliver to", "recipient"]
    for i, ln in enumerate(lines[:120]):
        ll = ln.lower()
        if any(a in ll for a in anchors):
            return lines[i + 1 : min(i + 10, len(lines))]
    return []


def _looks_like_person_name(line: str) -> bool:
    if len(line) < 3:
        return False
    if _is_noise_line(line):
        return False
    if re.search(r"\d", line):
        return False
    parts = [p for p in re.split(r"\s+", line.strip()) if p]
    return len(parts) >= 2


def _looks_like_single_name(line: str) -> bool:
    cand = (line or "").strip()
    if len(cand) < 3:
        return False
    if _is_noise_line(cand):
        return False
    if re.search(r"\d", cand):
        return False
    parts = [p for p in re.split(r"\s+", cand) if p]
    if len(parts) != 1:
        return False
    token = parts[0]
    if not token.isalpha():
        return False
    return token[0].isupper() and len(token) >= 4


def _looks_like_city_state_zip(line: str, postal: str = "") -> bool:
    cand = _normalize_space(line)
    if not cand:
        return False
    if postal and postal in cand:
        return True
    if re.search(r"\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b", cand):
        return True
    if re.search(r"\b[A-Za-z][A-Za-z .'-]+,\s*[A-Z]{2}\b", cand):
        return True
    return False


def _looks_like_street_line(line: str) -> bool:
    cand = _normalize_space(line)
    if not cand:
        return False
    return bool(
        re.search(r"\d", cand)
        and re.search(
            r"\b(?:st|street|ave|avenue|rd|road|dr|drive|ln|lane|ct|court|blvd|boulevard|trl|trail|way|pkwy|parkway|cir|circle|hwy|highway|apt|unit|ste|suite)\b",
            cand,
            re.IGNORECASE,
        )
    )


def _looks_like_filename_noise(line: str, pdf_name: str) -> bool:
    cand = _normalize_space(line).lower()
    if not cand:
        return False
    pdf_token = (pdf_name or "").strip().lower()
    if pdf_token and (cand == pdf_token or pdf_token in cand):
        return True
    if cand.endswith(".pdf") or "__p" in cand:
        return True
    if re.search(r"\b[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\b", cand):
        return True
    return False


def _looks_like_business_name(line: str) -> bool:
    parts = [p.lower() for p in re.split(r"\s+", _normalize_space(line)) if p]
    return any(p in BUSINESS_WORDS for p in parts)


def _valid_recipient_candidate(line: str, postal: str = "", pdf_name: str = "") -> bool:
    cand = (line or "").strip()
    if not cand:
        return False
    if _looks_like_city_state_zip(cand, postal):
        return False
    if _looks_like_street_line(cand):
        return False
    if _looks_like_filename_noise(cand, pdf_name):
        return False
    if _looks_like_business_name(cand):
        return False
    return _looks_like_person_name(cand) or _looks_like_single_name(cand)



def _group_words_into_lines(words: list[tuple[float, float, float, float, str]]) -> list[dict[str, Any]]:
    sorted_words = sorted(words, key=lambda w: (round(float(w[1]), 1), float(w[0])))
    lines: list[dict[str, Any]] = []
    for x0, y0, x1, y1, token in sorted_words:
        clean = str(token or "").strip()
        if not clean:
            continue
        placed = False
        for line in lines:
            if abs(float(line["y"]) - float(y0)) <= 6:
                line["words"].append((float(x0), clean))
                line["y"] = min(float(line["y"]), float(y0))
                line["x0"] = min(float(line["x0"]), float(x0))
                line["x1"] = max(float(line["x1"]), float(x1))
                placed = True
                break
        if not placed:
            lines.append({"y": float(y0), "x0": float(x0), "x1": float(x1), "words": [(float(x0), clean)]})
    out: list[dict[str, Any]] = []
    for line in sorted(lines, key=lambda ln: (float(ln["y"]), float(ln["x0"]))):
        words_sorted = [w for _, w in sorted(line["words"], key=lambda item: item[0])]
        text_line = _normalize_space(" ".join(words_sorted))
        if text_line:
            out.append({"text": text_line, "y": float(line["y"]), "x0": float(line["x0"]), "x1": float(line["x1"])} )
    return out


def _extract_positioned_shipto_block(pdf_path: Path) -> list[str]:
    best_block: list[str] = []
    best_score = -1
    try:
        with fitz.open(str(pdf_path)) as doc:
            if doc.page_count <= 0:
                return []
            page = doc[0]
            for rot in (0, 90, 180, 270):
                try:
                    m = fitz.Matrix(1, 1).prerotate(rot)
                    tp = page.get_textpage(matrix=m)
                    raw_words = page.get_text("words", textpage=tp) or []
                except Exception:
                    continue
                words: list[tuple[float, float, float, float, str]] = []
                for w in raw_words:
                    if len(w) < 5:
                        continue
                    words.append((float(w[0]), float(w[1]), float(w[2]), float(w[3]), str(w[4])))
                if not words:
                    continue
                lines = _group_words_into_lines(words)
                for idx, line in enumerate(lines[:120]):
                    ll = str(line.get("text", "") or "").lower()
                    if "ship to" not in ll and ll != "ship to:" and ll != "ship to":
                        continue
                    block = [str(ln.get("text", "") or "").strip() for ln in lines[idx + 1 : min(idx + 7, len(lines))] if str(ln.get("text", "") or "").strip()]
                    if not block:
                        continue
                    score = 0
                    if any(ZIP_RE.search(ln) for ln in block):
                        score += 3
                    if any(_valid_recipient_candidate(ln, "", pdf_path.name) for ln in block[:3]):
                        score += 3
                    if any(_looks_like_street_line(ln) for ln in block[:4]):
                        score += 2
                    if score > best_score:
                        best_score = score
                        best_block = block
    except Exception:
        return []
    return best_block


def _recipient_from_shipto_text(text: str, postal: str, pdf_name: str) -> str:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for i, line in enumerate(lines):
        if "ship to" not in line.lower():
            continue
        for cand in lines[i + 1 : min(i + 8, len(lines))]:
            if _valid_recipient_candidate(cand, postal, pdf_name):
                return cand
    return ""


def _recipient_from_zip_context(lines: list[str], postal: str, pdf_name: str = "") -> str:
    if not postal:
        return ""

    for i, ln in enumerate(lines[:160]):
        if postal not in ln:
            continue
        for k in (1, 2, 3):
            j = i - k
            if j < 0:
                break
            cand = lines[j].strip()
            if _valid_recipient_candidate(cand, postal, pdf_name):
                return cand
    return ""


def _pick_recipient(block: list[str], all_lines: list[str], postal: str, pdf_name: str = "", raw_text: str = "") -> str:
    def pick(cands: list[str]) -> str:
        for ln in cands:
            if _valid_recipient_candidate(ln, postal, pdf_name):
                return ln
        return ""

    r = pick(block)
    if r:
        return r

    if raw_text:
        r = _recipient_from_shipto_text(raw_text, postal, pdf_name)
        if r:
            return r

    r = _recipient_from_zip_context(all_lines, postal, pdf_name)
    if r:
        return r

    r = pick(all_lines[:100])
    if r:
        return r

    for ln in all_lines[:120]:
        if _valid_recipient_candidate(ln, postal, pdf_name):
            return ln
    return ""


def _format_zip_match(match: re.Match[str]) -> str:
    return match.group(1) + (f"-{match.group(2)}" if match.group(2) else "")


def _pick_zip(block: list[str], all_text: str) -> str:
    for ln in block:
        m = ZIP_RE.search(ln)
        if m:
            return _format_zip_match(m)

    all_zips = list(ZIP_RE.finditer(all_text))
    if all_zips:
        return _format_zip_match(all_zips[-1])
    return ""


def extract_label_signals(pdf_path: Path) -> dict[str, Any]:
    text, words_text = _extract_text(pdf_path)
    search_text = _normalize_space(f"{text} {words_text}")
    lower = search_text.lower()

    lines = _candidate_lines(text, words_text)
    shipto_block = _extract_shipto_block(lines)
    positional_shipto_block = _extract_positioned_shipto_block(pdf_path)
    postal = _pick_zip(shipto_block, search_text)
    if not postal and positional_shipto_block:
        postal = _pick_zip(positional_shipto_block, " ".join(positional_shipto_block))
    recipient = _pick_recipient(shipto_block, lines, postal, pdf_path.name, text)
    if (not recipient) and positional_shipto_block:
        positional_text = "\n".join(positional_shipto_block)
        recipient = _pick_recipient(positional_shipto_block, positional_shipto_block + lines, postal, pdf_path.name, positional_text)
    tracking = _extract_tracking(search_text)
    carrier = _detect_carrier(search_text, tracking)

    # eBay UPS labels often have no embedded text; use a tight OCR crop around
    # the recipient panel only when the normal text pass did not produce usable
    # recipient or ZIP signals.
    if _looks_like_ebay_label_pdf(pdf_path) and (not postal or not recipient):
        ocr_lines = _ocr_ebay_ups_recipient_block(pdf_path)
        if ocr_lines:
            ocr_text = "\n".join(ocr_lines)
            text = "\n".join(part for part in [text, ocr_text] if part.strip())
            search_text = _normalize_space(f"{search_text} {ocr_text}")
            lower = search_text.lower()
            lines = _candidate_lines(text, words_text)
            shipto_block = _extract_shipto_block(lines)
            postal = postal or _pick_zip(shipto_block, search_text)
            recipient = recipient or _pick_recipient(shipto_block, lines, postal, pdf_path.name, text)
            tracking = tracking or _extract_tracking(search_text)
            carrier = _detect_carrier(search_text, tracking)

    amz_match = AMZ_ORDER_RE.search(search_text)
    ebay_match = EBAY_ORDER_RE.search(search_text)

    platform_hint = "unknown"
    if "amazon" in lower or "amzn" in lower:
        platform_hint = "amazon"
    elif "ebay" in lower or "ebay international shipping" in lower or _looks_like_ebay_label_pdf(pdf_path):
        platform_hint = "ebay"

    return {
        "text": search_text,
        "raw_text": text,
        "platform_hint": platform_hint,
        "ship_postal": postal,
        "tracking_number": tracking,
        "carrier": carrier,
        "order_id_amazon": amz_match.group(0) if amz_match else "",
        "order_id_ebay": ebay_match.group(0) if ebay_match else "",
        "recipient_name": recipient,
    }

