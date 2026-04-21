from __future__ import annotations

import json
import math
import re
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytesseract
from PIL import Image, ImageFilter, ImageOps
from pytesseract import Output, TesseractError

from app.core.config import Settings
from app.core.exceptions import OCRExecutionError, PdfRenderError
from app.core.logger import get_logger
from app.pipeline.base import OCRPipeline

logger = get_logger(__name__)


# ============================================================================
# Modelos OCR
# ============================================================================

@dataclass(slots=True)
class OCRWord:
    text: str
    left: int
    top: int
    width: int
    height: int
    conf: float
    block_num: int
    par_num: int
    line_num: int

    @property
    def right(self) -> int:
        return self.left + self.width

    @property
    def bottom(self) -> int:
        return self.top + self.height

    @property
    def center_x(self) -> float:
        return self.left + (self.width / 2)

    @property
    def center_y(self) -> float:
        return self.top + (self.height / 2)


@dataclass(slots=True)
class OCRLine:
    words: list[OCRWord] = field(default_factory=list)

    @property
    def left(self) -> int:
        return min(w.left for w in self.words)

    @property
    def top(self) -> int:
        return min(w.top for w in self.words)

    @property
    def right(self) -> int:
        return max(w.right for w in self.words)

    @property
    def bottom(self) -> int:
        return max(w.bottom for w in self.words)

    @property
    def width(self) -> int:
        return self.right - self.left

    @property
    def height(self) -> int:
        return self.bottom - self.top

    @property
    def center_x(self) -> float:
        return (self.left + self.right) / 2

    @property
    def center_y(self) -> float:
        return (self.top + self.bottom) / 2

    @property
    def text(self) -> str:
        ordered = sorted(self.words, key=lambda w: w.left)
        return " ".join(w.text for w in ordered if w.text).strip()


@dataclass(slots=True)
class OCRCell:
    text: str
    left: int
    right: int
    top: int
    bottom: int

    @property
    def width(self) -> int:
        return self.right - self.left

    @property
    def height(self) -> int:
        return self.bottom - self.top

    @property
    def center_x(self) -> float:
        return (self.left + self.right) / 2

    @property
    def center_y(self) -> float:
        return (self.top + self.bottom) / 2


@dataclass(slots=True)
class OCRRow:
    lines: list[OCRLine] = field(default_factory=list)

    @property
    def left(self) -> int:
        return min(line.left for line in self.lines)

    @property
    def top(self) -> int:
        return min(line.top for line in self.lines)

    @property
    def right(self) -> int:
        return max(line.right for line in self.lines)

    @property
    def bottom(self) -> int:
        return max(line.bottom for line in self.lines)

    @property
    def width(self) -> int:
        return self.right - self.left

    @property
    def height(self) -> int:
        return self.bottom - self.top

    @property
    def center_x(self) -> float:
        return (self.left + self.right) / 2

    @property
    def center_y(self) -> float:
        return (self.top + self.bottom) / 2

    @property
    def text(self) -> str:
        return " ".join(line.text for line in self.lines if line.text).strip()


@dataclass(slots=True)
class OCRBlock:
    rows: list[OCRRow] = field(default_factory=list)

    @property
    def left(self) -> int:
        return min(row.left for row in self.rows)

    @property
    def top(self) -> int:
        return min(row.top for row in self.rows)

    @property
    def right(self) -> int:
        return max(row.right for row in self.rows)

    @property
    def bottom(self) -> int:
        return max(row.bottom for row in self.rows)

    @property
    def width(self) -> int:
        return self.right - self.left

    @property
    def height(self) -> int:
        return self.bottom - self.top

    @property
    def center_x(self) -> float:
        return (self.left + self.right) / 2

    @property
    def center_y(self) -> float:
        return (self.top + self.bottom) / 2


# ============================================================================
# Pipeline
# ============================================================================

class TesseractOCRPipeline(OCRPipeline):
    def __init__(self, settings: Settings):
        self.settings = settings

    # ---------------------------------------------------------------------
    # utilidades base
    # ---------------------------------------------------------------------

    def _build_tesseract_config(self) -> str:
        return f"--oem {self.settings.tesseract_oem} --psm {self.settings.tesseract_psm}"

    def _normalize_text(self, text: str) -> str:
        if not text:
            return ""

        text = text.replace("\r\n", "\n").replace("\r", "\n")
        lines = [" ".join(line.split()) for line in text.split("\n")]

        cleaned: list[str] = []
        blank_streak = 0

        for line in lines:
            if line:
                cleaned.append(line)
                blank_streak = 0
            else:
                blank_streak += 1
                if blank_streak <= 1:
                    cleaned.append("")

        result = "\n".join(cleaned).strip()
        return re.sub(r"\n{3,}", "\n\n", result)

    def _normalize_ocr_artifacts(self, text: str) -> str:
        text = self._normalize_text(text)
        if not text:
            return ""

        text = text.replace("￾", "-")
        text = text.replace("ﬁ", "fi").replace("ﬂ", "fl")
        text = text.replace("—", "-").replace("–", "-")
        text = text.replace("MemberID", "Member ID")
        text = text.replace("Co- Insurance", "Co-Insurance")
        text = text.replace("c o- nsurance", "Co-Insurance")
        text = text.replace("C overage", "Coverage")

        while True:
            old = text
            if len(text) >= 2 and text[0] in {"•", "*", "+", "-", "¢", "«", "»"} and text[1] == " ":
                text = text[2:].strip()
            elif len(text) >= 2 and text[0] in {"e", "E"} and text[1] == " ":
                text = text[2:].strip()
            if text == old:
                break

        return " ".join(text.split())

    def _sanitize_md_cell(self, text: str) -> str:
        return self._normalize_ocr_artifacts(text).replace("|", "\\|")

    def _safe_float(self, value: Any, default: float = -1.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def _median(self, values: list[float | int]) -> float:
        if not values:
            return 0.0
        values = sorted(float(v) for v in values)
        n = len(values)
        mid = n // 2
        if n % 2:
            return values[mid]
        return (values[mid - 1] + values[mid]) / 2.0

    def _mean(self, values: list[float | int]) -> float:
        if not values:
            return 0.0
        return sum(float(v) for v in values) / len(values)

    def _stdev(self, values: list[float | int]) -> float:
        if len(values) < 2:
            return 0.0
        mean = self._mean(values)
        return math.sqrt(sum((float(v) - mean) ** 2 for v in values) / len(values))

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    def _is_numeric_like(self, text: str) -> bool:
        text = self._normalize_ocr_artifacts(text)
        if not text:
            return False
        digits = sum(ch.isdigit() for ch in text)
        alpha = sum(ch.isalpha() for ch in text)
        return digits > 0 and digits >= alpha

    def _is_noise_line(self, text: str) -> bool:
        text = self._normalize_ocr_artifacts(text)
        if not text:
            return False

        if text.startswith("Printed "):
            return True

        if re.fullmatch(r"\d+/\d+", text):
            return True

        has_slash = "/" in text
        has_colon = ":" in text
        has_ampm = "AM" in text.upper() or "PM" in text.upper()
        if has_slash and has_colon and has_ampm:
            return True

        return False

    # ---------------------------------------------------------------------
    # preproceso
    # ---------------------------------------------------------------------

    def _preprocess_image(self, image: Image.Image) -> Image.Image:
        img = image.convert("L")
        img = ImageOps.autocontrast(img)

        min_width = 1800
        if img.width < min_width:
            ratio = min_width / img.width
            img = img.resize(
                (int(img.width * ratio), int(img.height * ratio)),
                Image.Resampling.LANCZOS,
            )

        img = img.filter(ImageFilter.SHARPEN)
        img = img.point(lambda p: 255 if p > 180 else 0)

        return img.convert("RGB")

    # ---------------------------------------------------------------------
    # PDF -> imágenes
    # ---------------------------------------------------------------------

    def _render_pdf_to_images(self, pdf_path: Path, job_id: str) -> list[Path]:
        render_root = pdf_path.parent / f"rendered_pages_{job_id}"
        render_root.mkdir(parents=True, exist_ok=True)

        output_prefix = render_root / "page"
        start = time.monotonic()

        cmd = [
            "pdftoppm",
            "-png",
            "-r",
            str(self.settings.pdf_render_dpi),
            str(pdf_path),
            str(output_prefix),
        ]

        try:
            subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=self.settings.pdftoppm_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise PdfRenderError("pdftoppm timed out while rendering PDF") from exc
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            raise PdfRenderError(f"pdftoppm failed: {stderr or exc}") from exc
        except FileNotFoundError as exc:
            raise PdfRenderError("pdftoppm is not installed in the worker image") from exc

        page_images = sorted(
            render_root.glob("page-*.png"),
            key=lambda p: int(p.stem.split("-")[-1]),
        )

        if not page_images:
            raise PdfRenderError("No pages were generated from the PDF")

        elapsed = round(time.monotonic() - start, 3)
        logger.info(
            f"stage=render_pdf file={pdf_path.name} pages={len(page_images)} elapsed_seconds={elapsed}",
            extra={"job_id": job_id},
        )

        return page_images

    # ---------------------------------------------------------------------
    # OCR base
    # ---------------------------------------------------------------------

    def _ocr_image_to_string(self, image: Image.Image, lang: str | None = None) -> str:
        try:
            text = pytesseract.image_to_string(
                image,
                lang=lang or self.settings.ocr_lang,
                config=self._build_tesseract_config(),
                timeout=self.settings.tesseract_timeout_seconds,
            )
            return self._normalize_text(text)
        except TesseractError as exc:
            raise OCRExecutionError(f"Tesseract failed: {exc}") from exc
        except RuntimeError as exc:
            raise OCRExecutionError(f"Tesseract timed out: {exc}") from exc
        except Exception as exc:
            raise OCRExecutionError(f"OCR image processing failed: {exc}") from exc
        

    def _ocr_image_data(self, image: Image.Image, lang: str | None = None) -> dict[str, list[Any]]:
        try:
            return pytesseract.image_to_data(
                image,
                lang=lang or self.settings.ocr_lang,
                config=self._build_tesseract_config(),
                output_type=Output.DICT,
                timeout=self.settings.tesseract_timeout_seconds,
            )
        except TesseractError as exc:
            raise OCRExecutionError(f"Tesseract data extraction failed: {exc}") from exc
        except RuntimeError as exc:
            raise OCRExecutionError(f"Tesseract data extraction timed out: {exc}") from exc
        except Exception as exc:
            raise OCRExecutionError(f"OCR structured extraction failed: {exc}") from exc
        
    # ---------------------------------------------------------------------
    # extracción OCR
    # ---------------------------------------------------------------------

    def _extract_words(self, data: dict[str, list[Any]]) -> list[OCRWord]:
        total = len(data.get("text", []))
        words: list[OCRWord] = []

        for i in range(total):
            raw_text = self._normalize_ocr_artifacts(str(data["text"][i] or ""))
            if not raw_text:
                continue

            conf = self._safe_float(data["conf"][i], -1.0)
            if conf < 0:
                continue

            words.append(
                OCRWord(
                    text=raw_text,
                    left=int(data["left"][i]),
                    top=int(data["top"][i]),
                    width=int(data["width"][i]),
                    height=int(data["height"][i]),
                    conf=conf,
                    block_num=int(data["block_num"][i]),
                    par_num=int(data["par_num"][i]),
                    line_num=int(data["line_num"][i]),
                )
            )

        return words

    def _group_lines(self, words: list[OCRWord]) -> list[OCRLine]:
        line_map: dict[tuple[int, int, int], list[OCRWord]] = {}

        for word in words:
            key = (word.block_num, word.par_num, word.line_num)
            line_map.setdefault(key, []).append(word)

        lines: list[OCRLine] = []
        for line_words in line_map.values():
            line_words.sort(key=lambda w: w.left)
            lines.append(OCRLine(words=line_words))

        lines.sort(key=lambda l: (l.top, l.left))
        return lines

    def _merge_lines_into_rows(self, lines: list[OCRLine]) -> list[OCRRow]:
        if not lines:
            return []

        rows: list[OCRRow] = []
        median_height = max(1.0, self._median([line.height for line in lines]))
        y_tolerance = max(8.0, median_height * 0.45)

        for line in sorted(lines, key=lambda l: (l.center_y, l.left)):
            placed = False

            for row in rows:
                vertical_close = abs(line.center_y - row.center_y) <= y_tolerance
                overlaps_y = min(line.bottom, row.bottom) - max(line.top, row.top) >= -2

                if vertical_close or overlaps_y:
                    row.lines.append(line)
                    placed = True
                    break

            if not placed:
                rows.append(OCRRow(lines=[line]))

        for row in rows:
            row.lines.sort(key=lambda l: l.left)

        rows.sort(key=lambda r: (r.top, r.left))
        return rows

    def _group_rows_into_blocks(self, rows: list[OCRRow], page_width: int) -> list[OCRBlock]:
        if not rows:
            return []

        blocks: list[OCRBlock] = []
        median_height = max(1.0, self._median([row.height for row in rows]))
        vertical_gap_tolerance = max(14.0, median_height * 1.35)
        horizontal_tolerance = max(35.0, page_width * 0.08)

        for row in rows:
            placed = False

            for block in blocks:
                last_row = block.rows[-1]
                vertical_gap = row.top - last_row.bottom
                horizontal_overlap = min(row.right, block.right) - max(row.left, block.left)
                horizontally_related = horizontal_overlap >= -horizontal_tolerance

                if vertical_gap <= vertical_gap_tolerance and horizontally_related:
                    block.rows.append(row)
                    placed = True
                    break

            if not placed:
                blocks.append(OCRBlock(rows=[row]))

        for block in blocks:
            block.rows.sort(key=lambda r: (r.top, r.left))

        blocks.sort(key=lambda b: (b.top, b.left))
        return blocks

    def _order_blocks(self, blocks: list[OCRBlock]) -> list[OCRBlock]:
        return sorted(blocks, key=lambda b: (b.top, b.left))

    # ---------------------------------------------------------------------
    # estadísticas geométricas
    # ---------------------------------------------------------------------

    def _build_page_stats(self, rows: list[OCRRow], page_width: int) -> dict[str, float]:
        heights = [row.height for row in rows if row.height > 0]
        widths = [row.width for row in rows if row.width > 0]
        gaps = [max(0, curr.top - prev.bottom) for prev, curr in zip(rows, rows[1:])]

        return {
            "page_width": float(page_width),
            "median_row_height": max(1.0, self._median(heights)),
            "median_row_width": max(1.0, self._median(widths)),
            "median_row_gap": max(0.0, self._median(gaps) if gaps else 0.0),
        }

    # ---------------------------------------------------------------------
    # row -> cells
    # ---------------------------------------------------------------------

    def _row_to_cells(self, row: OCRRow) -> list[OCRCell]:
        words: list[OCRWord] = []
        for line in row.lines:
            words.extend(line.words)

        words.sort(key=lambda w: w.left)
        if not words:
            return []

        median_height = max(1.0, self._median([w.height for w in words]))
        gaps = [max(0, curr.left - prev.right) for prev, curr in zip(words, words[1:])]
        median_gap = self._median(gaps) if gaps else 0.0
        stdev_gap = self._stdev(gaps) if gaps else 0.0

        gap_threshold = max(
            18.0,
            median_height * 1.2,
            median_gap + (stdev_gap * 0.7),
        )

        groups: list[list[OCRWord]] = []
        current = [words[0]]

        for prev, curr in zip(words, words[1:]):
            gap = curr.left - prev.right
            if gap > gap_threshold:
                groups.append(current)
                current = [curr]
            else:
                current.append(curr)

        if current:
            groups.append(current)

        cells: list[OCRCell] = []
        for group in groups:
            cells.append(
                OCRCell(
                    text=" ".join(w.text for w in group).strip(),
                    left=min(w.left for w in group),
                    right=max(w.right for w in group),
                    top=min(w.top for w in group),
                    bottom=max(w.bottom for w in group),
                )
            )

        return cells

    # ---------------------------------------------------------------------
    # subsegmentación estructural
    # ---------------------------------------------------------------------

    def _row_signature(
        self,
        row: OCRRow,
        cells: list[OCRCell],
        page_stats: dict[str, float],
        page_width: int,
    ) -> dict[str, float]:
        cell_count = len(cells)
        width_ratio = row.width / max(1.0, page_width)
        height_ratio = row.height / max(1.0, page_stats["median_row_height"])
        numeric_cells = sum(1 for cell in cells if self._is_numeric_like(cell.text))
        numeric_ratio = numeric_cells / max(1, cell_count)

        return {
            "cell_count": float(cell_count),
            "width_ratio": width_ratio,
            "height_ratio": height_ratio,
            "numeric_ratio": numeric_ratio,
        }

    def _row_signature_distance(self, a: dict[str, float], b: dict[str, float]) -> float:
        return (
            abs(a["cell_count"] - b["cell_count"]) * 0.9
            + abs(a["width_ratio"] - b["width_ratio"]) * 1.5
            + abs(a["height_ratio"] - b["height_ratio"]) * 1.2
            + abs(a["numeric_ratio"] - b["numeric_ratio"]) * 0.8
        )

    def _split_block_by_structure(
        self,
        block: OCRBlock,
        page_stats: dict[str, float],
        page_width: int,
    ) -> list[OCRBlock]:
        if len(block.rows) <= 2:
            return [block]

        rows = block.rows
        rows_cells = [self._row_to_cells(row) for row in rows]
        signatures = [
            self._row_signature(row, cells, page_stats, page_width)
            for row, cells in zip(rows, rows_cells)
        ]

        median_gap = max(1.0, page_stats["median_row_gap"] + 1.0)
        groups: list[list[OCRRow]] = [[rows[0]]]

        for i in range(1, len(rows)):
            prev_row = rows[i - 1]
            curr_row = rows[i]

            gap = max(0, curr_row.top - prev_row.bottom)
            gap_ratio = gap / median_gap

            prev_sig = signatures[i - 1]
            curr_sig = signatures[i]
            sig_dist = self._row_signature_distance(prev_sig, curr_sig)
            cell_jump = abs(prev_sig["cell_count"] - curr_sig["cell_count"])

            split_here = False

            if gap_ratio >= 2.0:
                split_here = True
            elif sig_dist >= 1.7 and cell_jump >= 2:
                split_here = True
            elif prev_sig["cell_count"] >= 3 and curr_sig["cell_count"] <= 1 and gap_ratio >= 1.2:
                split_here = True
            elif prev_sig["cell_count"] <= 1 and curr_sig["cell_count"] >= 3 and gap_ratio >= 1.2:
                split_here = True
            elif prev_sig["height_ratio"] >= 1.4 and curr_sig["height_ratio"] <= 1.0 and gap_ratio >= 1.0:
                split_here = True

            if split_here:
                groups.append([curr_row])
            else:
                groups[-1].append(curr_row)

        return [OCRBlock(rows=g) for g in groups if g]

    # ---------------------------------------------------------------------
    # títulos geométricos
    # ---------------------------------------------------------------------

    def _heading_score(
        self,
        row: OCRRow,
        idx: int,
        block: OCRBlock,
        page_stats: dict[str, float],
        page_width: int,
    ) -> float:
        prev_row = block.rows[idx - 1] if idx > 0 else None
        next_row = block.rows[idx + 1] if idx + 1 < len(block.rows) else None

        cells = self._row_to_cells(row)
        cell_count = len(cells)

        height_ratio = row.height / max(1.0, page_stats["median_row_height"])
        width_ratio = row.width / max(1.0, page_width)
        centeredness = 1.0 - min(1.0, abs(row.center_x - (page_width / 2)) / (page_width / 2))

        gap_up = max(0, row.top - prev_row.bottom) if prev_row else page_stats["median_row_gap"] * 1.5
        gap_down = max(0, next_row.top - row.bottom) if next_row else page_stats["median_row_gap"] * 1.5
        gap_up_ratio = gap_up / max(1.0, page_stats["median_row_gap"] + 1.0)
        gap_down_ratio = gap_down / max(1.0, page_stats["median_row_gap"] + 1.0)

        score = 0.0

        if height_ratio >= 1.12:
            score += min(0.30, (height_ratio - 1.0) * 0.45)
        if width_ratio <= 0.72:
            score += 0.12
        if gap_up_ratio >= 1.2:
            score += min(0.16, (gap_up_ratio - 1.0) * 0.12)
        if gap_down_ratio >= 1.2:
            score += min(0.16, (gap_down_ratio - 1.0) * 0.12)

        score += centeredness * 0.08

        if cell_count <= 2:
            score += 0.08
        elif cell_count >= 4:
            score -= 0.22

        if width_ratio >= 0.90 and height_ratio <= 1.05:
            score -= 0.14

        return self._clamp(score, 0.0, 1.0)

    def _extract_heading_rows(
        self,
        block: OCRBlock,
        page_stats: dict[str, float],
        page_width: int,
    ) -> tuple[list[tuple[int, str, int]], list[OCRRow]]:
        heading_rows: list[tuple[int, str, int]] = []
        normal_rows: list[OCRRow] = []

        for idx, row in enumerate(block.rows):
            score = self._heading_score(row, idx, block, page_stats, page_width)
            text = self._normalize_ocr_artifacts(row.text)

            if score >= 0.63 and text:
                height_ratio = row.height / max(1.0, page_stats["median_row_height"])
                level = 2 if height_ratio >= 1.45 else 3
                heading_rows.append((idx, text, level))
            else:
                normal_rows.append(row)

        return heading_rows, normal_rows

    # ---------------------------------------------------------------------
    # detección de regiones tabulares
    # ---------------------------------------------------------------------

    def _table_window_score(
        self,
        rows_ref: list[OCRRow],
        rows_cells: list[list[OCRCell]],
        block_width: int,
    ) -> float:
        if len(rows_cells) < 3:
            return 0.0

        row_lengths = [len(row) for row in rows_cells if row]
        if not row_lengths:
            return 0.0

        ratio_2plus = sum(1 for n in row_lengths if n >= 2) / len(row_lengths)

        centers: list[float] = []
        for row in rows_cells:
            for cell in row:
                centers.append(cell.center_x)

        if len(centers) < 4:
            return 0.0

        tolerance = max(18.0, block_width * 0.035)
        anchors = self._merge_nearby_anchors(centers, tolerance)
        anchor_count = len(anchors)

        row_width_ratios = []
        for row in rows_ref:
            row_width_ratios.append(row.width / max(1.0, block_width))

        width_consistency = 1.0 - self._clamp(self._stdev(row_width_ratios) / 0.22, 0.0, 1.0)

        score = (
            0.45 * ratio_2plus
            + 0.30 * self._clamp(anchor_count / 6.0, 0.0, 1.0)
            + 0.25 * width_consistency
        )
        return self._clamp(score, 0.0, 1.0)

    def _detect_table_regions_in_block(
        self,
        block: OCRBlock,
        page_stats: dict[str, float],
        page_width: int,
    ) -> list[dict[str, Any]]:
        rows = block.rows
        if not rows:
            return []

        rows_cells = [self._row_to_cells(row) for row in rows]
        n = len(rows)

        if n <= 2:
            return [{"kind": "text_region", "rows": rows}]

        window_size = min(5, n)
        flags = [False] * n

        for start in range(0, n - window_size + 1):
            window_rows = rows[start:start + window_size]
            window_cells = rows_cells[start:start + window_size]
            score = self._table_window_score(window_rows, window_cells, block.width)
            if score >= 0.52:
                for i in range(start, start + window_size):
                    flags[i] = True

        # relleno simple de huecos de 1 fila entre zonas tabulares
        for i in range(1, n - 1):
            if not flags[i] and flags[i - 1] and flags[i + 1]:
                flags[i] = True

        regions: list[dict[str, Any]] = []
        current_kind = "table_region" if flags[0] else "text_region"
        current_rows = [rows[0]]

        for i in range(1, n):
            kind = "table_region" if flags[i] else "text_region"
            if kind == current_kind:
                current_rows.append(rows[i])
            else:
                regions.append({"kind": current_kind, "rows": current_rows})
                current_kind = kind
                current_rows = [rows[i]]

        if current_rows:
            regions.append({"kind": current_kind, "rows": current_rows})

        return regions

    # ---------------------------------------------------------------------
    # bandas tabulares
    # ---------------------------------------------------------------------

    def _row_occupancy_pattern(
        self,
        row_cells: list[OCRCell],
        anchors: list[float],
    ) -> tuple[set[int], list[str]]:
        values, occupied, _ = self._assign_cells_to_columns(row_cells, anchors)
        return occupied, values

    def _split_table_region_into_bands(
        self,
        rows_ref: list[OCRRow],
        rows_cells: list[list[OCRCell]],
        page_stats: dict[str, float],
        block_width: int,
    ) -> list[dict[str, Any]]:
        if len(rows_ref) <= 2:
            return [{"rows_ref": rows_ref, "rows_cells": rows_cells}]

        anchors = self._cluster_column_anchors(rows_cells, block_width)
        if len(anchors) < 2:
            return [{"rows_ref": rows_ref, "rows_cells": rows_cells}]

        median_gap = max(1.0, page_stats["median_row_gap"] + 1.0)

        bands: list[dict[str, Any]] = []
        current_rows_ref = [rows_ref[0]]
        current_rows_cells = [rows_cells[0]]

        prev_occupied, _ = self._row_occupancy_pattern(rows_cells[0], anchors)

        for i in range(1, len(rows_ref)):
            curr_row = rows_ref[i]
            prev_row = rows_ref[i - 1]
            curr_cells = rows_cells[i]
            curr_occupied, _ = self._row_occupancy_pattern(curr_cells, anchors)

            gap = max(0, curr_row.top - prev_row.bottom)
            gap_ratio = gap / median_gap

            inter = prev_occupied & curr_occupied
            union = prev_occupied | curr_occupied
            overlap_ratio = len(inter) / max(1, len(union))

            split_here = False
            if gap_ratio >= 1.8:
                split_here = True
            elif len(prev_occupied) >= 2 and len(curr_occupied) >= 2 and overlap_ratio <= 0.15:
                split_here = True
            elif abs(len(prev_occupied) - len(curr_occupied)) >= 3:
                split_here = True

            if split_here:
                bands.append({
                    "rows_ref": current_rows_ref,
                    "rows_cells": current_rows_cells,
                })
                current_rows_ref = [curr_row]
                current_rows_cells = [curr_cells]
            else:
                current_rows_ref.append(curr_row)
                current_rows_cells.append(curr_cells)

            prev_occupied = curr_occupied

        if current_rows_ref:
            bands.append({
                "rows_ref": current_rows_ref,
                "rows_cells": current_rows_cells,
            })

        return bands

    # ---------------------------------------------------------------------
    # reconstrucción tabular
    # ---------------------------------------------------------------------

    def _merge_nearby_anchors(self, anchors: list[float], tolerance: float) -> list[float]:
        if not anchors:
            return []

        anchors = sorted(anchors)
        groups: list[list[float]] = [[anchors[0]]]

        for value in anchors[1:]:
            avg = sum(groups[-1]) / len(groups[-1])
            if abs(value - avg) <= tolerance:
                groups[-1].append(value)
            else:
                groups.append([value])

        return [sum(group) / len(group) for group in groups]

    def _build_x_coverage_profile(
        self,
        rows_cells: list[list[OCRCell]],
        block_left: int,
        block_right: int,
    ) -> list[float]:
        width = max(1, block_right - block_left + 1)
        profile = [0.0] * width

        for row in rows_cells:
            for cell in row:
                start = max(0, cell.left - block_left)
                end = min(width - 1, cell.right - block_left)
                for x in range(start, end + 1):
                    profile[x] += 1.0

        return profile

    def _extract_column_zones_from_profile(
        self,
        profile: list[float],
        block_left: int,
    ) -> list[tuple[int, int]]:
        if not profile:
            return []

        max_v = max(profile)
        if max_v <= 0:
            return []

        threshold = max_v * 0.22

        zones: list[tuple[int, int]] = []
        in_zone = False
        start = 0

        for i, v in enumerate(profile):
            if v >= threshold and not in_zone:
                start = i
                in_zone = True
            elif v < threshold and in_zone:
                zones.append((block_left + start, block_left + i - 1))
                in_zone = False

        if in_zone:
            zones.append((block_left + start, block_left + len(profile) - 1))

        return zones

    def _cluster_column_anchors(self, rows_cells: list[list[OCRCell]], block_width: int) -> list[float]:
        centers: list[float] = []
        lefts: list[float] = []
        rights: list[float] = []

        for row in rows_cells:
            for cell in row:
                centers.append(cell.center_x)
                lefts.append(cell.left)
                rights.append(cell.right)

        tolerance = max(18.0, block_width * 0.035)

        merged_centers = self._merge_nearby_anchors(centers, tolerance)
        merged_lefts = self._merge_nearby_anchors(lefts, tolerance)
        merged_rights = self._merge_nearby_anchors(rights, tolerance)

        candidates = [merged_centers, merged_lefts, merged_rights]
        candidates.sort(key=len, reverse=True)

        return sorted(candidates[0])

    def _refine_anchors_with_profile(
        self,
        rows_cells: list[list[OCRCell]],
        block_left: int,
        block_right: int,
        block_width: int,
    ) -> list[float]:
        profile = self._build_x_coverage_profile(rows_cells, block_left, block_right)
        zones = self._extract_column_zones_from_profile(profile, block_left)

        zone_centers = [((a + b) / 2.0) for a, b in zones]
        clustered = self._cluster_column_anchors(rows_cells, block_width)

        if len(zone_centers) >= len(clustered) and len(zone_centers) >= 2:
            return zone_centers
        return clustered

    def _assign_cells_to_columns(
        self,
        row_cells: list[OCRCell],
        anchors: list[float],
    ) -> tuple[list[str], set[int], list[float]]:
        if not anchors:
            return [], set(), []

        values = [""] * len(anchors)
        occupied: set[int] = set()
        distances: list[float] = []

        for cell in row_cells:
            best_idx = 0
            best_dist = float("inf")

            for idx, anchor in enumerate(anchors):
                dist = abs(cell.center_x - anchor)
                if dist < best_dist:
                    best_idx = idx
                    best_dist = dist

            distances.append(best_dist)
            occupied.add(best_idx)

            current = self._sanitize_md_cell(cell.text)
            existing = values[best_idx].strip()
            values[best_idx] = f"{existing} {current}".strip() if existing else current

        return values, occupied, distances

    def _collapse_empty_columns(self, matrix: list[list[str]]) -> list[list[str]]:
        if not matrix:
            return []

        width = max(len(row) for row in matrix)
        padded = [row + [""] * (width - len(row)) for row in matrix]

        keep_indices: list[int] = []
        for col_idx in range(width):
            if any(padded[r][col_idx].strip() for r in range(len(padded))):
                keep_indices.append(col_idx)

        if not keep_indices:
            return []

        return [[row[idx] for idx in keep_indices] for row in padded]

    def _should_merge_fragment_rows(
        self,
        upper_values: list[str],
        lower_values: list[str],
        upper_occupied: set[int],
        lower_occupied: set[int],
        upper_row: OCRRow,
        lower_row: OCRRow,
        page_stats: dict[str, float],
    ) -> bool:
        if not upper_occupied or not lower_occupied:
            return False

        gap = max(0, lower_row.top - upper_row.bottom)
        gap_ratio = gap / max(1.0, page_stats["median_row_gap"] + 1.0)
        if gap_ratio > 1.15:
            return False

        inter = upper_occupied & lower_occupied
        union = upper_occupied | lower_occupied
        overlap_ratio = len(inter) / max(1, len(union))

        if len(inter) == 0:
            return True

        if overlap_ratio <= 0.25 and (len(upper_occupied) <= 2 or len(lower_occupied) <= 2):
            return True

        upper_non_empty = sum(1 for v in upper_values if v.strip())
        lower_non_empty = sum(1 for v in lower_values if v.strip())
        if overlap_ratio <= 0.35 and (upper_non_empty <= 2 or lower_non_empty <= 2):
            return True

        return False

    def _reconstruct_band_matrix(
        self,
        rows_ref: list[OCRRow],
        rows_cells: list[list[OCRCell]],
        page_stats: dict[str, float],
        block_width: int,
        block_left: int,
        block_right: int,
    ) -> tuple[list[list[str]], dict[str, float]]:
        anchors = self._refine_anchors_with_profile(
            rows_cells=rows_cells,
            block_left=block_left,
            block_right=block_right,
            block_width=block_width,
        )

        if len(anchors) < 2:
            return [], {"anchor_count": 0.0, "mean_alignment_distance": 0.0}

        assigned_rows: list[tuple[list[str], set[int], list[float], OCRRow]] = []
        for row_cells, row_ref in zip(rows_cells, rows_ref):
            values, occupied, distances = self._assign_cells_to_columns(row_cells, anchors)
            assigned_rows.append((values, occupied, distances, row_ref))

        merged_rows: list[list[str]] = []
        all_distances: list[float] = []

        i = 0
        while i < len(assigned_rows):
            current_values, current_occupied, current_distances, current_row = assigned_rows[i]
            current_values = current_values[:]
            current_occupied = set(current_occupied)
            all_distances.extend(current_distances)

            while i + 1 < len(assigned_rows):
                next_values, next_occupied, next_distances, next_row = assigned_rows[i + 1]

                if not self._should_merge_fragment_rows(
                    current_values,
                    next_values,
                    current_occupied,
                    next_occupied,
                    current_row,
                    next_row,
                    page_stats,
                ):
                    break

                for col_idx, value in enumerate(next_values):
                    if value.strip():
                        if current_values[col_idx].strip():
                            current_values[col_idx] = f"{current_values[col_idx]} {value}".strip()
                        else:
                            current_values[col_idx] = value

                current_occupied |= next_occupied
                all_distances.extend(next_distances)
                current_row = next_row
                i += 1

            merged_rows.append(current_values)
            i += 1

        matrix = self._collapse_empty_columns(merged_rows)

        return matrix, {
            "anchor_count": float(len(anchors)),
            "mean_alignment_distance": self._mean(all_distances),
        }

    # ---------------------------------------------------------------------
    # clasificación estructural
    # ---------------------------------------------------------------------

    def _score_true_table(
        self,
        rows_cells: list[list[OCRCell]],
        rows_ref: list[OCRRow],
        block_width: int,
        page_stats: dict[str, float],
        block_left: int,
        block_right: int,
    ) -> tuple[float, dict[str, float], list[list[str]]]:
        if len(rows_cells) < 2:
            return 0.0, {}, []

        row_lengths = [len(row) for row in rows_cells if row]
        if not row_lengths:
            return 0.0, {}, []

        rows_with_2plus = sum(1 for n in row_lengths if n >= 2) / len(row_lengths)

        matrix, matrix_meta = self._reconstruct_band_matrix(
            rows_ref=rows_ref,
            rows_cells=rows_cells,
            page_stats=page_stats,
            block_width=block_width,
            block_left=block_left,
            block_right=block_right,
        )

        if not matrix or len(matrix) < 2:
            return 0.0, {}, []

        col_count = len(matrix[0])
        if col_count < 2:
            return 0.0, {}, []

        row_non_empty_counts = [sum(1 for cell in row if cell.strip()) for row in matrix]
        dominant = max(set(row_non_empty_counts), key=row_non_empty_counts.count)
        dominant_ratio = row_non_empty_counts.count(dominant) / len(row_non_empty_counts)

        col_fill_counts = [0] * col_count
        numeric_cells = 0
        filled = 0

        for row in matrix:
            for idx, cell in enumerate(row):
                if cell.strip():
                    filled += 1
                    col_fill_counts[idx] += 1
                    if self._is_numeric_like(cell):
                        numeric_cells += 1

        fill_ratio = filled / max(1, len(matrix) * col_count)
        column_reuse = sum(
            1 for count in col_fill_counts if count >= max(2, int(len(matrix) * 0.4))
        ) / max(1, col_count)
        numeric_ratio = numeric_cells / max(1, filled)

        tolerance = max(18.0, block_width * 0.04)
        align_quality = 1.0 - self._clamp(
            matrix_meta["mean_alignment_distance"] / max(1.0, tolerance * 1.8),
            0.0,
            1.0,
        )

        score = (
            0.24 * rows_with_2plus
            + 0.24 * dominant_ratio
            + 0.22 * column_reuse
            + 0.16 * fill_ratio
            + 0.14 * align_quality
        )
        score += min(0.08, numeric_ratio * 0.08)

        meta = {
            "ratio_2plus": rows_with_2plus,
            "dominant_ratio": dominant_ratio,
            "column_reuse": column_reuse,
            "fill_ratio": fill_ratio,
            "numeric_ratio": numeric_ratio,
            "align_quality": align_quality,
            "anchor_count": matrix_meta["anchor_count"],
        }

        return self._clamp(score, 0.0, 1.0), meta, matrix

    def _score_key_value(self, rows_cells: list[list[OCRCell]], block_width: int) -> tuple[float, dict[str, float]]:
        if len(rows_cells) < 2:
            return 0.0, {}

        candidate_rows = [row for row in rows_cells if len(row) >= 2]
        if len(candidate_rows) < 2:
            return 0.0, {}

        ratio_candidate = len(candidate_rows) / len(rows_cells)

        left_right_edges = [row[0].right for row in candidate_rows]
        second_left_edges = [row[1].left for row in candidate_rows]
        split_positions = [(row[0].right + row[1].left) / 2 for row in candidate_rows]

        left_var = self._stdev(left_right_edges)
        second_var = self._stdev(second_left_edges)
        split_var = self._stdev(split_positions)

        left_width_mean = self._mean([row[0].width / max(1.0, block_width) for row in candidate_rows])
        right_width_mean = self._mean([row[1].width / max(1.0, block_width) for row in candidate_rows])

        alignment_score = 1.0
        alignment_score -= self._clamp(left_var / max(20.0, block_width * 0.03), 0.0, 0.45)
        alignment_score -= self._clamp(second_var / max(24.0, block_width * 0.04), 0.0, 0.30)
        alignment_score -= self._clamp(split_var / max(28.0, block_width * 0.05), 0.0, 0.25)
        alignment_score = self._clamp(alignment_score, 0.0, 1.0)

        width_balance = 0.0
        if left_width_mean <= 0.42:
            width_balance += 0.55
        if right_width_mean >= 0.20:
            width_balance += 0.45
        width_balance = self._clamp(width_balance, 0.0, 1.0)

        score = (
            0.42 * ratio_candidate
            + 0.34 * alignment_score
            + 0.24 * width_balance
        )

        meta = {
            "ratio_candidate": ratio_candidate,
            "alignment_score": alignment_score,
            "width_balance": width_balance,
        }

        return self._clamp(score, 0.0, 1.0), meta

    def _score_plain_text(self, rows_cells: list[list[OCRCell]], block_width: int) -> tuple[float, dict[str, float]]:
        if not rows_cells:
            return 0.0, {}

        row_lengths = [len(row) for row in rows_cells]
        avg_cells = self._mean(row_lengths)

        width_ratios = []
        wide_rows = 0
        for row in rows_cells:
            if not row:
                continue
            left = min(cell.left for cell in row)
            right = max(cell.right for cell in row)
            ratio = (right - left) / max(1.0, block_width)
            width_ratios.append(ratio)
            if ratio >= 0.78:
                wide_rows += 1

        wide_ratio = wide_rows / max(1, len(rows_cells))
        width_consistency = 1.0 - self._clamp(self._stdev(width_ratios) / 80.0, 0.0, 1.0)

        score = (
            0.45 * wide_ratio
            + 0.35 * width_consistency
            + 0.20 * self._clamp(1.0 - abs(avg_cells - 1.3) / 2.0, 0.0, 1.0)
        )

        meta = {
            "wide_ratio": wide_ratio,
            "width_consistency": width_consistency,
            "avg_cells": avg_cells,
        }

        return self._clamp(score, 0.0, 1.0), meta

    def _classify_region(
        self,
        rows_cells: list[list[OCRCell]],
        rows_ref: list[OCRRow],
        block_width: int,
        page_stats: dict[str, float],
        block_left: int,
        block_right: int,
    ) -> tuple[str, dict[str, float], list[list[str]]]:
        table_score, table_meta, table_matrix = self._score_true_table(
            rows_cells=rows_cells,
            rows_ref=rows_ref,
            block_width=block_width,
            page_stats=page_stats,
            block_left=block_left,
            block_right=block_right,
        )
        kv_score, kv_meta = self._score_key_value(rows_cells, block_width)
        plain_score, plain_meta = self._score_plain_text(rows_cells, block_width)

        scores = {
            "true_table": table_score,
            "key_value_list": kv_score,
            "plain_text": plain_score,
        }

        best_type = max(scores, key=scores.get)
        best_score = scores[best_type]

        meta: dict[str, float] = {}
        for prefix, source in (
            ("table_", table_meta),
            ("kv_", kv_meta),
            ("plain_", plain_meta),
        ):
            for key, value in source.items():
                meta[prefix + key] = value
        meta["best_score"] = best_score

        if best_score < 0.42:
            return "plain_text", meta, table_matrix

        return best_type, meta, table_matrix

    # ---------------------------------------------------------------------
    # render
    # ---------------------------------------------------------------------

    def _render_key_value_list(self, rows_cells: list[list[OCRCell]]) -> str:
        lines: list[str] = []

        for row in rows_cells:
            cells = [self._normalize_ocr_artifacts(cell.text) for cell in row if self._normalize_ocr_artifacts(cell.text)]
            if not cells:
                continue

            if len(cells) == 1:
                lines.append(cells[0])
                continue

            left = cells[0]
            right = " ".join(cells[1:]).strip()

            if left and right:
                lines.append(f"- **{left}:** {right}")
            elif left:
                lines.append(f"- **{left}**")

        return "\n".join(lines).strip()

    def _normalize_markdown_matrix(self, matrix: list[list[str]]) -> list[list[str]]:
        if not matrix:
            return []

        width = max(len(row) for row in matrix)
        normalized: list[list[str]] = []

        for row in matrix:
            padded = row + [""] * (width - len(row))
            normalized.append([self._sanitize_md_cell(cell) for cell in padded])

        return self._collapse_empty_columns(normalized)

    def _choose_table_header(self, matrix: list[list[str]]) -> tuple[list[str], list[list[str]]]:
        if not matrix:
            return [], []

        if len(matrix) == 1:
            return [f"Columna {i+1}" for i in range(len(matrix[0]))], matrix

        first = matrix[0]
        second = matrix[1]

        first_non_empty = sum(1 for c in first if c.strip())
        second_non_empty = sum(1 for c in second if c.strip())

        first_numeric_ratio = sum(1 for c in first if self._is_numeric_like(c)) / max(1, len(first))
        second_numeric_ratio = sum(1 for c in second if self._is_numeric_like(c)) / max(1, len(second))

        looks_like_header = (
            first_non_empty >= 2
            and second_non_empty >= 2
            and first_numeric_ratio <= second_numeric_ratio
        )

        if looks_like_header:
            header = [c if c.strip() else f"Columna {i+1}" for i, c in enumerate(first)]
            return header, matrix[1:]

        return [f"Columna {i+1}" for i in range(len(first))], matrix

    def _render_true_table(self, rows_cells: list[list[OCRCell]], matrix: list[list[str]]) -> str:
        matrix = self._normalize_markdown_matrix(matrix)
        if not matrix:
            return self._render_plain_rows(rows_cells)

        if len(matrix[0]) < 2:
            return self._render_plain_rows(rows_cells)

        header, body = self._choose_table_header(matrix)

        lines = [
            "| " + " | ".join(header) + " |",
            "| " + " | ".join("---" for _ in header) + " |",
        ]

        for row in body:
            if not any(cell.strip() for cell in row):
                continue
            row_render = [cell if cell.strip() else " " for cell in row]
            lines.append("| " + " | ".join(row_render) + " |")

        return "\n".join(lines).strip()

    def _render_plain_rows(self, rows_cells: list[list[OCRCell]]) -> str:
        lines: list[str] = []

        for row in rows_cells:
            cells = [self._normalize_ocr_artifacts(cell.text) for cell in row if self._normalize_ocr_artifacts(cell.text)]
            if cells:
                lines.append(" ".join(cells).strip())

        return "\n".join(lines).strip()

    def _render_text_region(
        self,
        region_rows: list[OCRRow],
        page_stats: dict[str, float],
        page_width: int,
    ) -> tuple[str, dict[str, Any]]:
        subblock = OCRBlock(rows=region_rows)
        heading_rows, non_heading_rows = self._extract_heading_rows(subblock, page_stats, page_width)

        parts: list[str] = []
        for _, heading_text, level in heading_rows:
            parts.append(f"{'#' * level} {heading_text}")

        if not non_heading_rows:
            return "\n\n".join(parts).strip(), {
                "type": "heading_only",
                "bbox": [subblock.left, subblock.top, subblock.right, subblock.bottom],
                "scores": {},
                "row_count": len(subblock.rows),
            }

        rows_cells = [self._row_to_cells(row) for row in non_heading_rows]
        rows_cells = [row for row in rows_cells if row]

        if not rows_cells:
            return "\n\n".join(parts).strip(), {
                "type": "empty",
                "bbox": [subblock.left, subblock.top, subblock.right, subblock.bottom],
                "scores": {},
                "row_count": 0,
            }

        block_type, scores, table_matrix = self._classify_region(
            rows_cells=rows_cells,
            rows_ref=non_heading_rows,
            block_width=subblock.width,
            page_stats=page_stats,
            block_left=subblock.left,
            block_right=subblock.right,
        )

        if block_type == "true_table":
            rendered = self._render_true_table(rows_cells, table_matrix)
        elif block_type == "key_value_list":
            rendered = self._render_key_value_list(rows_cells)
        else:
            rendered = self._render_plain_rows(rows_cells)

        if rendered.strip():
            parts.append(rendered)

        return "\n\n".join(part for part in parts if part.strip()).strip(), {
            "type": block_type,
            "bbox": [subblock.left, subblock.top, subblock.right, subblock.bottom],
            "scores": scores,
            "row_count": len(rows_cells),
            "heading_count": len(heading_rows),
        }

    def _render_table_region(
        self,
        region_rows: list[OCRRow],
        page_stats: dict[str, float],
        page_width: int,
    ) -> tuple[str, list[dict[str, Any]]]:
        region_cells = [self._row_to_cells(row) for row in region_rows]
        bands = self._split_table_region_into_bands(
            rows_ref=region_rows,
            rows_cells=region_cells,
            page_stats=page_stats,
            block_width=max(1, max(r.right for r in region_rows) - min(r.left for r in region_rows)),
        )

        parts: list[str] = []
        metas: list[dict[str, Any]] = []

        for band in bands:
            rows_ref = band["rows_ref"]
            rows_cells = band["rows_cells"]
            band_block = OCRBlock(rows=rows_ref)

            block_type, scores, table_matrix = self._classify_region(
                rows_cells=rows_cells,
                rows_ref=rows_ref,
                block_width=band_block.width,
                page_stats=page_stats,
                block_left=band_block.left,
                block_right=band_block.right,
            )

            if block_type == "true_table":
                rendered = self._render_true_table(rows_cells, table_matrix)
            elif block_type == "key_value_list":
                rendered = self._render_key_value_list(rows_cells)
            else:
                rendered = self._render_plain_rows(rows_cells)

            metas.append({
                "type": block_type,
                "bbox": [band_block.left, band_block.top, band_block.right, band_block.bottom],
                "scores": scores,
                "row_count": len(rows_ref),
                "heading_count": 0,
            })

            if rendered.strip():
                parts.append(rendered)

        return "\n\n".join(parts).strip(), metas

    def _render_block(
        self,
        block: OCRBlock,
        page_stats: dict[str, float],
        page_width: int,
    ) -> tuple[str, list[dict[str, Any]]]:
        structural_subblocks = self._split_block_by_structure(block, page_stats, page_width)

        parts: list[str] = []
        metas: list[dict[str, Any]] = []

        for subblock in structural_subblocks:
            regions = self._detect_table_regions_in_block(subblock, page_stats, page_width)

            for region in regions:
                if region["kind"] == "table_region":
                    md, region_metas = self._render_table_region(
                        region_rows=region["rows"],
                        page_stats=page_stats,
                        page_width=page_width,
                    )
                    metas.extend(region_metas)
                    if md.strip():
                        parts.append(md)
                else:
                    md, meta = self._render_text_region(
                        region_rows=region["rows"],
                        page_stats=page_stats,
                        page_width=page_width,
                    )
                    metas.append(meta)
                    if md.strip():
                        parts.append(md)

        return "\n\n".join(parts).strip(), metas

    # ---------------------------------------------------------------------
    # postproceso
    # ---------------------------------------------------------------------

    def _dedupe_consecutive_lines(self, lines: list[str]) -> list[str]:
        result: list[str] = []
        prev_norm: str | None = None

        for line in lines:
            norm = self._normalize_ocr_artifacts(line)
            if norm and norm == prev_norm:
                continue
            result.append(line)
            prev_norm = norm if norm else prev_norm

        return result

    def _postprocess_markdown(self, markdown_text: str) -> str:
        lines = markdown_text.splitlines()

        cleaned: list[str] = []
        for line in lines:
            text = self._normalize_ocr_artifacts(line.rstrip())

            if not text:
                cleaned.append("")
                continue

            if self._is_noise_line(text):
                continue

            cleaned.append(text)

        cleaned = self._dedupe_consecutive_lines(cleaned)
        text = "\n".join(cleaned).strip()
        return re.sub(r"\n{3,}", "\n\n", text)

    # ---------------------------------------------------------------------
    # debug
    # ---------------------------------------------------------------------

    def _save_debug_artifacts(
        self,
        *,
        image_path: Path,
        page_index: int,
        ocr_data: dict[str, Any],
        markdown_text: str,
        block_debug: list[dict[str, Any]],
    ) -> None:
        if not self.settings.debug_save_ocr_artifacts:
            return

        debug_dir = image_path.parent / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        base_name = f"page_{page_index:04d}"

        (debug_dir / f"{base_name}.ocr.json").write_text(
            json.dumps(ocr_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (debug_dir / f"{base_name}.blocks.json").write_text(
            json.dumps(block_debug, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (debug_dir / f"{base_name}.md").write_text(markdown_text, encoding="utf-8")

    # ---------------------------------------------------------------------
    # página completa
    # ---------------------------------------------------------------------

    def _ocr_page_to_markdown(self, image_path: Path, page_index: int, job_id: str, lang: str | None = None) -> str:
        start = time.monotonic()

        try:
            with Image.open(image_path) as raw:
                image = self._preprocess_image(raw)
                data = self._ocr_image_data(image, lang=lang)

                words = self._extract_words(data)
                lines = self._group_lines(words)
                rows = self._merge_lines_into_rows(lines)
                blocks = self._group_rows_into_blocks(rows, page_width=image.width)
                ordered_blocks = self._order_blocks(blocks)
                page_stats = self._build_page_stats(rows, page_width=image.width)

                page_sections: list[str] = []
                block_debug: list[dict[str, Any]] = []

                for block in ordered_blocks:
                    block_md, metas = self._render_block(block, page_stats, image.width)
                    block_debug.extend(metas)
                    if block_md.strip():
                        page_sections.append(block_md)

                markdown_text = "\n\n".join(page_sections).strip()
                markdown_text = self._postprocess_markdown(markdown_text)

                if not markdown_text:
                    markdown_text = self._ocr_image_to_string(image, lang=lang)
                    markdown_text = self._postprocess_markdown(markdown_text)

                if not markdown_text:
                    markdown_text = "_No se detectó texto_"

                self._save_debug_artifacts(
                    image_path=image_path,
                    page_index=page_index,
                    ocr_data=data,
                    markdown_text=markdown_text,
                    block_debug=block_debug,
                )

        except OCRExecutionError:
            raise
        except Exception as exc:
            raise OCRExecutionError(f"Failed to OCR page {page_index}: {exc}") from exc

        elapsed = round(time.monotonic() - start, 3)
        logger.info(
            f"stage=ocr_page page={page_index} elapsed_seconds={elapsed}",
            extra={"job_id": job_id},
        )

        return markdown_text

    # ---------------------------------------------------------------------
    # documento completo
    # ---------------------------------------------------------------------

    def process_document(
        self,
        *,
        document_path: Path,
        job_id: str,
        report_progress,
        lang: str | None = None,
    ) -> dict[str, str]:
        total_start = time.monotonic()
        suffix = document_path.suffix.lower()

        if suffix == ".pdf":
            report_progress(
                stage="render_pdf",
                progress=10,
                current_page=None,
                total_pages=None,
                message="Rendering PDF pages to images",
            )

            page_images = self._render_pdf_to_images(document_path, job_id=job_id)
            total_pages = len(page_images)
            page_sections: list[str] = []
            include_page_headers = getattr(self.settings, "markdown_include_page_headers", False)

            for index, page_image in enumerate(page_images, start=1):
                progress = 20 + int((index / total_pages) * 70)
                report_progress(
                    stage="ocr_pages",
                    progress=progress,
                    current_page=index,
                    total_pages=total_pages,
                    message=f"Running OCR on page {index} of {total_pages}",
                )

                page_md = self._ocr_page_to_markdown(page_image, index, job_id, lang=lang)

                if include_page_headers:
                    page_sections.append(f"## Página {index}\n\n{page_md}")
                else:
                    page_sections.append(page_md)

            markdown_text = "\n\n---\n\n".join(page_sections).strip()

        else:
            report_progress(
                stage="ocr_image",
                progress=30,
                current_page=1,
                total_pages=1,
                message="Running OCR on image",
            )

            page_md = self._ocr_page_to_markdown(document_path, 1, job_id, lang=lang)

            report_progress(
                stage="finalize_markdown",
                progress=90,
                current_page=1,
                total_pages=1,
                message="Building markdown output",
            )

            markdown_text = page_md.strip()

        markdown_text = self._postprocess_markdown(markdown_text)

        if not markdown_text.strip():
            raise OCRExecutionError("Empty markdown output generated by Tesseract")

        if self.settings.debug_save_ocr_artifacts:
            debug_dir = document_path.parent / "debug"
            debug_dir.mkdir(parents=True, exist_ok=True)
            (debug_dir / "final_markdown.md").write_text(markdown_text, encoding="utf-8")

        elapsed_total = round(time.monotonic() - total_start, 3)
        logger.info(
            f"stage=complete elapsed_seconds={elapsed_total}",
            extra={"job_id": job_id},
        )

        return {
            "markdown_text": markdown_text,
        }