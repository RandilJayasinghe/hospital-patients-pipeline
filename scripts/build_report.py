"""
Build the submission documents from docs/REPORT.md:

    docs/REPORT.docx   editable Word document (pandoc)
    docs/REPORT.pdf    print-ready PDF (pandoc -> styled HTML -> headless Chrome/Edge)

Images referenced by the Markdown (docs/report/images/*.png) are embedded in both.

Requirements (host side, not needed to run the pipeline):
    pip install pypandoc_binary        # bundles pandoc
    Google Chrome or Microsoft Edge    # for the PDF step

Usage:
    python scripts/build_report.py            # both
    python scripts/build_report.py --docx     # Word only
    python scripts/build_report.py --pdf      # PDF only
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

import pypandoc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, "docs")
SOURCE = os.path.join(DOCS, "REPORT.md")
DOCX_OUT = os.path.join(DOCS, "REPORT.docx")
PDF_OUT = os.path.join(DOCS, "REPORT.pdf")

BROWSERS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    "google-chrome",
    "chromium",
    "microsoft-edge",
]

PRINT_CSS = """
@page { size: A4; margin: 14mm 13mm 15mm 13mm; }
html { font-size: 9.6pt; }
body { font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif; line-height: 1.33; color: #1a1a1a;
       max-width: none; margin: 0; padding: 0; }
h1 { font-size: 20pt; margin: 0 0 4pt; line-height: 1.2; }
h2 { font-size: 13.5pt; margin-top: 15pt; border-bottom: 1.5px solid #2b4c7e; padding-bottom: 3pt;
     color: #1f3b63; page-break-after: avoid; }
h3 { font-size: 11pt; margin-top: 9pt; color: #1f3b63; page-break-after: avoid; }
p, li { orphans: 3; widows: 3; margin: 0 0 4pt; }
ul, ol { margin: 2pt 0 4pt; padding-left: 16pt; }
table { border-collapse: collapse; width: 100%; table-layout: auto; margin: 5pt 0; font-size: 8pt; page-break-inside: auto; }
col { width: auto !important; }
tr { page-break-inside: avoid; }
th, td { border: 1px solid #b9c2cf; padding: 2pt 4pt; vertical-align: top; text-align: left; }
th { background: #e8eef7; }
code { font-family: Consolas, "Courier New", monospace; font-size: 8.2pt; background: #f3f5f8; padding: 0 2px; }
pre { background: #f3f5f8; border: 1px solid #d5dbe4; padding: 4pt; font-size: 7.8pt; white-space: pre-wrap;
      page-break-inside: avoid; }
pre code { background: none; padding: 0; }
figure { margin: 6pt 0; text-align: center; page-break-inside: avoid; }
figure img, p > img { max-width: 100%; max-height: 118mm; width: auto; height: auto; object-fit: contain; border: 1px solid #d5dbe4; }
figcaption { font-size: 7.8pt; color: #444; font-style: italic; margin-top: 3pt; text-align: left; }
hr { border: none; border-top: 1px solid #ccd3dd; margin: 12pt 0; }
blockquote { border-left: 3px solid #2b4c7e; margin: 8pt 0; padding: 2pt 10pt; color: #333; background: #f6f8fb; }
"""


def find_browser() -> str:
    for candidate in BROWSERS:
        path = candidate if os.path.isabs(candidate) else shutil.which(candidate)
        if path and os.path.exists(path):
            return path
    sys.exit("No Chrome/Edge found for the PDF step; install one or use --docx only.")


def build_docx() -> None:
    pypandoc.convert_file(
        SOURCE,
        "docx",
        outputfile=DOCX_OUT,
        extra_args=[
            "--from=markdown+pipe_tables+implicit_figures+grid_tables",
            f"--resource-path={DOCS}",
            "--toc",
            "--toc-depth=2",
        ],
    )
    autosize_docx_tables(DOCX_OUT)
    print(f"wrote {DOCX_OUT} ({os.path.getsize(DOCX_OUT) // 1024} KB)")


def autosize_docx_tables(path: str) -> None:
    """Pandoc gives pipe-table columns equal fixed widths, which wastes space on narrow
    columns (e.g. "#"). Switch every table to Word's auto-fit layout, use full page
    width, and add cell padding so the document reads well without manual fixing."""
    import docx
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn

    document = docx.Document(path)
    for table in document.tables:
        table.autofit = True
        tbl_pr = table._tbl.tblPr
        for tag in ("w:tblW",):
            for el in tbl_pr.findall(qn(tag)):
                tbl_pr.remove(el)
        width = OxmlElement("w:tblW")
        width.set(qn("w:type"), "pct")
        width.set(qn("w:w"), "5000")  # 100 % of text width
        tbl_pr.append(width)
        for cell_props in table._tbl.iter(qn("w:tcW")):
            cell_props.set(qn("w:type"), "auto")
            cell_props.set(qn("w:w"), "0")
        for grid_col in table._tbl.iter(qn("w:gridCol")):
            grid_col.attrib.pop(qn("w:w"), None)
        _add_borders_and_header_shading(table, OxmlElement, qn)
    document.save(path)


def _add_borders_and_header_shading(table, OxmlElement, qn) -> None:
    tbl_pr = table._tbl.tblPr
    for el in tbl_pr.findall(qn("w:tblBorders")):
        tbl_pr.remove(el)
    borders = OxmlElement("w:tblBorders")
    for side in ("top", "left", "bottom", "right", "insideH", "insideV"):
        edge = OxmlElement(f"w:{side}")
        edge.set(qn("w:val"), "single")
        edge.set(qn("w:sz"), "4")
        edge.set(qn("w:space"), "0")
        edge.set(qn("w:color"), "B9C2CF")
        borders.append(edge)
    tbl_pr.append(borders)
    for cell in table.rows[0].cells:  # header row: light blue shading
        tc_pr = cell._tc.get_or_add_tcPr()
        shade = OxmlElement("w:shd")
        shade.set(qn("w:val"), "clear")
        shade.set(qn("w:color"), "auto")
        shade.set(qn("w:fill"), "E8EEF7")
        tc_pr.append(shade)


def build_pdf() -> None:
    browser = find_browser()
    with tempfile.TemporaryDirectory() as tmp:
        css_path = os.path.join(tmp, "print.css")
        html_path = os.path.join(tmp, "REPORT.html")
        with open(css_path, "w", encoding="utf-8") as fh:
            fh.write(PRINT_CSS)
        # --embed-resources inlines the images and CSS, so the HTML is self-contained.
        pypandoc.convert_file(
            SOURCE,
            "html5",
            outputfile=html_path,
            extra_args=[
                "--from=markdown+pipe_tables+implicit_figures+grid_tables",
                f"--resource-path={DOCS}",
                "--standalone",
                "--embed-resources",
                f"--css={css_path}",
                "--metadata=pagetitle:EC8203 Mini Project Report",
            ],
        )
        subprocess.run(
            [
                browser,
                "--headless=new",
                "--disable-gpu",
                "--no-pdf-header-footer",
                f"--print-to-pdf={PDF_OUT}",
                "file:///" + html_path.replace("\\", "/"),
            ],
            check=True,
            timeout=180,
        )
    print(f"wrote {PDF_OUT} ({os.path.getsize(PDF_OUT) // 1024} KB)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build REPORT.docx / REPORT.pdf from docs/REPORT.md")
    parser.add_argument("--docx", action="store_true")
    parser.add_argument("--pdf", action="store_true")
    args = parser.parse_args()
    both = not (args.docx or args.pdf)
    if args.docx or both:
        build_docx()
    if args.pdf or both:
        build_pdf()


if __name__ == "__main__":
    main()
