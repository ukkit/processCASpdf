# Mutual Fund CAS PDF Statement Parser

A Python tool/library that extracts data from Consolidated Account Statement (CAS) PDFs (https://github.com/ukkit/processCASpdf) — tested with KFintech — into CSV, DataFrame, JSON, or a list of dictionaries.

## Requirements

- Python >= 3.9
- [uv](https://github.com/astral-sh/uv) package manager
- Internet connection (fetches AMFI scheme data on each run)

## Installation

Install from [PyPI](https://pypi.org/project/processcaspdf/):

```bash
pip install processcaspdf
# or
uv add processcaspdf
```

### From source (for development)

```bash
git clone https://github.com/ukkit/processCASpdf.git
cd processCASpdf
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

## Usage

```python
from processCASpdf import ProcessPDF

pdf = ProcessPDF("CAS_statement.pdf", password="your_pdf_password")
```

- `filename` (required) - Path to the CAS PDF file.
- `password` (optional) - PDF password (usually your PAN in uppercase).

### Output Formats

Call `get_pdf_data(format)` with one of: `"csv"` (default), `"df"`, `"json"`, `"dicts"`.

```python
pdf.get_pdf_data("csv")          # writes CAS_data_<timestamp>.csv to current directory
df  = pdf.get_pdf_data("df")     # returns pandas DataFrame
js  = pdf.get_pdf_data("json")   # returns JSON string
rec = pdf.get_pdf_data("dicts")  # returns list of dicts
```

### Output Fields

| Field | Type | Description |
|---|---|---|
| `fund_name` | str | Mutual fund scheme name |
| `isin` | str | ISIN code (e.g. `INF...`) |
| `scheme_code` | str | AMFI scheme code; empty if lookup fails |
| `folio_num` | str | Folio number |
| `date` | str | Transaction date (e.g. `01-Jan-2025`) |
| `txn` | str | `Buy`, `Sell`, `IDCW_PAYOUT`, or `IDCW_REINVEST` |
| `amount` | float | Transaction amount (INR) |
| `units` | float | Units transacted (`0.0` for `IDCW_PAYOUT`, which allots no units) |
| `nav` | float | NAV at time of transaction (`0.0` for `IDCW_PAYOUT`) |
| `balance_units` | float | Unit balance after transaction (`0.0` for `IDCW_PAYOUT`, since it isn't printed on that row) |
| `narration` | str | Raw transaction line text as it appeared in the PDF |

### Example

```python
import logging
from processCASpdf import ProcessPDF

logging.basicConfig(level=logging.DEBUG)  # optional

pdf = ProcessPDF("MyCAS.pdf", password="ABCDE1234F")
df = pdf.get_pdf_data("df")

df[df["fund_name"].str.contains("HDFC", case=False)]
df.to_excel("cas_transactions.xlsx", index=False)
```

## Troubleshooting

| Problem | Likely cause / fix |
|---|---|
| `PDFPasswordIncorrect` | Wrong or missing password. Try your PAN in uppercase. |
| No transactions extracted | Enable debug logging to inspect raw PDF text. |
| `scheme_code` is empty | ISIN not found in current AMFI data (new or discontinued scheme). |
| Network error on startup | AMFI data fetch requires outbound HTTPS. |

## Development

```bash
uv sync --group dev
uv run ruff check .
uv run ruff format .
uv run mypy processCASpdf.py
uv run pre-commit install
```

## Changelog

### 0.3.2
- Recognize IDCW/dividend transactions: `IDCW_PAYOUT` (cash payout, no units allotted) and `IDCW_REINVEST` (reinvested, same shape as a Buy) are now emitted instead of being silently dropped.
- Add a `narration` field carrying the raw PDF transaction line text, so any row can be inspected even if a future statement format doesn't cleanly match the parser's patterns.
- Fix the PyPI publish workflow, which was broken by a nonexistent `astral-sh/setup-uv@v10` action pin.

### 0.3.1
- Add PyPI packaging metadata (readme, authors, SPDX license, project URLs).
- Fix license header/attribution to match `LICENSE` and credit the original `camspdf.py`.

### 0.3.0
- Refactor fund-name/ISIN extraction and trim irrelevant fund-name indicator patterns.
- Add `_strip_registrar()` helper (CAMS/KFintech/Karvy) and remove duplicated cleanup logic.
- Simplify `process()`, removing redundant ISIN-extraction fallback paths.
- Rename CSV output prefix from `CAMS_data_` to `CAS_data_`.

## Credits

Based on [`camspdf.py`](https://github.com/srbharadwaj/CAMSPdfExtractor) originally written by Suhas Bharadwaj.
