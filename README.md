# Mutual Fund CAS    PDF Statement Parser

A Python tool/library that extracts data from Consolidated Account Statement (CAS) PDFs (India) — tested with CAMS and KFintech — into CSV, DataFrame, JSON, or a list of dictionaries.

## Requirements

- Python >= 3.9
- [uv](https://github.com/astral-sh/uv) package manager
- Internet connection (fetches AMFI scheme data on each run)

## Installation

```bash
git clone https://github.com/your-username/processCASpdf.git
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
| `txn` | str | `Buy` or `Sell` |
| `amount` | float | Transaction amount (INR) |
| `units` | float | Units transacted |
| `nav` | float | NAV at time of transaction |
| `balance_units` | float | Unit balance after transaction |

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

## Credits

Based on `camspdf.py` originally written by Suhas Bharadwaj.
