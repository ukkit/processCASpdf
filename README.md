# CAMS Mutual Fund PDF Statement Parser

A Python tool/library that extracts data from CAMS Mutual Fund PDF statements (India) into various formats including CSV, DataFrame, JSON, or a list of dictionaries.

## Features

- Extracts fund details, transaction history, and other relevant information from CAMS Mutual Fund PDF statements
- Supports password-protected PDFs
- Outputs data in multiple formats: CSV, DataFrame, JSON, or list of dictionaries
- Automatically fetches latest NAV data from AMFI India portal
- Handles various fund name formats and ISIN codes, including multi-line cases

## Installation

1. Clone this repository or download the files
2. Install [uv](https://docs.astral.sh/uv/) if you haven't already
3. Install the required dependencies:

```bash
uv sync
```

## Usage

### Basic Usage

```python
from processCASpdf import ProcessPDF

# Create a processor instance
pdf = ProcessPDF("path/to/your/CAS_statement.pdf", password="your_pdf_password")
```

- `filename` (required) - Path to the CAMS CAS PDF file.
- `password` (optional) - PDF password, if the file is password-protected.

### Output Formats

Call `get_pdf_data()` with one of four format options:

#### CSV (default)

Writes a CSV file to the current directory named `CAMS_data_<timestamp>.csv`.

```python
pdf.get_pdf_data("csv")
```

#### DataFrame

Returns a pandas `DataFrame`.

```python
df = pdf.get_pdf_data("df")
print(df.head())
```

#### JSON

Returns a JSON string.

```python
json_str = pdf.get_pdf_data("json")
```

#### List of Dicts

Returns a list of Python dictionaries.

```python
records = pdf.get_pdf_data("dicts")
for record in records:
    print(record)
```

### Output Fields

Each record contains:

| Field | Type | Description |
|---|---|---|
| `fund_name` | str | Mutual fund scheme name |
| `isin` | str | ISIN code (e.g. `INF...`) |
| `scheme_code` | str | AMFI scheme code (fetched live) |
| `folio_num` | str | Folio number |
| `date` | str | Transaction date (e.g. `01-Jan-2025`) |
| `txn` | str | Transaction type (`Buy` or `Sell`) |
| `amount` | float | Transaction amount |
| `units` | float | Number of units transacted |
| `nav` | float | NAV at time of transaction |
| `balance_units` | float | Unit balance after transaction |

### Full Example

```python
import logging
from processCASpdf import ProcessPDF

# Enable debug logging (optional)
logging.basicConfig(level=logging.DEBUG)

# Process the PDF and get a DataFrame
pdf = ProcessPDF("MyCAS.pdf", password="mypassword")
df = pdf.get_pdf_data("df")

# Filter by fund
hdfc_txns = df[df["fund_name"].str.contains("HDFC", case=False)]
print(hdfc_txns)

# Export to Excel
df.to_excel("cas_transactions.xlsx", index=False)
```

## Notes

- The script fetches the latest NAV data from AMFI on each run to resolve scheme codes. An internet connection is required.
- Commas in the PDF text are stripped automatically before parsing.
- The parser handles fund names and ISIN codes that span multiple lines in the PDF.

## License

This software is provided under the BSD license. See the copyright notice in the source code for details.

## Credits

This script is an extension of the `camspdf.py` script originally written by Suhas Bharadwaj.
