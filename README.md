# CAMS Mutual Fund PDF Statement Parser

A Python tool/library that extracts data from CAMS Mutual Fund PDF statements (India) into various formats including CSV, DataFrame, JSON, or a list of dictionaries.

## Features

- Extracts fund details, transaction history, and other relevant information from CAMS Mutual Fund PDF statements
- Supports password-protected PDFs
- Outputs data in multiple formats: CSV, DataFrame, JSON, or list of dictionaries
- Automatically fetches latest NAV data from AMFI India portal
- Handles various fund name formats and ISIN codes

## Installation

1. Clone this repository or download the files
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```python
from app.processCASpdf import ProcessPDF

# Initialize with PDF file path and password (if required)
filename = "path/to/your/statement.pdf"
password = "your_password"  # Use None if PDF is not password-protected

# Create a ProcessPDF instance
pp = ProcessPDF(filename, password)

# Get data in your preferred format
# Available formats: "csv", "df", "json", "dicts"
output = pp.get_pdf_data(output_format="df")

# If output_format is "csv", a CSV file will be created with the current timestamp
# If output_format is "df", a pandas DataFrame will be returned
# If output_format is "json", a JSON string will be returned
# If output_format is "dicts", a list of dictionaries will be returned
```

### Example Output

When using `output_format="df"`, the output will be a pandas DataFrame with the following columns:

- `fund_name`: Name of the mutual fund
- `isin`: ISIN code of the fund
- `scheme_code`: Scheme code from AMFI
- `folio_num`: Folio number
- `date`: Transaction date
- `txn`: Transaction type (Buy/Sell)
- `amount`: Transaction amount
- `units`: Number of units
- `nav`: Net Asset Value
- `balance_units`: Balance units after transaction

## Requirements

See `requirements.txt` for a list of required Python packages.

## License

This software is provided under the BSD license. See the copyright notice in the source code for details.

## Credits

This script is an extension of the `camspdf.py` script originally written by Suhas Bharadwaj. 