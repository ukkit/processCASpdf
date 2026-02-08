"""
#
# This script is extension of camspdf.py script written by Suhas Bharadwaj:
#  https://github.com/srbharadwaj/CAMSPdfExtractor
#
# I have modified the script to extract fund name and isin from CAS statements that are on multiple lines
#
# Version: 1.0.0
# Date: 2025-04-05
# Copyright (c) 2025, Neeraj <@ukkit>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
#   derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
"""

import csv
import json
import logging
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime

import pandas as pd
import pdfplumber
import requests

logger = logging.getLogger(__name__)

# Defining RegEx patterns
REGULAR_BUY_TXN = r"(?P<date>\d+\-\S+\-\d+)\s+(?P<txn>.*)\s+(?P<amount>[0-9]+\.[0-9]*)\s+(?P<units>[0-9]+\.[0-9]*)\s+(?P<nav>[0-9]+\.[0-9]*)\s+(?P<unitbalance>[0-9]+\.[0-9]*).*"
REGULAR_SELL_TXN = r"(?P<date>\d+\-\S+\-\d+)\s+(?P<txn>.*)\s+(?P<amount>\([0-9]+\.[0-9]*\))\s+(?P<units>\([0-9]+\.[0-9]*\))\s+(?P<nav>[0-9]+\.[0-9]*)\s+(?P<unitbalance>[0-9]+\.[0-9]*).*"
SEGR_BUY_TXN = r"(?P<date>\d+\-\S+\-\d+)\s+(?P<txn>.*)\s+(?P<units>[0-9]+\.[0-9]*)\s+(?P<unitbalance>[0-9]+\.[0-9]*).*"
FOLIO_PAN = r"^Folio No:\s+(?P<folio_num>.*)\s+PAN:\s+(?P<pan>[A-Z,0-9]{10})"
FNAME_ISIN = r"^(?P<fund_name>.*?)(?:\s*-\s*|\s+)ISIN:\s*(?P<isin>INF[A-Z0-9]{9}).*"

# Fund name indicator patterns used to identify lines containing mutual fund names
_FUND_NAME_PATTERNS = (
    "PAMP-",
    "-Growth",
    "-Direct",
    "-Regular",
    "-Plan",
    "-Fund",
    "-HDFC",
    "-ICICI",
    "-SBI",
    "-Axis",
    "-Kotak",
    "-Nippon",
    "-Tata",
    "-UTI",
    "-Aditya",
    "-Mirae",
    "-Parag",
    "-Edelweiss",
    "-DSP",
    "-Invesco",
    "-PGIM",
    "-HSBC",
    "-BNP",
    "-Franklin",
    "-IDFC",
    "-Reliance",
    "-L&T",
    "-Mahindra",
    "-Canara",
    "-Indiabulls",
    "-Motilal",
    "-Quantum",
    "-Sundaram",
    "-Taurus",
    "-JM",
    "-Principal",
    "-Baroda",
    "-LIC",
    "-BOI",
    "-Union",
    "-IDBI",
    "-IIFL",
    "-PPFAS",
    "-WhiteOak",
    "-Samco",
    "-Groww",
    "-KFintech",
    "-CAMS",
    "-Karvy",
    "-NSDL",
    "-CDSL",
    "-SEBI",
    "-AMFI",
    "-RBI",
    "-NSE",
    "-BSE",
    "-MCX",
    "-NCDEX",
    "-MCX-SX",
    "-OTCEI",
    "-ISE",
    "-USE",
    "-CSE",
    "-DSE",
    "-MSE",
    "-VSE",
    "-PSE",
    "-ASE",
    "-KSE",
    "-TSE",
    "-SSE",
    "-HSE",
    "-LSE",
    "-NYSE",
    "-NASDAQ",
    "-HKSE",
    "-SGX",
    "-ASX",
    "-TSX",
    "-FSE",
    "-XETRA",
    "-Euronext",
)


def _has_fund_name_pattern(line):
    """Check if a line contains a known mutual fund name indicator pattern."""
    return any(pattern in line for pattern in _FUND_NAME_PATTERNS)


def _clean_fund_name(raw_name):
    """Extract and clean fund name from raw text.

    Splits on the first hyphen and takes the right side,
    strips trailing hyphens/spaces, and removes "Registrar : CAMS".
    """
    name = raw_name.strip()
    if "-" in name:
        name = name.split("-", 1)[1].strip()
        name = name.rstrip("- ").strip()
    if "Registrar : CAMS" in name:
        name = name.replace("Registrar : CAMS", "").strip()
    return name


def _clean_fund_name_smart(raw_name):
    """Extract fund name using last-hyphen strategy with fallback.

    Tries splitting on the last hyphen first. If the result is too short
    or starts with '(', falls back to splitting on the first hyphen.
    """
    name = raw_name.strip()
    if "-" in name:
        last_part = name.split("-")[-1].strip()
        if len(last_part) < 5 or last_part.startswith("("):
            parts = name.split("-", 1)
            name = parts[1].strip() if len(parts) > 1 else last_part
        else:
            name = last_part
    if "Registrar : CAMS" in name:
        name = name.replace("Registrar : CAMS", "").strip()
    return name


def _extract_isin(text):
    """Extract an ISIN (INF + 9 alphanumeric chars) from text. Returns match or empty string."""
    match = re.search(r"INF[A-Z0-9]{9}", text)
    return match.group(0) if match else ""


@dataclass
class _EachLine:
    scheme_code: str
    isin_growth: str
    isin_div_reinv: str
    scheme_name: str
    nav: str
    date: str


class _LatestNav:
    def __init__(self) -> None:
        self.alldata: list[_EachLine] = []
        url = "https://portal.amfiindia.com/spages/NAVopen.txt"
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            html_content = response.text
            alllines = html_content.splitlines()
            self.process(alllines)
        else:
            logger.warning("Failed to retrieve the latest nav page. Status code: %s", response.status_code)

    def process(self, alllines):
        for eachline in alllines:
            if ";" in eachline and "Scheme Code" not in eachline:
                alltokens = eachline.split(";")
                a = _EachLine(
                    scheme_code=alltokens[0],
                    isin_growth=alltokens[1],
                    isin_div_reinv=alltokens[2],
                    scheme_name=alltokens[3],
                    nav=alltokens[4],
                    date=alltokens[5],
                )
                self.alldata.append(a)

    def get_sch_code(self, isin):
        for a in self.alldata:
            if a.isin_growth == isin or a.isin_div_reinv == isin:
                return a.scheme_code
        return ""


@dataclass
class _FundDetails:
    fund_name: str
    isin: str
    scheme_code: str
    folio_num: str
    date: str
    txn: str
    amount: float
    units: float
    nav: float
    balance_units: float


class _ProcessTextFile:
    def __init__(
        self,
        alllines="text.txt",
    ) -> None:
        self.alldata: list[_FundDetails] = []
        self.lnav = _LatestNav()
        if alllines == "text.txt":
            with open(alllines) as f:
                self.alllines = f.readlines()
        else:
            self.alllines = alllines
        self.process()

    def extract_fund_and_isin(self, lines, start_idx):
        """Extract fund name and ISIN from potentially multi-line text"""
        fund_name = ""
        isin = ""

        current_line = lines[start_idx].strip()
        logger.debug("Checking line for ISIN: %s", current_line)

        # Check if the current line ends with a hyphen, indicating ISIN might be on the next line
        if current_line.endswith("-") and start_idx + 1 < len(lines):
            next_line = lines[start_idx + 1].strip()
            logger.debug("Line ends with hyphen, checking next line: %s", next_line)

            if next_line.startswith("ISIN:"):
                potential_fund_name = current_line.rstrip("-").strip()
                fund_name = _clean_fund_name(potential_fund_name)
                logger.debug("Extracted fund_name from hyphen-ended line: %s", fund_name)

                isin_part = next_line.replace("ISIN:", "").strip()
                isin = _extract_isin(isin_part)
                if isin:
                    return fund_name, isin, start_idx + 1

        # Try to find ISIN in the current line
        if "ISIN:" in current_line:
            parts = current_line.split("ISIN:")
            if len(parts) > 1:
                fund_name = _clean_fund_name(parts[0])
                logger.debug("Extracted fund_name: %s", fund_name)

                # Look for ISIN in the same line
                isin_part = parts[1].strip()
                isin = _extract_isin(isin_part)
                if isin:
                    return fund_name, isin, start_idx

                # Check if ISIN is split across lines
                if (isin_part.endswith("INF") or "INF" in isin_part) and start_idx + 1 < len(lines):
                    next_line = lines[start_idx + 1].strip()
                    logger.debug("Found 'INF' in current line, checking next line: %s", next_line)

                    isin_rest_match = re.search(r"([A-Z0-9]{9})", next_line)
                    if isin_rest_match:
                        isin = f"INF{isin_rest_match.group(1)}"
                        logger.debug("Found split ISIN: %s", isin)
                        return fund_name, isin, start_idx + 1

        # If ISIN not found in current line, check next line
        if start_idx + 1 < len(lines):
            next_line = lines[start_idx + 1].strip()
            logger.debug("Checking next line for ISIN: %s", next_line)

            if "ISIN:" in next_line:
                if _has_fund_name_pattern(current_line):
                    fund_name = _clean_fund_name(current_line)
                    logger.debug("Extracted fund_name from current line (ISIN on next line): %s", fund_name)

                    isin_part = next_line.replace("ISIN:", "").strip()
                    isin = _extract_isin(isin_part)
                    if isin:
                        return fund_name, isin, start_idx + 1
                elif (
                    next_line.startswith("(Non-Demat)")
                    or next_line.startswith("(Demat)")
                    or next_line.startswith("(Physical)")
                ):
                    fund_name = _clean_fund_name(current_line)
                    logger.debug("Extracted fund_name from current line (Non-Demat on next line): %s", fund_name)

                    isin_part = next_line.replace("ISIN:", "").strip()
                    isin = _extract_isin(isin_part)
                    if isin:
                        return fund_name, isin, start_idx + 1
                else:
                    # Try to extract from the next line
                    isin_parts = next_line.split("ISIN:")
                    if len(isin_parts) > 1:
                        fund_name = _clean_fund_name(isin_parts[0])
                        logger.debug("Extracted fund_name from next line: %s", fund_name)

                        isin = _extract_isin(isin_parts[1])
                        if isin:
                            return fund_name, isin, start_idx + 1

            # Check if the next line contains the rest of a split ISIN
            isin_rest_match = re.search(r"([A-Z0-9]{9})", next_line)
            if isin_rest_match and "INF" in current_line:
                isin = f"INF{isin_rest_match.group(1)}"
                logger.debug("Found split ISIN across lines: %s", isin)
                return fund_name, isin, start_idx + 1

        # Aggressive approach: look ahead up to 3 lines for any ISIN
        for i in range(start_idx, min(start_idx + 3, len(lines))):
            line = lines[i].strip()
            if "ISIN:" in line:
                isin_parts = line.split("ISIN:")
                if len(isin_parts) > 1:
                    fund_name = _clean_fund_name(isin_parts[0])
                    logger.debug("Extracted fund_name from aggressive search: %s", fund_name)

                    isin = _extract_isin(isin_parts[1])
                    if isin:
                        return fund_name, isin, i

        return fund_name, isin, start_idx

    def write_to_csv(self, csv_file_name=None):
        if csv_file_name is None:
            csv_file_name = f"CAMS_data_{datetime.now().strftime('%d_%m_%Y_%H_%M')}.csv"
        fieldnames = [field.name for field in _FundDetails.__dataclass_fields__.values()]

        with open(csv_file_name, mode="w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for item in self.alldata:
                writer.writerow(asdict(item))

        logger.info('CSV file "%s" created successfully.', csv_file_name)

    def process(self):
        if not self.alllines:
            return
        folio_num = ""
        fund_name = ""
        isin = ""

        logger.debug("First 20 lines of the PDF text:")
        for idx, line in enumerate(self.alllines[:20]):
            logger.debug("Line %d: %s", idx, line.strip())

        i = 0
        while i < len(self.alllines):
            eachline = self.alllines[i]

            m = re.match(FOLIO_PAN, eachline)
            if m:
                folio_num = m.groupdict().get("folio_num", "")
                logger.debug("Found folio_num: %s", folio_num)
                i += 1
                continue

            # Try the regex pattern first
            m = re.match(FNAME_ISIN, eachline)
            if m:
                fund_name = m.groupdict().get("fund_name", "")
                isin = m.groupdict().get("isin", "")

                if "Registrar : CAMS" in fund_name:
                    fund_name = fund_name.replace("Registrar : CAMS", "").strip()

                logger.debug("Regex match - Found fund_name: %s", fund_name)
                logger.debug("Regex match - Found isin: %s", isin)
                i += 1
                continue

            # Try the multi-line extraction function
            if "ISIN:" in eachline or (i + 1 < len(self.alllines) and "ISIN:" in self.alllines[i + 1]):
                logger.debug("Attempting multi-line extraction starting at line %d", i)
                extracted_fund_name, extracted_isin, new_idx = self.extract_fund_and_isin(self.alllines, i)
                if extracted_fund_name and extracted_isin:
                    fund_name = extracted_fund_name
                    isin = extracted_isin
                    logger.debug("Multi-line extraction - Found fund_name: %s", fund_name)
                    logger.debug("Multi-line extraction - Found isin: %s", isin)
                    i = new_idx + 1
                    continue

            # Check for split ISIN codes (INF on one line, rest on next line)
            if "INF" in eachline and "ISIN:" in eachline and i + 1 < len(self.alllines):
                next_line = self.alllines[i + 1].strip()
                logger.debug("Checking for split ISIN - Current line: %s", eachline.strip())
                logger.debug("Checking for split ISIN - Next line: %s", next_line)

                isin_rest_match = re.search(r"([A-Z0-9]{9})", next_line)
                if isin_rest_match:
                    isin_parts = eachline.split("ISIN:")
                    if len(isin_parts) > 1:
                        fund_name = _clean_fund_name_smart(isin_parts[0])
                        logger.debug("Extracted fund_name: %s", fund_name)

                    isin = f"INF{isin_rest_match.group(1)}"
                    logger.debug("Found split ISIN across lines: %s", isin)
                    logger.debug("Found fund_name: %s", fund_name)
                    i += 1  # Skip the next line since we've processed it
                    continue

            # Special case: Fund name on current line, ISIN on next line
            if i + 1 < len(self.alllines) and "ISIN:" in self.alllines[i + 1]:
                current_line = eachline.strip()
                next_line = self.alllines[i + 1].strip()

                if _has_fund_name_pattern(current_line):
                    fund_name = _clean_fund_name(current_line)
                    logger.debug("Special case - Extracted fund_name from current line: %s", fund_name)

                    isin_part = next_line.replace("ISIN:", "").strip()
                    isin = _extract_isin(isin_part)
                    if isin:
                        logger.debug("Special case - Found isin: %s", isin)
                        i += 1  # Skip the next line since we've processed it
                        continue

                # Special case: Check if the next line starts with "(Non-Demat)" or similar
                if (
                    next_line.startswith("(Non-Demat)")
                    or next_line.startswith("(Demat)")
                    or next_line.startswith("(Physical)")
                ):
                    fund_name = _clean_fund_name(current_line)
                    logger.debug("Special case (Non-Demat) - Extracted fund_name from current line: %s", fund_name)

                    isin_part = next_line.replace("ISIN:", "").strip()
                    isin = _extract_isin(isin_part)
                    if isin:
                        logger.debug("Special case (Non-Demat) - Found isin: %s", isin)
                        i += 1  # Skip the next line since we've processed it
                        continue

            # Fallback: Look for lines containing "ISIN:" and extract manually
            if "ISIN:" in eachline:
                logger.debug("Found line with ISIN: %s", eachline.strip())

                isin_parts = eachline.split("ISIN:")
                if len(isin_parts) > 1:
                    fund_name = _clean_fund_name_smart(isin_parts[0])

                    isin_part = isin_parts[1].strip()
                    isin = _extract_isin(isin_part)
                    if not isin and (isin_part.endswith("INF") or "INF" in isin_part) and i + 1 < len(self.alllines):
                        next_line = self.alllines[i + 1].strip()
                        logger.debug("Found 'INF' in current line, checking next line: %s", next_line)

                        isin_rest_match = re.search(r"([A-Z0-9]{9})", next_line)
                        if isin_rest_match:
                            isin = f"INF{isin_rest_match.group(1)}"
                            logger.debug("Found split ISIN: %s", isin)
                            i += 1  # Skip the next line since we've processed it

                    logger.debug("Manual extraction - Found fund_name: %s", fund_name)
                    logger.debug("Manual extraction - Found isin: %s", isin)
                i += 1
                continue

            # Process transaction lines
            m = re.match(REGULAR_BUY_TXN, eachline)
            if m:
                date = m.groupdict().get("date", "")
                txn = "Buy"
                amount = float(m.groupdict().get("amount", 0))
                units = float(m.groupdict().get("units", 0))
                nav = float(m.groupdict().get("nav", 0))
                balance_units = float(m.groupdict().get("unitbalance", 0))

                t = _FundDetails(
                    folio_num=folio_num,
                    fund_name=fund_name,
                    isin=isin,
                    scheme_code=self.lnav.get_sch_code(isin),
                    date=date,
                    txn=txn,
                    amount=amount,
                    units=units,
                    nav=nav,
                    balance_units=balance_units,
                )
                self.alldata.append(t)
                i += 1
                continue

            m = re.match(REGULAR_SELL_TXN, eachline)
            if m:
                date = m.groupdict().get("date", "")
                txn = "Sell"
                amount_str = m.groupdict().get("amount", "0")
                amount = float(re.sub(r"\(|\)", "", amount_str))
                units_str = m.groupdict().get("units", "0")
                units = float(re.sub(r"\(|\)", "", units_str))
                nav = float(m.groupdict().get("nav", 0))
                balance_units = float(m.groupdict().get("unitbalance", 0))

                t = _FundDetails(
                    folio_num=folio_num,
                    fund_name=fund_name,
                    isin=isin,
                    scheme_code=self.lnav.get_sch_code(isin),
                    date=date,
                    txn=txn,
                    amount=amount,
                    units=units,
                    nav=nav,
                    balance_units=balance_units,
                )
                self.alldata.append(t)
                i += 1
                continue

            m = re.match(SEGR_BUY_TXN, eachline)
            if m:
                date = m.groupdict().get("date", "")
                txn = "Buy"
                amount = 0.0
                units = float(m.groupdict().get("units", 0))
                nav = 0.0
                balance_units = float(m.groupdict().get("unitbalance", 0))

                t = _FundDetails(
                    folio_num=folio_num,
                    fund_name=fund_name,
                    isin=isin,
                    scheme_code=self.lnav.get_sch_code(isin),
                    date=date,
                    txn=txn,
                    amount=amount,
                    units=units,
                    nav=nav,
                    balance_units=balance_units,
                )
                self.alldata.append(t)
                i += 1
                continue

            # If we get here, we didn't match any pattern
            i += 1


class ProcessPDF:
    def __init__(self, filename, password=None) -> None:
        if not filename:
            raise ValueError("filename cannot be empty")
        if not os.path.isfile(filename):
            raise FileNotFoundError(f"PDF file not found: {filename}")
        self.filename = filename
        self.password = password
        self.alldata: list[_FundDetails] = []

    def get_pdf_data(self, output_format="csv"):
        format_specifiers = ["dicts", "csv", "json", "df"]
        if output_format not in format_specifiers:
            raise ValueError(f"Output format must be one of {', '.join(format_specifiers)}")

        file_path = self.filename
        doc_pwd = self.password
        final_text = ""
        logger.info("Processing PDF. Please wait...")
        with pdfplumber.open(file_path, password=doc_pwd) as pdf:
            for page in pdf.pages:
                txt = page.extract_text()
                if txt:
                    final_text = final_text + "\n" + txt

        # Replace all occurrences of ',' with an empty string
        final_text = final_text.replace(",", "")
        pt = _ProcessTextFile(alllines=final_text.splitlines())

        if output_format == "csv":
            pt.write_to_csv()
            return None
        item_dicts = [asdict(item) for item in pt.alldata]
        if output_format == "df":
            return pd.DataFrame(item_dicts)
        elif output_format == "json":
            return json.dumps(item_dicts)
        else:
            return item_dicts
