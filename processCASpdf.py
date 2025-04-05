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

import os
import re
from datetime import datetime
from dataclasses import dataclass, asdict
import csv
import json
import traceback
import pdfplumber
import pandas as pd
import requests


basedir = os.path.dirname(__file__)


# Defining RegEx patterns
REGULAR_BUY_TXN = r"(?P<date>\d+\-\S+\-\d+)\s+(?P<txn>.*)\s+(?P<amount>[0-9]+\.[0-9]*)\s+(?P<units>[0-9]+\.[0-9]*)\s+(?P<nav>[0-9]+\.[0-9]*)\s+(?P<unitbalance>[0-9]+\.[0-9]*).*"
REGULAR_SELL_TXN = r"(?P<date>\d+\-\S+\-\d+)\s+(?P<txn>.*)\s+(?P<amount>\([0-9]+\.[0-9]*\))\s+(?P<units>\([0-9]+\.[0-9]*\))\s+(?P<nav>[0-9]+\.[0-9]*)\s+(?P<unitbalance>[0-9]+\.[0-9]*).*"
SEGR_BUY_TXN = r"(?P<date>\d+\-\S+\-\d+)\s+(?P<txn>.*)\s+(?P<units>[0-9]+\.[0-9]*)\s+(?P<unitbalance>[0-9]+\.[0-9]*).*"
FOLIO_PAN = r"^Folio No:\s+(?P<folio_num>.*)\s+PAN:\s+(?P<pan>[A-Z,0-9]{10})"
FNAME_ISIN = r"^(?P<fund_name>.*?)(?:\s*-\s*|\s+)ISIN:\s*(?P<isin>INF[A-Z0-9]{9}).*"


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
        self.alldata = []
        url = "https://portal.amfiindia.com/spages/NAVopen.txt"
        response = requests.get(url, timeout=60)
        if response.status_code == 200:
            # Parse the HTML content of the page
            html_content = response.text
            # print(html_content)
            alllines = html_content.splitlines()
            # print(len(alllines))
            # print(alllines[0])
            self.process(alllines)
        else:
            print(
                f"Failed to retrieve the latest nav page. Status code: {response.status_code}"
            )
            # You can handle errors or exit the script here

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
        self.alldata = []
        self.lnav = _LatestNav()
        if alllines == "text.txt":
            with open(alllines, "r") as f:
                self.alllines = f.readlines()
        else:
            self.alllines = alllines
        self.process()

    def extract_fund_and_isin(self, lines, start_idx):
        """Extract fund name and ISIN from potentially multi-line text"""
        fund_name = ""
        isin = ""
        
        # Look for ISIN in the current line
        current_line = lines[start_idx].strip()
        print(f"Checking line for ISIN: {current_line}")
        
        # Check if the current line ends with a hyphen, indicating ISIN might be on the next line
        if current_line.endswith("-"):
            # Check the next line for ISIN
            if start_idx + 1 < len(lines):
                next_line = lines[start_idx + 1].strip()
                print(f"Line ends with hyphen, checking next line: {next_line}")
                
                # Check if the next line starts with "ISIN:"
                if next_line.startswith("ISIN:"):
                    # Extract fund name from the current line (remove trailing hyphen)
                    potential_fund_name = current_line.rstrip("-").strip()
                    
                    # Handle fund names with hyphens
                    if "-" in potential_fund_name:
                        # Take everything after the first hyphen
                        fund_name = potential_fund_name.split("-", 1)[1].strip()
                        # Trim any trailing hyphens and spaces
                        fund_name = fund_name.rstrip("- ").strip()
                    else:
                        fund_name = potential_fund_name.strip()
                    
                    # Remove "Registrar : CAMS" if present
                    if "Registrar : CAMS" in fund_name:
                        fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                    
                    print(f"Extracted fund_name from hyphen-ended line: {fund_name}")
                    
                    # Extract ISIN from the next line
                    isin_part = next_line.replace("ISIN:", "").strip()
                    isin_match = re.search(r'INF[A-Z0-9]{9}', isin_part)
                    if isin_match:
                        isin = isin_match.group(0)
                        return fund_name, isin, start_idx + 1
        
        # Try to find ISIN in the current line
        if "ISIN:" in current_line:
            # Split by ISIN: and extract fund name
            parts = current_line.split("ISIN:")
            if len(parts) > 1:
                # Extract fund name - take everything after the first hyphen
                potential_fund_name = parts[0].strip()
                
                # Handle fund names with hyphens
                if "-" in potential_fund_name:
                    # Take everything after the first hyphen
                    fund_name = potential_fund_name.split("-", 1)[1].strip()
                    # Trim any trailing hyphens and spaces
                    fund_name = fund_name.rstrip("- ").strip()
                else:
                    fund_name = potential_fund_name.strip()
                
                # Remove "Registrar : CAMS" if present
                if "Registrar : CAMS" in fund_name:
                    fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                
                print(f"Extracted fund_name: {fund_name}")
                
                # Look for ISIN in the same line
                isin_part = parts[1].strip()
                isin_match = re.search(r'INF[A-Z0-9]{9}', isin_part)
                if isin_match:
                    isin = isin_match.group(0)
                    return fund_name, isin, start_idx
                
                # Check if the line ends with "INF" or contains "INF" followed by something else
                if isin_part.endswith("INF") or "INF" in isin_part:
                    # The ISIN might be split across lines
                    # Check the next line for the rest of the ISIN
                    if start_idx + 1 < len(lines):
                        next_line = lines[start_idx + 1].strip()
                        print(f"Found 'INF' in current line, checking next line: {next_line}")
                        
                        # Look for a pattern that might be the rest of the ISIN (9 alphanumeric characters)
                        isin_rest_match = re.search(r'([A-Z0-9]{9})', next_line)
                        if isin_rest_match:
                            isin_rest = isin_rest_match.group(1)
                            isin = f"INF{isin_rest}"
                            print(f"Found split ISIN: {isin}")
                            return fund_name, isin, start_idx + 1
        
        # If ISIN not found in current line, check next line
        if start_idx + 1 < len(lines):
            next_line = lines[start_idx + 1].strip()
            print(f"Checking next line for ISIN: {next_line}")
            
            # Look for ISIN in the next line
            if "ISIN:" in next_line:
                # Check if the current line contains a fund name pattern
                # Look for patterns like "PAMP-" or other fund name indicators
                if ("PAMP-" in current_line or "-Growth" in current_line or "-Direct" in current_line or 
                    "-Regular" in current_line or "-Plan" in current_line or "-Fund" in current_line or
                    "-HDFC" in current_line or "-ICICI" in current_line or "-SBI" in current_line or
                    "-Axis" in current_line or "-Kotak" in current_line or "-Nippon" in current_line or
                    "-Tata" in current_line or "-UTI" in current_line or "-Aditya" in current_line or
                    "-Mirae" in current_line or "-Parag" in current_line or "-Edelweiss" in current_line or
                    "-DSP" in current_line or "-Invesco" in current_line or "-PGIM" in current_line or
                    "-HSBC" in current_line or "-BNP" in current_line or "-Franklin" in current_line or
                    "-IDFC" in current_line or "-Reliance" in current_line or "-L&T" in current_line or
                    "-Mahindra" in current_line or "-Canara" in current_line or "-Indiabulls" in current_line or
                    "-Motilal" in current_line or "-Quantum" in current_line or "-Sundaram" in current_line or
                    "-Taurus" in current_line or "-JM" in current_line or "-Principal" in current_line or
                    "-Baroda" in current_line or "-LIC" in current_line or "-BOI" in current_line or
                    "-Union" in current_line or "-IDBI" in current_line or "-IIFL" in current_line or
                    "-PPFAS" in current_line or "-WhiteOak" in current_line or "-Samco" in current_line or
                    "-Groww" in current_line or "-KFintech" in current_line or "-CAMS" in current_line or
                    "-Karvy" in current_line or "-NSDL" in current_line or "-CDSL" in current_line or
                    "-SEBI" in current_line or "-AMFI" in current_line or "-RBI" in current_line or
                    "-NSE" in current_line or "-BSE" in current_line or "-MCX" in current_line or
                    "-NCDEX" in current_line or "-MCX-SX" in current_line or "-OTCEI" in current_line or
                    "-ISE" in current_line or "-USE" in current_line or "-CSE" in current_line or
                    "-DSE" in current_line or "-MSE" in current_line or "-VSE" in current_line or
                    "-PSE" in current_line or "-ASE" in current_line or "-KSE" in current_line or
                    "-TSE" in current_line or "-SSE" in current_line or "-HSE" in current_line or
                    "-LSE" in current_line or "-NYSE" in current_line or "-NASDAQ" in current_line or
                    "-LSE" in current_line or "-TSE" in current_line or "-HKSE" in current_line or
                    "-SGX" in current_line or "-ASX" in current_line or "-TSX" in current_line or
                    "-FSE" in current_line or "-XETRA" in current_line or "-Euronext" in current_line or
                    "-LSE" in current_line or "-TSE" in current_line or "-HKSE" in current_line or
                    "-SGX" in current_line or "-ASX" in current_line or "-TSX" in current_line or
                    "-FSE" in current_line or "-XETRA" in current_line or "-Euronext" in current_line):
                    # Extract fund name from the current line
                    potential_fund_name = current_line.strip()
                    
                    # Handle fund names with hyphens
                    if "-" in potential_fund_name:
                        # Take everything after the first hyphen
                        fund_name = potential_fund_name.split("-", 1)[1].strip()
                        # Trim any trailing hyphens and spaces
                        fund_name = fund_name.rstrip("- ").strip()
                    else:
                        fund_name = potential_fund_name.strip()
                    
                    # Remove "Registrar : CAMS" if present
                    if "Registrar : CAMS" in fund_name:
                        fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                    
                    print(f"Extracted fund_name from current line (ISIN on next line): {fund_name}")
                    
                    # Extract ISIN from next line
                    isin_part = next_line.replace("ISIN:", "").strip()
                    isin_match = re.search(r'INF[A-Z0-9]{9}', isin_part)
                    if isin_match:
                        isin = isin_match.group(0)
                        return fund_name, isin, start_idx + 1
                else:
                    # If the current line doesn't look like a fund name, try to extract from the next line
                    # But first check if the next line starts with "(Non-Demat)" or similar
                    if next_line.startswith("(Non-Demat)") or next_line.startswith("(Demat)") or next_line.startswith("(Physical)"):
                        # Extract fund name from the current line
                        potential_fund_name = current_line.strip()
                        
                        # Handle fund names with hyphens
                        if "-" in potential_fund_name:
                            # Take everything after the first hyphen
                            fund_name = potential_fund_name.split("-", 1)[1].strip()
                            # Trim any trailing hyphens and spaces
                            fund_name = fund_name.rstrip("- ").strip()
                        else:
                            fund_name = potential_fund_name.strip()
                        
                        # Remove "Registrar : CAMS" if present
                        if "Registrar : CAMS" in fund_name:
                            fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                        
                        print(f"Extracted fund_name from current line (Non-Demat on next line): {fund_name}")
                        
                        # Extract ISIN from next line
                        isin_part = next_line.replace("ISIN:", "").strip()
                        isin_match = re.search(r'INF[A-Z0-9]{9}', isin_part)
                        if isin_match:
                            isin = isin_match.group(0)
                            return fund_name, isin, start_idx + 1
                    else:
                        # Try to extract from the next line
                        isin_parts = next_line.split("ISIN:")
                        if len(isin_parts) > 1:
                            potential_fund_name = isin_parts[0].strip()
                            
                            # Handle fund names with hyphens
                            if "-" in potential_fund_name:
                                # Take everything after the first hyphen
                                fund_name = potential_fund_name.split("-", 1)[1].strip()
                                # Trim any trailing hyphens and spaces
                                fund_name = fund_name.rstrip("- ").strip()
                            else:
                                fund_name = potential_fund_name.strip()
                            
                            # Remove "Registrar : CAMS" if present
                            if "Registrar : CAMS" in fund_name:
                                fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                            
                            print(f"Extracted fund_name from next line: {fund_name}")
                            
                            # Extract ISIN from next line
                            isin_part = isin_parts[1].strip()
                            isin_match = re.search(r'INF[A-Z0-9]{9}', isin_part)
                            if isin_match:
                                isin = isin_match.group(0)
                                return fund_name, isin, start_idx + 1
            
            # Check if the next line contains a pattern that looks like the rest of an ISIN
            # (9 alphanumeric characters, possibly followed by advisor info)
            isin_rest_match = re.search(r'([A-Z0-9]{9})', next_line)
            if isin_rest_match and "INF" in current_line:
                isin_rest = isin_rest_match.group(1)
                isin = f"INF{isin_rest}"
                print(f"Found split ISIN across lines: {isin}")
                return fund_name, isin, start_idx + 1
        
        # If still not found, try a more aggressive approach
        # Look for any line containing INF followed by 9 alphanumeric characters
        for i in range(start_idx, min(start_idx + 3, len(lines))):
            line = lines[i].strip()
            if "ISIN:" in line:
                isin_parts = line.split("ISIN:")
                if len(isin_parts) > 1:
                    potential_fund_name = isin_parts[0].strip()
                    
                    # Handle fund names with hyphens
                    if "-" in potential_fund_name:
                        # Take everything after the first hyphen
                        fund_name = potential_fund_name.split("-", 1)[1].strip()
                        # Trim any trailing hyphens and spaces
                        fund_name = fund_name.rstrip("- ").strip()
                    else:
                        fund_name = potential_fund_name.strip()
                    
                    # Remove "Registrar : CAMS" if present
                    if "Registrar : CAMS" in fund_name:
                        fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                    
                    print(f"Extracted fund_name from aggressive search: {fund_name}")
                    
                    isin_match = re.search(r'INF[A-Z0-9]{9}', isin_parts[1])
                    if isin_match:
                        isin = isin_match.group(0)
                        return fund_name, isin, i
        
        return fund_name, isin, start_idx

    def write_to_csv(self, csv_file_name=None):
        if csv_file_name is None:
            csv_file_name = f'CAMS_data_{datetime.now().strftime("%d_%m_%Y_%H_%M")}.csv'
        # Get the fieldnames from the Item dataclass
        fieldnames = [
            field.name for field in _FundDetails.__dataclass_fields__.values()
        ]

        # Write the list of dataclass objects to the CSV file
        with open(csv_file_name, mode="w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            # Write the header
            writer.writeheader()

            # Write the data
            for item in self.alldata:
                d = {}
                for f in fieldnames:
                    d[f] = getattr(item, f)
                writer.writerow(d)

        print(f'CSV file "{csv_file_name}" created successfully.')

    def process(self):
        if not self.alllines:
            return
        folio_num = ""
        fund_name = ""
        isin = ""
        
        # Debug: Print the first 20 lines to understand the format
        print("First 20 lines of the PDF text:")
        for i, line in enumerate(self.alllines[:20]):
            print(f"Line {i}: {line.strip()}")
        
        i = 0
        while i < len(self.alllines):
            eachline = self.alllines[i]
            # Debug: Print each line being processed
            # print(f"Processing line {i}: {eachline.strip()}")
            
            m = re.match(FOLIO_PAN, eachline)
            if m:
                folio_num = m.groupdict().get("folio_num")
                print(f"Found folio_num: {folio_num}")
                i += 1
                continue

            # Try the regex pattern first
            m = re.match(FNAME_ISIN, eachline)
            if m:
                fund_name = m.groupdict().get("fund_name")
                isin = m.groupdict().get("isin")
                
                # Remove "Registrar : CAMS" if present
                if "Registrar : CAMS" in fund_name:
                    fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                
                print(f"Regex match - Found fund_name: {fund_name}")
                print(f"Regex match - Found isin: {isin}")
                i += 1
                continue
            
            # Try the multi-line extraction function
            if "ISIN:" in eachline or (i + 1 < len(self.alllines) and "ISIN:" in self.alllines[i + 1]):
                print(f"Attempting multi-line extraction starting at line {i}")
                extracted_fund_name, extracted_isin, new_idx = self.extract_fund_and_isin(self.alllines, i)
                if extracted_fund_name and extracted_isin:
                    fund_name = extracted_fund_name
                    isin = extracted_isin
                    print(f"Multi-line extraction - Found fund_name: {fund_name}")
                    print(f"Multi-line extraction - Found isin: {isin}")
                    i = new_idx + 1
                    continue
            
            # Check for split ISIN codes (INF on one line, rest on next line)
            if "INF" in eachline and "ISIN:" in eachline and i + 1 < len(self.alllines):
                next_line = self.alllines[i + 1].strip()
                print(f"Checking for split ISIN - Current line: {eachline.strip()}")
                print(f"Checking for split ISIN - Next line: {next_line}")
                
                # Look for a pattern that might be the rest of the ISIN (9 alphanumeric characters)
                isin_rest_match = re.search(r'([A-Z0-9]{9})', next_line)
                if isin_rest_match:
                    # Extract fund name - everything before "ISIN:"
                    isin_parts = eachline.split("ISIN:")
                    if len(isin_parts) > 1:
                        potential_fund_name = isin_parts[0].strip()
                        
                        # Handle complex fund names with multiple hyphens
                        if "-" in potential_fund_name:
                            # Try to extract the most meaningful part of the fund name
                            # First, try to get everything after the last hyphen
                            fund_name = potential_fund_name.split("-")[-1].strip()
                            
                            # If the fund name is too short or doesn't look right, try a different approach
                            if len(fund_name) < 5 or fund_name.startswith("("):
                                # Try to get everything after the first hyphen
                                parts = potential_fund_name.split("-", 1)
                                if len(parts) > 1:
                                    fund_name = parts[1].strip()
                        else:
                            fund_name = potential_fund_name
                        
                        # Remove "Registrar : CAMS" if present
                        if "Registrar : CAMS" in fund_name:
                            fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                        
                        print(f"Extracted fund_name: {fund_name}")
                    
                    # Combine INF with the rest of the ISIN
                    isin_rest = isin_rest_match.group(1)
                    isin = f"INF{isin_rest}"
                    print(f"Found split ISIN across lines: {isin}")
                    print(f"Found fund_name: {fund_name}")
                    i += 1  # Skip the next line since we've processed it
                    continue
            
            # Special case: Fund name on current line, ISIN on next line
            if i + 1 < len(self.alllines) and "ISIN:" in self.alllines[i + 1]:
                current_line = eachline.strip()
                next_line = self.alllines[i + 1].strip()
                
                # Check if the current line might contain a fund name
                # Look for patterns like "PAMP-" or other fund name indicators
                if ("PAMP-" in current_line or "-Growth" in current_line or "-Direct" in current_line or 
                    "-Regular" in current_line or "-Plan" in current_line or "-Fund" in current_line or
                    "-HDFC" in current_line or "-ICICI" in current_line or "-SBI" in current_line or
                    "-Axis" in current_line or "-Kotak" in current_line or "-Nippon" in current_line or
                    "-Tata" in current_line or "-UTI" in current_line or "-Aditya" in current_line or
                    "-Mirae" in current_line or "-Parag" in current_line or "-Edelweiss" in current_line or
                    "-DSP" in current_line or "-Invesco" in current_line or "-PGIM" in current_line or
                    "-HSBC" in current_line or "-BNP" in current_line or "-Franklin" in current_line or
                    "-IDFC" in current_line or "-Reliance" in current_line or "-L&T" in current_line or
                    "-Mahindra" in current_line or "-Canara" in current_line or "-Indiabulls" in current_line or
                    "-Motilal" in current_line or "-Quantum" in current_line or "-Sundaram" in current_line or
                    "-Taurus" in current_line or "-JM" in current_line or "-Principal" in current_line or
                    "-Baroda" in current_line or "-LIC" in current_line or "-BOI" in current_line or
                    "-Union" in current_line or "-IDBI" in current_line or "-IIFL" in current_line or
                    "-PPFAS" in current_line or "-WhiteOak" in current_line or "-Samco" in current_line or
                    "-Groww" in current_line or "-KFintech" in current_line or "-CAMS" in current_line or
                    "-Karvy" in current_line or "-NSDL" in current_line or "-CDSL" in current_line or
                    "-SEBI" in current_line or "-AMFI" in current_line or "-RBI" in current_line or
                    "-NSE" in current_line or "-BSE" in current_line or "-MCX" in current_line or
                    "-NCDEX" in current_line or "-MCX-SX" in current_line or "-OTCEI" in current_line or
                    "-ISE" in current_line or "-USE" in current_line or "-CSE" in current_line or
                    "-DSE" in current_line or "-MSE" in current_line or "-VSE" in current_line or
                    "-PSE" in current_line or "-ASE" in current_line or "-KSE" in current_line or
                    "-TSE" in current_line or "-SSE" in current_line or "-HSE" in current_line or
                    "-LSE" in current_line or "-NYSE" in current_line or "-NASDAQ" in current_line or
                    "-LSE" in current_line or "-TSE" in current_line or "-HKSE" in current_line or
                    "-SGX" in current_line or "-ASX" in current_line or "-TSX" in current_line or
                    "-FSE" in current_line or "-XETRA" in current_line or "-Euronext" in current_line or
                    "-LSE" in current_line or "-TSE" in current_line or "-HKSE" in current_line or
                    "-SGX" in current_line or "-ASX" in current_line or "-TSX" in current_line or
                    "-FSE" in current_line or "-XETRA" in current_line or "-Euronext" in current_line):
                    # Extract fund name from the current line
                    potential_fund_name = current_line.strip()
                    
                    # Handle fund names with hyphens
                    if "-" in potential_fund_name:
                        # Take everything after the first hyphen
                        fund_name = potential_fund_name.split("-", 1)[1].strip()
                        # Trim any trailing hyphens and spaces
                        fund_name = fund_name.rstrip("- ").strip()
                    else:
                        fund_name = potential_fund_name.strip()
                    
                    # Remove "Registrar : CAMS" if present
                    if "Registrar : CAMS" in fund_name:
                        fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                    
                    print(f"Special case - Extracted fund_name from current line: {fund_name}")
                    
                    # Extract ISIN from the next line
                    isin_part = next_line.replace("ISIN:", "").strip()
                    isin_match = re.search(r'INF[A-Z0-9]{9}', isin_part)
                    if isin_match:
                        isin = isin_match.group(0)
                        print(f"Special case - Found isin: {isin}")
                        i += 1  # Skip the next line since we've processed it
                        continue
                
                # Special case: Check if the next line starts with "(Non-Demat)" or similar
                if next_line.startswith("(Non-Demat)") or next_line.startswith("(Demat)") or next_line.startswith("(Physical)"):
                    # Extract fund name from the current line
                    potential_fund_name = current_line.strip()
                    
                    # Handle fund names with hyphens
                    if "-" in potential_fund_name:
                        # Take everything after the first hyphen
                        fund_name = potential_fund_name.split("-", 1)[1].strip()
                        # Trim any trailing hyphens and spaces
                        fund_name = fund_name.rstrip("- ").strip()
                    else:
                        fund_name = potential_fund_name.strip()
                    
                    # Remove "Registrar : CAMS" if present
                    if "Registrar : CAMS" in fund_name:
                        fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                    
                    print(f"Special case (Non-Demat) - Extracted fund_name from current line: {fund_name}")
                    
                    # Extract ISIN from the next line
                    isin_part = next_line.replace("ISIN:", "").strip()
                    isin_match = re.search(r'INF[A-Z0-9]{9}', isin_part)
                    if isin_match:
                        isin = isin_match.group(0)
                        print(f"Special case (Non-Demat) - Found isin: {isin}")
                        i += 1  # Skip the next line since we've processed it
                        continue
            
            # Fallback: Look for lines containing "ISIN:" and extract manually
            if "ISIN:" in eachline:
                print(f"Found line with ISIN: {eachline.strip()}")
                
                # Try to extract fund name - everything before "ISIN:"
                isin_parts = eachline.split("ISIN:")
                if len(isin_parts) > 1:
                    # Extract fund name - take everything after the last hyphen if it exists
                    potential_fund_name = isin_parts[0].strip()
                    if "-" in potential_fund_name:
                        fund_name = potential_fund_name.split("-")[-1].strip()
                    else:
                        fund_name = potential_fund_name
                    
                    # Remove "Registrar : CAMS" if present
                    if "Registrar : CAMS" in fund_name:
                        fund_name = fund_name.replace("Registrar : CAMS", "").strip()
                    
                    # Extract ISIN - look for INF followed by 9 alphanumeric characters
                    isin_part = isin_parts[1].strip()
                    isin_match = re.search(r'INF[A-Z0-9]{9}', isin_part)
                    if isin_match:
                        isin = isin_match.group(0)
                    else:
                        # Check if the line ends with "INF" or contains "INF" followed by something else
                        if isin_part.endswith("INF") or "INF" in isin_part:
                            # The ISIN might be split across lines
                            # Check the next line for the rest of the ISIN
                            if i + 1 < len(self.alllines):
                                next_line = self.alllines[i + 1].strip()
                                print(f"Found 'INF' in current line, checking next line: {next_line}")
                                
                                # Look for a pattern that might be the rest of the ISIN (9 alphanumeric characters)
                                isin_rest_match = re.search(r'([A-Z0-9]{9})', next_line)
                                if isin_rest_match:
                                    isin_rest = isin_rest_match.group(1)
                                    isin = f"INF{isin_rest}"
                                    print(f"Found split ISIN: {isin}")
                                    i += 1  # Skip the next line since we've processed it
                    
                    print(f"Manual extraction - Found fund_name: {fund_name}")
                    print(f"Manual extraction - Found isin: {isin}")
                i += 1
                continue
                
            # Process transaction lines
            m = re.match(REGULAR_BUY_TXN, eachline)
            if m:
                date = m.groupdict().get("date")
                txn = "Buy"
                amount = m.groupdict().get("amount")
                units = m.groupdict().get("units")
                nav = m.groupdict().get("nav")
                balance_units = m.groupdict().get("unitbalance")

                # date_format = "%d-%b-%Y"  # Specify the format of the input date string
                # # Convert the string to a datetime object
                # date_obj = datetime.strptime(date, date_format)

                t = _FundDetails(
                    folio_num=folio_num,
                    fund_name=fund_name,
                    isin=isin,
                    scheme_code=self.lnav.get_sch_code(isin),
                    date=date,
                    txn=txn,
                    amount=amount,
                    units=units,
                    nav=float(nav),
                    balance_units=float(balance_units),
                )
                self.alldata.append(t)
                i += 1
                continue

            m = re.match(REGULAR_SELL_TXN, eachline)
            if m:
                date = m.groupdict().get("date")
                txn = "Sell"
                amount = m.groupdict().get("amount")
                amtstring = re.sub(r"\(|\)", "", amount)
                units = m.groupdict().get("units")
                unitstring = re.sub(r"\(|\)", "", units)
                nav = m.groupdict().get("nav")
                balance_units = m.groupdict().get("unitbalance")

                # date_format = "%d-%b-%Y"  # Specify the format of the input date string
                # # Convert the string to a datetime object
                # date_obj = datetime.strptime(date, date_format)
                t = _FundDetails(
                    folio_num=folio_num,
                    fund_name=fund_name,
                    isin=isin,
                    scheme_code=self.lnav.get_sch_code(isin),
                    date=date,
                    txn=txn,
                    amount=float(amtstring),
                    units=float(unitstring),
                    nav=float(nav),
                    balance_units=float(balance_units),
                )
                self.alldata.append(t)
                i += 1
                continue

            m = re.match(SEGR_BUY_TXN, eachline)
            if m:
                date = m.groupdict().get("date")
                txn = "Buy"
                amount = "0"
                units = m.groupdict().get("units")
                nav = "0"
                balance_units = m.groupdict().get("unitbalance")

                # date_format = "%d-%b-%Y"  # Specify the format of the input date string
                # # Convert the string to a datetime object
                # date_obj = datetime.strptime(date, date_format)
                t = _FundDetails(
                    folio_num=folio_num,
                    fund_name=fund_name,
                    isin=isin,
                    scheme_code=self.lnav.get_sch_code(isin),
                    date=date,
                    txn=txn,
                    amount=amount,
                    units=units,
                    nav=float(nav),
                    balance_units=float(balance_units),
                )
                self.alldata.append(t)
                i += 1
                continue
                
            # If we get here, we didn't match any pattern
            i += 1


class ProcessPDF:
    def __init__(self, filename, password) -> None:
        self.filename = filename
        self.password = password
        self.alldata = []

    def get_pdf_data(self, output_format="csv"):
        file_path = self.filename
        doc_pwd = self.password
        final_text = ""
        print("Processing PDF. Please wait...")
        try:
            with pdfplumber.open(file_path, password=doc_pwd) as pdf:
                for i in range(len(pdf.pages)):
                    txt = pdf.pages[i].extract_text()
                    final_text = final_text + "\n" + txt
                pdf.close()
            # Replace all occurrences of ',' with an empty string
            final_text = final_text.replace(",", "")
            # print("Text found, writing to file")
            # with open("text.txt", "w+") as f:
            #     f.write(final_text)
            # self.extract_text(final_text)
            format_specifiers = ["dicts", "csv", "json", "df"]
            if output_format not in format_specifiers:
                raise Exception(
                    f"Error!! Output format can be one of {','.join(format_specifiers)}"
                )
            pt = _ProcessTextFile(alllines=final_text.splitlines())

            if output_format == "csv":
                pt.write_to_csv()
            else:
                item_dicts = [asdict(item) for item in pt.alldata]
                if output_format == "df":
                    # Convert the list of dictionaries to a DataFrame
                    df = pd.DataFrame(item_dicts)
                    return df
                elif output_format == "json":
                    json_string = json.dumps(item_dicts)
                    return json_string
                else:
                    return item_dicts
        except Exception as ex:
            print(ex)
            traceback.print_exc()

    def process(self, pdf_text):
        """Process the PDF text and extract transaction data"""
        lines = pdf_text.split('\n')
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Debug print for the first 20 lines
            if i < 20:
                print(f"Line {i}: {line}")
            
            # Try to match FOLIO_PAN pattern
            folio_match = re.match(FOLIO_PAN, line)
            if folio_match:
                self.folio = folio_match.group('folio_num')
                self.pan = folio_match.group('pan')
                print(f"Found folio: {self.folio}, pan: {self.pan}")
                i += 1
                continue
            
            # Try to match FNAME_ISIN pattern
            fname_match = re.match(FNAME_ISIN, line)
            if fname_match:
                self.fund_name = fname_match.group('fund_name')
                self.isin = fname_match.group('isin')
                print(f"Found fund_name: {self.fund_name}, isin: {self.isin}")
                i += 1
                continue
            
            # If regex fails, try manual extraction
            if "ISIN:" in line:
                parts = line.split("ISIN:")
                if len(parts) > 1:
                    # Extract fund name - take everything after the last hyphen if it exists
                    potential_fund_name = parts[0].strip()
                    
                    # Handle complex fund names with multiple hyphens
                    if "-" in potential_fund_name:
                        # Try to extract the most meaningful part of the fund name
                        # First, try to get everything after the last hyphen
                        self.fund_name = potential_fund_name.split("-")[-1].strip()
                        
                        # If the fund name is too short or doesn't look right, try a different approach
                        if len(self.fund_name) < 5 or self.fund_name.startswith("("):
                            # Try to get everything after the first hyphen
                            parts = potential_fund_name.split("-", 1)
                            if len(parts) > 1:
                                self.fund_name = parts[1].strip()
                    else:
                        self.fund_name = potential_fund_name
                    
                    print(f"Extracted fund_name: {self.fund_name}")
                    
                    # Check for ISIN in the same line or next line
                    isin_part = parts[1].strip()
                    isin_match = re.search(r'INF[A-Z0-9]{9}', isin_part)
                    if isin_match:
                        self.isin = isin_match.group(0)
                    elif isin_part.endswith("INF") or "INF" in isin_part:
                        # Check next line for ISIN remainder
                        if i + 1 < len(lines):
                            next_line = lines[i + 1].strip()
                            isin_rest_match = re.search(r'([A-Z0-9]{9})', next_line)
                            if isin_rest_match:
                                self.isin = f"INF{isin_rest_match.group(1)}"
                                i += 1  # Skip the next line since we've processed it
                    
                    print(f"Found fund_name: {self.fund_name}, isin: {self.isin}")
                    i += 1
                    continue
            
            # Process transaction lines
            # ... rest of the transaction processing code ...
            i += 1
