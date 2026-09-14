"""Tests for IDCW payout/reinvest recognition and narration preservation.

Uses synthetic lines built from the standard CAMS/KFintech CAS layout
(Date  Transaction  Amount  Units  Price  Unit Balance), since no real
statement with an actual dividend row was available to validate against.
The narration phrasing ("Dividend @ Rs. X per unit" / "... Reinvestment ...")
follows the convention documented by codereverser/casparser, an established
open-source CAS parser.
"""

import unittest
from unittest.mock import patch

import processCASpdf


class _DummyNav:
    """Stand-in for _LatestNav that skips the AMFI network fetch."""

    def get_sch_code(self, isin):
        return ""


def _process(lines):
    with patch.object(processCASpdf, "_LatestNav", return_value=_DummyNav()):
        pt = processCASpdf._ProcessTextFile(alllines=lines)
    return pt.alldata


HEADER = [
    "Folio No: 12345678 / 90 PAN: ABCDE1234F",
    "SCHM-Sample Fund - Direct Plan - Growth - ISIN: INF000A01234",
]


class TestIDCWRecognition(unittest.TestCase):
    def test_regular_buy_unaffected(self):
        rows = _process([*HEADER, "01-Apr-2024 Purchase Online 5000.00 45.678 109.4500 45.678"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].txn, "Buy")
        self.assertEqual(rows[0].narration, "01-Apr-2024 Purchase Online 5000.00 45.678 109.4500 45.678")

    def test_regular_sell_unaffected(self):
        rows = _process([*HEADER, "01-May-2024 Redemption (5000.00) (45.678) 109.4500 0.000"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].txn, "Sell")

    def test_idcw_reinvest(self):
        line = "01-Jun-2024 IDCW Reinvestment @ Rs. 2.50 per unit 500.00 4.545 110.0000 50.223"
        rows = _process([*HEADER, line])
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.txn, "IDCW_REINVEST")
        self.assertEqual(row.amount, 500.00)
        self.assertEqual(row.units, 4.545)
        self.assertEqual(row.nav, 110.0000)
        self.assertEqual(row.balance_units, 50.223)
        self.assertEqual(row.narration, line)

    def test_idcw_payout(self):
        line = "01-Jul-2024 Dividend Payout @ Rs. 2.50 per unit 500.00"
        rows = _process([*HEADER, line])
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.txn, "IDCW_PAYOUT")
        self.assertEqual(row.amount, 500.00)
        self.assertEqual(row.units, 0.0)
        self.assertEqual(row.nav, 0.0)
        self.assertEqual(row.balance_units, 0.0)
        self.assertEqual(row.narration, line)

    def test_stamp_duty_and_stt_still_skipped(self):
        rows = _process(
            [
                *HEADER,
                "01-Apr-2024 *** Stamp Duty *** 0.25",
                "01-May-2024 *** STT Paid *** 0.05",
            ]
        )
        self.assertEqual(rows, [])

    def test_segregated_buy_unaffected(self):
        rows = _process([*HEADER, "01-Aug-2024 Segregated Portfolio Allotment 12.345 62.577"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].txn, "Buy")
        self.assertEqual(rows[0].narration, "01-Aug-2024 Segregated Portfolio Allotment 12.345 62.577")

    def test_all_rows_carry_narration(self):
        lines = [
            *HEADER,
            "01-Apr-2024 Purchase Online 5000.00 45.678 109.4500 45.678",
            "01-May-2024 Redemption (5000.00) (45.678) 109.4500 0.000",
        ]
        rows = _process(lines)
        for row in rows:
            self.assertTrue(row.narration)


if __name__ == "__main__":
    unittest.main()
