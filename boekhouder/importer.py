import abc
import dataclasses
import logging
import os
from decimal import Decimal
from typing import Iterator

import beancount.core.data
from beancount.core.data import Posting
import polars as pl
import re2
from beancount.core.amount import Amount

from boekhouder.data import StrFilter, XlTransaction


class Handler:
    """A handler converts a transaction to beancount"""
    HANDLERS = {}
    replacements = {}

    def __init__(self, yaml):
        self.replacements = yaml.get("replace", {})
        pass

    def __init_subclass__(cls, **kwargs):
        try:
            name = kwargs["name"]
        except KeyError:
            pass
        else:
            cls.HANDLERS[name] = cls

    def handle(self, repo, txn: XlTransaction) -> beancount.core.data.Transaction:
        """Default handler for transaction conversion. Simply transfers money from the default source account to the
        bank account"""
        meta = {
            "reference": txn.reference,
        }
        txn = dataclasses.replace(txn, **self.replacements)
        tags = set()
        return beancount.core.data.Transaction(
            date=txn.booking_date,
            meta=meta,
            flag="?" if type(self) is Handler else "!",
            payee=txn.counterparty_name,
            narration=txn.message,
            tags=tags,
            links=set(),
            postings=[
                Posting(
                    account=repo.get_account(txn.account),
                    units=Amount(txn.amount, txn.currency),
                    cost=None,
                    price=None,
                    meta={},
                    flag=None,
                ),
                Posting(
                    account=f"{self.source_type(txn)}:{self.default_source(txn)}",
                    units=Amount(-txn.amount, txn.currency),
                    cost=None,
                    price=None,
                    meta={},
                    flag=None,
                )
            ]
        )

    def source_type(self, txn: XlTransaction) -> str:
        return "Income" if txn.amount > 0 else "Expenses"

    def default_source(self, txn: XlTransaction) -> str:
        return "UnaccountedFunds"


Handler.HANDLERS[None] = Handler
DEFAULT_HANDLER = Handler({})


class Categorize(Handler, name="categorize"):
    def __init__(self, yaml):
        super().__init__(yaml)
        self.category = yaml["category"]

    def default_source(self, txn: XlTransaction) -> str:
        return self.category


class Membership(Handler, name="membership"):
    POS_1 = Decimal(1)

    def __init__(self, yaml):
        super().__init__(yaml)
        self.member = yaml["member"]
        self.monthly_cost = Decimal(yaml.get("monthly_cost", "25.00")).quantize(DEC_2)

    def handle(self, repo, txn: XlTransaction) -> beancount.core.data.Transaction:
        if self.monthly_cost != txn.amount:
            return DEFAULT_HANDLER.handle(repo, txn)
        new_txn = super(self.__class__, self).handle(repo, txn)
        new_txn.tags.add("membership")
        new_txn.postings.append(Posting(
            account="Assets:Membership",
            units=Amount(self.POS_1, "MEMBERSHIP_MONTH"),
            cost=None,
            price=None,
            meta={},
            flag=None,
        ))
        new_txn.postings.append(Posting(
            account=f"Liabilities:Members:{self.member}",
            units=Amount(-self.POS_1, "MEMBERSHIP_MONTH"),
            cost=None,
            price=None,
            meta={},
            flag=None,
        ))
        return new_txn

    def default_source(self, txn: XlTransaction) -> str:
        return "Membership"


class Filter:
    name: list[StrFilter]
    kind: list[StrFilter]
    message: list[StrFilter]
    account: list[StrFilter]
    handler: Handler

    def __init__(self, yaml):
        self.name = self.parse_filter(yaml.get("name", []))
        self.kind = self.parse_filter(yaml.get("kind", []))
        self.message = self.parse_filter(yaml.get("message", []))
        self.account = self.parse_filter(yaml.get("account", []))
        self.handler = Handler.HANDLERS[yaml.get("handler", None)](yaml)

    @staticmethod
    def parse_filter(entries):
        if isinstance(entries, list):
            return entries
        return [entries]

    @staticmethod
    def _test1(field: str, pattern: list[StrFilter]) -> bool:
        if len(pattern) == 0:
            return True
        for item in pattern:
            if type(item) is str and item == field:
                return True
            if type(item) is re2._Regexp and item.match(field):
                return True
        return False

    def test(self, txn: "XlTransaction"):
        return all([
            self._test1(txn.message, self.message),
            self._test1(txn.counterparty_account, self.account),
            self._test1(txn.counterparty_name, self.name),
            self._test1(txn.description, self.kind)
        ])


def import_file(fname):
    _, ext = os.path.splitext(fname)
    if ext == ".xlsx":
        logging.info(f"Importing file as XLSX: {fname}")
        return import_xl(fname)
    else:
        logging.warning(f"Skipping {fname} ({ext})")


DEC_2 = Decimal("0.00")


def import_xl(fname: str):
    log = logging.getLogger("boekhouder.import.import_xl")
    # Load as XLSX
    fname = os.path.join(fname)
    ws = pl.read_excel(
        fname,
        sheet_name="Verrichtingen",
        engine="calamine",
        read_options={"header_row": 0},
        schema_overrides={"Bedrag": pl.Decimal(scale=2)}
    )
    # doc = fastexcel.read_excel(fname)
    # doc = openpyxl.load_workbook(fname, read_only=True, data_only=True)
    # ws = doc.load_sheet("Verrichtingen", header_row=1).to_polars()
    # compute headers
    id = lambda x: x
    to_date = lambda x: x.date()
    to_decimal = lambda x: Decimal.from_float(x).quantize(DEC_2)
    col_assq = {
        "Rekening": ("account", id),
        "Boekdatum": ("booking_date", id),
        "Valutadatum": ("value_date", id),
        "Referentie": ("reference", id),
        "Beschrijving": ("description", id),
        "Bedrag": ("amount", to_decimal),
        "Munt": ("currency", id),
        "Verrichtingsdatum": ("value_date", id),
        "Rekening tegenpartij": ("counterparty_account", id),
        "Naam tegenpartij": ("counterparty_name", id),
        "Mededeling": ("message", id),
    }
    col_actions = []
    for (col, col_head) in enumerate(ws.columns):
        try:
            field, xform = col_assq[col_head]

            col_actions.append((col, lambda txn, value, f=field, xf=xform: setattr(txn, f, xf(value))))
        except KeyError:
            log.warning(f"Unknown column {col_head} in {fname}")
    xl_txns = []
    nrows = len(ws)
    for (i, row) in enumerate(ws.iter_rows(named=False)):
        txn = XlTransaction()
        for col, action in col_actions:
            action(txn, row[col])
        xl_txns.append(txn)
    return xl_txns