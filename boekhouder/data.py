import logging
import os.path
import sys
from decimal import Decimal
from typing import Union, List, Any

import beancount.loader
import beancount.parser.printer
import re2
import yaml
from beancount.core import data as bc_data
from dataclasses import dataclass
from datetime import date
from . import importer
import openpyxl

StrFilter = Union[str, re2._Regexp]


# Configure regex loader
class ConfigLoader(yaml.SafeLoader):
    pass


def construct_regex(_, node):
    if isinstance(node, yaml.ScalarNode):
        return re2.compile(node.value)


ConfigLoader.add_constructor("!regex", construct_regex)


@dataclass
class XlTransaction:
    account: str = ""
    booking_date: date = date(1970, 1, 1)
    value_date: date = date(1970, 1, 1)
    operation_date: date = date(1970, 1, 1)
    reference: str = ""
    description: str = ""
    amount: Decimal = Decimal("0.00")

    currency: str = "EUR"
    counterparty_account: str = ""
    counterparty_name: str = ""
    message: str = ""


DEC_2 = Decimal("1.00")


class Repository:
    config: dict[str, Any]
    bc_ledger: List[bc_data.Directive]
    bc_txn_map: dict[str, bc_data.Transaction]
    bc_loaded: bool

    xl_txns: list[XlTransaction]
    xl_txn_map: dict[str, XlTransaction]
    xl_loaded: bool

    basedir: str

    def __init__(self, basedir=os.path.curdir):
        self.basedir = basedir
        self.config = {}
        self.load_config()
        # We always want to load the beancount data
        self.scan_beancount()
        ledger_data, errors, options = beancount.loader.load_file(os.path.join(basedir, "ledger.beancount"))
        if errors:
            beancount.parser.printer.print_errors(errors, file=sys.stderr)
            raise Exception("Unable to load beancount file")
        self.bc_ledger = ledger_data
        # Compute transaction map
        self.bc_txn_map = {}
        for directive in self.bc_ledger:
            if type(directive) is not bc_data.Transaction:
                continue
            if "reference" in directive.meta:
                self.bc_txn_map[directive.meta["reference"]] = directive
        self.bc_loaded = True
        self.xl_loaded = False

    def load_config(self):
        fname = os.path.join(self.basedir, "config.yaml")

        with open(fname, "rt") as f:
            config = yaml.load(f, Loader=ConfigLoader)
        config["filters"] = [importer.Filter(f) for f in config.get("filters", [])]
        self.config = config

    def load_extracts(self):
        xl_dir = os.path.join(self.basedir, "extracts")
        logging.info(f"Loading extracts from {xl_dir}")
        self.xl_txns = []
        self.xl_txn_map = {}
        for fname in os.listdir(xl_dir):
            path = os.path.join(xl_dir, fname)
            txns = importer.import_file(path)
            self.xl_txns.extend(txns)
            for txn in txns:
                if txn.reference:
                    self.xl_txn_map[txn.reference] = txn

            # TODO: keep track of date ranges in each file and warn when no overlap

    def get_account(self, iban):
        return self.config["accounts"][iban]

    def scan_beancount(self):
        txn_dir = os.path.join(self.basedir, "transactions")
        with open(os.path.join(self.basedir, "ledger.beancount"), "wt") as f:
            for name in sorted(os.listdir(txn_dir)):
                print(f'include "transactions/{name}"', file=f)

