import glob
import logging
import sys
from datetime import datetime

import beancount.parser.printer
import click
import os
import io

import boekhouder.importer
from boekhouder.data import Repository

def bh_args(callback):
    import inspect
    args = inspect.signature(callback)

    @click.option('-d', "--debug", count=True,
                  help="Increase verbosity level. One -v prints details. Two results in excessive prolixity")
    @click.option('-r', '--repo', default=os.getcwd())
    def wrapper(debug, repo, **kwargs):
        if debug == 0:
            level = logging.WARNING
        elif debug == 1:
            level = logging.INFO
        else:
            level = logging.DEBUG
        # Configure logging
        logging.basicConfig(level=level)

        # Create repo
        import inspect

        return callback(**kwargs)
    

@click.group()
def cli(debug):
    if debug:
        print("Debug mode is on.")
    logging.basicConfig(level = logging.DEBUG)
    pass


@cli.command()
def init():
    for dir in [
        "extracts",
        "transactions",
    ]:
        os.makedirs(dir, exist_ok=True)
    open("ledger.beancount", "w+").close()
    # TODO: Generate sample config


@cli.command("import")
@click.option("--verbose", "-v", is_flag=True)
@click.option("--dry-run", "-n", is_flag=True)
def cmd_import(verbose, dry_run):
    repo = Repository()
    repo.load_extracts()

    bc_txns = set(repo.bc_txn_map)
    xl_txns = set(repo.xl_txn_map)
    if bc_txns - xl_txns:
        click.echo("WARNING: Transactions in ledger not found in extracts", err=not verbose)
        if not verbose:
            click.echo("Pass -v for details", err=True)
        else:
            for txnid in bc_txns - xl_txns:
                txn = repo.bc_txn_map[txnid]
                click.echo(f" - {txn.date} - {txnid} {txn.narration})")

    new_txn_ids = xl_txns - bc_txns

    new_txns = []
    if not new_txn_ids:
        click.echo("No new transactions found", err=True)
        return
    processed = set()
    for txn in new_txn_ids:
        txn = repo.xl_txn_map[txn]
        if txn.reference in processed:
            continue
        processed.add(txn.reference)
        for filter in repo.config.get("filters", []):
            if filter.test(txn):
                handler = filter.handler
                break
        else:
            handler = boekhouder.importer.DEFAULT_HANDLER
        new_txns.append(handler.handle(repo, txn))
    new_txns.sort(key=lambda txn: txn.date)
    if dry_run:
        beancount.parser.printer.print_entries(new_txns)
    else:
        date = datetime.today().strftime("%Y-%m-%d")
        for i in range(1,1000):
            base = os.path.join(repo.basedir, "transactions", f"{date}_{i:03d}")
            if not glob.glob(base + "_*"):
                break
        else:
            logging.error("Unable to create a new file for the new records")
            return
        dates = [txn.date for txn in new_txns]

        auto_file = base + "_auto.beancount"
        auto_entries = [txn for txn in new_txns if txn.flag == "!"]
        manual_file = base + "_manual.beancount"
        manual_entries = [txn for txn in new_txns if txn.flag != "!"]
        with open(auto_file, "wt") as f:
            beancount.parser.printer.print_entries(auto_entries, file=f)
        with open(manual_file, "wt") as f:
            beancount.parser.printer.print_entries(manual_entries, file=f)
        repo.scan_beancount()
        click.echo("TODOs:")
        if auto_entries:
            click.echo(f" - check {auto_file} for accuracy")
        if manual_entries:
            click.echo(f" - Fix the items in {manual_file}")

@cli.command()
def validate():
    pass