#! /usr/bin/env python
# encoding: utf-8
"""
reset_tape_status.py

This should be used if a file is marked as TAPE but the copy to tape did not succeed.
This results in url_scheme, url_netloc and root being null strings ("").
These are checked before the TAPE location is removed, unless --force option is
supplied.
"""
__author__ = "Neil Massey"
__date__ = "24 Sep 2024"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import click

from nlds_processors.catalog.catalog import Catalog, CatalogError
from nlds_processors.catalog.catalog_models import (
    Transaction,
)
from nlds_processors.monitor.monitor_models import TransactionRecord, SubRecord

from nlds_admin.rabbit.state import State
from connect import connect_to_catalog, connect_to_monitor

from sqlalchemy.exc import MultipleResultsFound


def merge_sub_record(target, source):
    if source.state.value > target.state.value:
        target.state = source.state

    failed_file_paths = [ff.filepath for ff in target.failed_files]
    for f in source.failed_files:
        if not f.filepath in failed_file_paths:
            print(f"    Adding failed file {f.filepath}")
            f.sub_record_id = target.id
            target.failed_files.add(f)
        else:
            print(f"    Failed file already in SubRecord {f.filepath}")


def merge_sub_records(target, sources):
    """Merge a list of SubRecords from sources into target"""
    # get the max state
    for msr in sources:
        merge_sub_record(target, msr)

def check_monitor_transaction_sub_records(nlds_mon, transaction, fix):
    """
    This checks whether a transaction has multiple sub records that have the same
    sub_id.
    The fix is to merge the sub records by picking the highest state and appending any
    failed files.
    """
    for sr in transaction.sub_records:
        # get the sub_record from the transaction and sub_id, these will cause a
        # MultipleResultsFound exception
        try:
            sub_rec = nlds_mon.get_sub_record(transaction, sr.sub_id)
        except MultipleResultsFound:
            multi_sub_recs_q = (
                nlds_mon.session.query(SubRecord)
                .filter(SubRecord.transaction_record_id == transaction.id)
                .filter(SubRecord.sub_id == sr.sub_id)
            )
            print(
                f"Multiple SubRecords with sub_id: {sr.sub_id} found for "
                f"TransactionRecord with transaction_id {transaction.transaction_id}, "
                f"number : {multi_sub_recs_q.count()}"
            )
            # the fixes are below here
            if not fix:
                return
            # merge the sub records
            merge_sub_records(multi_sub_recs_q[0], multi_sub_recs_q[1:])
            # delete the sub records
            for del_msr in multi_sub_recs_q[1:]:
                nlds_mon.session.delete(del_msr)


def check_monitor_transaction_sub_records_consistency(nlds_mon, transaction, fix):
    print(transaction.id, len(transaction.sub_records))
    for sr in transaction.sub_records:
        if sr.transaction_record_id != transaction.id:
            print("Inconsistent sub_id")

def check_monitor_transaction_records(nlds_mon, transaction, fix):
    """Check and merge monitor transactions"""
    try:
        test_tr = nlds_mon.get_transaction_record(
            user=transaction.user,
            group=transaction.group,
            transaction_id=transaction.transaction_id,
        )
    except MultipleResultsFound as e:
        multi_tr_q = nlds_mon.session.query(TransactionRecord).filter(
            TransactionRecord.transaction_id == transaction.transaction_id
        )
        print(
            f"Multiple transactions found for {transaction.transaction_id}, "
            f"number : {multi_tr_q.count()}"
        )
        if not fix:
            return
        # get the list of sub_ids for the first TransactionRecord
        sub_ids = [sr.sub_id for sr in multi_tr_q[0].sub_records]
        # Loop over the transactions and merge the sub records into the first one
        for mtr in multi_tr_q[1:]:
            # merge the sub records
            for msr in mtr.sub_records:
                if msr.sub_id in sub_ids:
                    # merge into appropriate sub_record
                    for sr in multi_tr_q[0].sub_records:
                        if msr.sub_id == sr.sub_id:
                            merge_sub_record(sr, msr)
                            print(f"  Merged SubRecords: {sr}, {msr}")
                else:
                    # add as not present
                    msr.transaction_record_id = multi_tr_q[0].id
                    multi_tr_q[0].sub_records.add(msr)
                    print(f"  Appended SubRecord {msr}")
                # delete the failed files in the sub record
                for ff in msr.failed_files:
                    nlds_mon.session.delete(ff)
                # delete the sub record
                nlds_mon.session.delete(msr)
            # delete the transaction record
            nlds_mon.session.delete(mtr)
            


def check_monitor_transactions(settings, id=None, fix=False):
    print("+----+ Check monitor transactions +----+")
    nlds_mon = connect_to_monitor(settings)
    nlds_mon.start_session()
    transactions = nlds_mon.get_transaction_records(
        user="**all**",
        group="**all**",
    )
    # two possible errors for transactions:
    # 1. Each TransactionRecord contains multiple SubRecords with the same sub_id
    #     Fix first by amalgamating SubRecords.  Merge the FAILED_FILES.  Take the
    #       highest state.
    # 2. More than one TransactionRecord exists for each transaction_id
    #     Fix by merging SubRecords from the subsequent TransactionRecords into the
    #       first TransactionRecord.
    #     If the SubRecord already exists then take the highest state and merge the
    #       FAILED_FILES.

    # 1. Multiple SubRecords
    for tr in transactions:
        if id is not None and tr.id != id:
            continue
        check_monitor_transaction_sub_records(nlds_mon, tr, fix)

    # commit those changes if fix is selected
    if fix:
        nlds_mon.session.commit()

    # 2. TransactionRecords with same transaction_id
    for tr in transactions:
        if id is not None and tr.id != id:
            continue
        check_monitor_transaction_records(nlds_mon, tr, fix)

    if fix:
        nlds_mon.session.commit()

    # 3. Check the consistency in the sub_ids for the transactions
    for tr in transactions:
        if id is not None and tr.id != id:
            continue
        check_monitor_transaction_sub_records_consistency(nlds_mon, tr, fix)

    nlds_mon.end_session()


def check_catalog_holding_labels(settings, id=None, fix=False):
    print("+----+ Check catalog holding labels +----+")
    nlds_cat = connect_to_catalog(settings)
    nlds_cat.start_session()
    holdings = nlds_cat.get_holdings(
        user="**all**",
        group="**all**",
        groupall=False,
    )
    for h in holdings:
        if id is not None and h.id != id:
            continue
        try:
            test_h = nlds_cat.get_holding(user=h.user, group=h.group, label=h.label)
        except MultipleResultsFound:
            print(f"More than one holding found with label: {h.label}")


def check_catalog_transactions(settings, id=None, fix=False):
    print("+----+ Check catalog transactions +----+")
    nlds_cat = connect_to_catalog(settings)
    nlds_cat.start_session()
    holdings = nlds_cat.get_holdings(
        user="**all**",
        group="**all**",
        groupall=False,
    )
    for h in holdings:
        if id is not None and h.id != id:
            continue
        for tr in h.transactions:
            # get the transaction using the transaction id
            try:
                test_tr = nlds_cat.get_transaction(transaction_id=tr.transaction_id)
            except CatalogError as e:
                print(
                    f"{tr.transaction_id} : {e.message}",
                )
                if "Multiple transactions" in e.message:
                    multi_tr_q = nlds_cat.session.query(Transaction).filter(
                        Transaction.transaction_id == tr.transaction_id
                    )
                    for mtr in multi_tr_q:
                        if (len(mtr.files) == 0) and fix:
                            nlds_cat.session.delete(mtr)
                if fix:
                    print("Fixed")
    if fix:
        nlds_cat.save()
    nlds_cat.end_session()


@click.command
@click.option(
    "-d",
    "--database",
    default=None,
    type=str,
    help="The database to check the consistency of : catalog_db|monitor_db.",
)
@click.option(
    "-S",
    "--settings",
    default="",
    type=str,
    help="The location of the settings file for NLDS.",
)
@click.option(
    "-i",
    "--id",
    default=None,
    type=int,
    help="Id of Holding or TransactionRecord to change"
)
@click.option("-T", "--test", default=0, type=int, help="Consistency check to perform")
@click.option("-f", "--fix", default=False, is_flag=True, help="Fix the problem")
def check_db_consistency(database, settings, id, test, fix):
    if database == "monitor_db":
        # monitor consistency checks
        # 0. all
        # 1. check that each transaction is unique
        if test == 0:
            check_monitor_transactions(settings, id, fix)
        elif test == 1:
            check_monitor_transactions(settings, id, fix)
    elif database == "catalog_db":
        # catalog consistency checks
        # 0. all
        # 1. check that each Holding Label is unique for each user / group combo
        # 2. check that each Transaction is unique within the Holding
        # 3. check that each original_path is unique within the Transaction and Holding
        # 4. check that there are not duplicates of the location
        # 5. check that there are not duplicates of the aggregate
        if test == 0:
            check_catalog_holding_labels(settings, id, fix)
            check_catalog_transactions(settings, id, fix)
        elif test == 1:
            check_catalog_holding_labels(settings, id, fix)
        elif test == 2:
            check_catalog_transactions(settings, id, fix)
    else:
        click.echo("Unknown database, choices are: catalog_db | monitor_db")


if __name__ == "__main__":
    check_db_consistency()
