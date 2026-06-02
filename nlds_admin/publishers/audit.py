# encoding: utf-8
"""
audit.py
"""

__author__ = "Neil Massey"
__date__ = "13 May 2026"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional
from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds_admin.publishers.list import list_holdings
from nlds_admin.publishers.find import find_files
from nlds_admin.common.deserialize import deserialize
from nlds_admin.rabbit import message_keys as MSG


def audit_holding(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    label: Optional[str] = None,
):
    # need user and group
    if not user:
        raise RuntimeError("User is required to perform an audit.")
    if not group:
        raise RuntimeError("Group is required to perform an audit.")

    # need one of id, transaction id or job_label
    if not (id or transaction_id or label):
        raise RuntimeError(
            "One of the following is required to perform an audit: "
            "--id (-i), --transaction_id (-n), --label (-l)"
        )
    # get the singular holding with the id, label or transaction_id
    ret = list_holdings(
        rpc_publisher=rpc_publisher,
        user=user,
        group=group,
        groupall=False,
        label=label,
        holding_id=id,
        transaction_id=transaction_id,
    )
    # need to deserialise the return
    json_response = deserialize(ret)
    # get the (singular) holding and then the transactions
    holding = json_response[MSG.DATA][MSG.HOLDINGS]
    transactions = holding[0][MSG.TRANSACTIONS]
    # for each transaction, get the files in the transaction
    for t in transactions:
        ret = find_files(
            rpc_publisher=rpc_publisher,
            user=user,
            group=group,
            groupall=False,
            transaction_id=t,
        )
        # need to deserialise the return
        json_response = deserialize(ret)
        # get the holding, transactions, files
        t_holding = json_response[MSG.DATA][MSG.HOLDINGS]
        # bit of munging to get the first holding (and only, hopefully!)
        holding = t_holding[list(t_holding.keys())[0]]
        # get the first and only transaction, using the same munging
        t_transaction = holding[MSG.TRANSACTIONS]
        transaction = t_transaction[list(t_transaction.keys())[0]]
        files = transaction[MSG.FILELIST]
        for f in files:
            print(f["original_path"], f["size"])
