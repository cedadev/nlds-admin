# encoding: utf-8
"""
untape.py
"""

__author__ = "Neil Massey"
__date__ = "28 September 2026"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional

from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds_admin.rabbit.publisher import RabbitMQPublisher
from nlds_admin.publishers.unstage import get_files_from_holding
from nlds_admin.publishers.fix_tape_records import send_archive_remove_message


def untape_holding(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    holding_id: int,
    transaction_id: Optional[str] = None,
    limit=1000,
    max_per_message=1000,
    json: Optional[bool] = False,
):
    # error check - need to supply user, group, id and / or transaction
    # need user and group
    if not user or not group:
        raise RuntimeError("User and group are required to perform an untape.")

    # need one of holding id or transaction id
    if not (holding_id or transaction_id):
        raise RuntimeError(
            "One of the following is required to perform a untape: holding_id, "
            "transaction_id."
        )

    files_dict = get_files_from_holding(
        rpc_publisher=rpc_publisher,
        user=user,
        group=group,
        holding_id=holding_id,
        transaction_id=transaction_id,
        limit=limit,
    )

    rabbit_publisher = RabbitMQPublisher()
    rabbit_publisher.get_connection()

    for tr in files_dict:
        filelist = files_dict[tr]
        fl_len = len(filelist)
        if len(filelist) == 0:
            continue
        for i in range(0, fl_len, max_per_message):
            slc = slice(i, min(i + max_per_message, fl_len))
            sub_filelist = filelist[slc]
            send_archive_remove_message(
                rabbit_publisher=rabbit_publisher,
                user=user,
                group=group,
                holding_id=holding_id,
                transaction_id=tr,
                filelist=sub_filelist,
            )

    rabbit_publisher.close_connection()
