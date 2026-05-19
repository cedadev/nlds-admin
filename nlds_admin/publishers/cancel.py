# encoding: utf-8
"""
cancel.py
"""

__author__ = "Neil Massey"
__date__ = "13 May 2026"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional, Union
import uuid

import nlds_admin.rabbit.message_keys as MSG
import nlds_admin.rabbit.routing_keys as RK
from nlds_admin.rabbit.state import State
from nlds_admin.deserialize import deserialize

from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher


def cancel_transaction(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    job_label: Optional[str] = None,
):
    # Validate transaction_id is a valid uuid
    if transaction_id is not None:
        try:
            uuid.UUID(transaction_id)
        except ValueError:
            msg = "Given transaction_id not a valid uuid-4."
            raise RuntimeError(msg)

    # Assemble message ready for RCP call
    msg_dict = {
        MSG.DETAILS: {
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.ID: id,
            MSG.API_ACTION: RK.CANCEL,
            MSG.TRANSACT_ID: transaction_id,
            MSG.JOB_LABEL: job_label,
        },
        MSG.DATA: {},
        MSG.META: {},
        MSG.TYPE: MSG.TYPE_STANDARD,
    }

    # call RPC function
    routing_key = RK.NLDS_Q
    response = rpc_publisher.call(msg_dict=msg_dict, routing_key=routing_key)
    # Check if response is valid or whether the request timed out
    if response is not None:
        response = response.decode()
        return response
    else:
        msg = ("NLDS service could not be reached in time.",)
        raise RuntimeError(msg)
