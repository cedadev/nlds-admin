# encoding: utf-8
"""
status.py
"""

__author__ = "Neil Massey"
__date__ = "24 Feb 2025"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional, Union
import uuid

import nlds_admin.rabbit.message_keys as MSG
import nlds_admin.rabbit.routing_keys as RK
from nlds_admin.rabbit.state import State
from nlds_admin.deserialize import deserialize

from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher


def get_request_status(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    groupall: Optional[bool] = False,
    id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    job_label: Optional[str] = None,
    state: Optional[list[str]] = None,
    sub_id: Optional[str] = None,
    api_action: Optional[list[str]] = None,
    exclude_api_action: Optional[list[str]] = None,
    query_user: Optional[str] = None,
    query_group: Optional[str] = None,
    limit: Optional[int] = None,
    descending: Optional[bool] = False,
):
    # Validate state at this point.
    for s in state:
        # Attempt to convert to int, if can't then put in upper case for name
        # comparison
        try:
            s = int(s)
        except (ValueError, TypeError):
            s = s.upper()

        if State.has_name(s):
            s = State[s].value
        elif State.has_value(s):
            s = State(s).value
        else:
            msg = f"Given State {s} not valid."
            raise RuntimeError(msg)

    # Validate transaction_id is a valid uuid
    if transaction_id is not None:
        try:
            uuid.UUID(transaction_id)
        except ValueError:
            msg = "Given transaction_id not a valid uuid-4."
            raise RuntimeError(msg)
    # Validate sub_id is a valid uuid
    if sub_id is not None:
        try:
            uuid.UUID(sub_id)
        except ValueError:
            msg = "Given sub_id not a valid uuid-4."
            raise RuntimeError(msg)

    # Assemble message ready for RCP call
    msg_dict = {
        MSG.DETAILS: {
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.GROUPALL: groupall,
            MSG.ID: id,
            MSG.API_ACTION: RK.STAT,
            MSG.TRANSACT_ID: transaction_id,
            MSG.JOB_LABEL: job_label,
            MSG.SUB_ID: sub_id,
            MSG.USER_QUERY: query_user,
            MSG.GROUP_QUERY: query_group,
        },
        MSG.DATA: {},
        MSG.META: {
            MSG.LIMIT: limit,
            MSG.DESCENDING: descending,
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    if len(api_action) > 0:
        msg_dict[MSG.META][MSG.API_ACTION] = api_action
    if len(exclude_api_action) > 0:
        msg_dict[MSG.META][MSG.EXCLUDE_API_ACTION] = exclude_api_action
    if len(state) > 0:
        msg_dict[MSG.META][MSG.STATE] = state

    # call RPC function
    routing_key = RK.MONITOR_Q
    response = rpc_publisher.call(msg_dict=msg_dict, routing_key=routing_key)
    # Check if response is valid or whether t   he request timed out
    if response is not None:
        # convert byte response to dict for label fetching
        response_dict = deserialize(response)
        # Attempt to get list of transaction records
        transaction_records = None
        try:
            transaction_records = response_dict[MSG.DATA][MSG.RECORD_LIST]
        except KeyError as e:
            msg = (
                f"Encountered error when trying to get a record list from the"
                f" message response ({e})"
            )
            raise RuntimeError(msg)

        transaction_response = None
        # Only continue if the response actually had any transactions in it
        if transaction_records is not None and len(transaction_records) > 0:
            routing_key = RK.CATALOG_Q
            transaction_response = rpc_publisher.call(
                msg_dict=response_dict, routing_key=routing_key
            )

        if transaction_response is not None:
            response = transaction_response

        # convert byte response to str
        response = response.decode()
        return response
    else:
        msg = ("Monitoring service could not be reached in time.",)
        raise RuntimeError(msg)
