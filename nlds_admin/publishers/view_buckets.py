# encoding: utf-8
"""
view_buckets.py
"""

__author__ = "Neil Massey"
__date__ = "11 November 2026"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional

import nlds_admin.rabbit.routing_keys as RK
import nlds_admin.rabbit.message_keys as MSG

from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds_admin.common.deserialize import deserialize
from nlds_admin.common.connect import ObjectStoreConnection
from minio.error import S3Error

import click


def get_bucket_name(transaction_id: str) -> str:
    return "nlds." + transaction_id


def decode_policy(policy: dict) -> str:
    """Return a string representation of the policy"""
    statements = policy["Statement"]
    policy = ""
    for S in statements:
        P = S["Principal"]
        A = S["Action"]
        if "group" in P and P["group"]:
            policy += f"Group: {", ".join(P["group"])}: "
        if "user" in P and P["user"]:
            policy += f"User: {", ".join(P["user"])}: "
        policy += ", ".join(A)
        policy += " ; "
    return policy


def make_list_message(
    user: Optional[str] = "",
    group: Optional[str] = "",
    holding_id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    limit: Optional[int] = None,
):
    # create the message dictionary
    msg_dict = {
        MSG.DETAILS: {
            MSG.USER: "nlds",
            MSG.GROUP: "**all**",
            MSG.GROUPALL: False,
            MSG.HOLDING_ID: holding_id,
            MSG.TRANSACT_ID: transaction_id,
            MSG.API_ACTION: RK.LIST,
            MSG.USER_QUERY: user,
            MSG.GROUP_QUERY: group,
        },
        MSG.DATA: {},
        MSG.META: {
            MSG.LIMIT: limit,
            MSG.DESCENDING: True,
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    meta_dict = {}
    if holding_id:
        meta_dict[MSG.HOLDING_ID] = holding_id
    if transaction_id:
        meta_dict[MSG.TRANSACT_ID] = transaction_id
    if len(meta_dict) > 0:
        msg_dict[MSG.META] = meta_dict
    return msg_dict


def view_holding_buckets(
    rpc_publisher: RabbitMQRPCPublisher,
    user: Optional[str] = "",
    group: Optional[str] = "",
    holding_id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    limit: Optional[int] = None,
    policy: Optional[bool] = False,
    json: Optional[bool] = False,
):
    # connect to object store if policy is wanted to output
    if policy:
        s3_client = ObjectStoreConnection()

    msg_dict = make_list_message(user, group, holding_id, transaction_id, limit)
    # call RPC function
    routing_key = f"{RK.CATALOG_Q}"
    response = rpc_publisher.call(msg_dict=msg_dict, routing_key=routing_key)
    # Check if response is valid or whether the request timed out
    if response is not None:
        # convert byte response to str
        response = response.decode()
        json_response = deserialize(response)
        holdings = json_response[MSG.DATA][MSG.HOLDINGS]
        for h in holdings:
            click.echo(f"{'':<1}{h['user']:<16}{h['group']:<16}{h['id']:<6}")
            for t in h[MSG.TRANSACTIONS]:
                # if we want to view the policy then we have to fetch it from the
                # object store
                if policy:
                    try:
                        raw_policy = s3_client.get_bucket_policy(get_bucket_name(t))
                    except S3Error:
                        click.echo(f"{'':<5}{get_bucket_name(t)} No bucket (yet?)")
                    else:
                        click.echo(
                            f"{'':<5}{get_bucket_name(t)} {decode_policy(raw_policy)}"
                        )
                else:
                    click.echo(f"{'':<5}{get_bucket_name(t)}")
    else:
        msg = "Catalog service could not be reached in time."
        raise RuntimeError(msg)
