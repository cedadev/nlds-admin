from typing import Optional
import click

import nlds_admin.rabbit.routing_keys as RK
import nlds_admin.rabbit.message_keys as MSG

import nlds_admin.common.config as CFG

from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds_admin.common.deserialize import deserialize
from nlds_admin.common.connect import ObjectStoreConnection
from nlds_admin.publishers.view_buckets import (
    make_list_message,
    get_bucket_name,
    decode_policy,
)

import grp
import copy

OBJECT_STORE_ACCESS_POLICY_CONFIG = "object_store_access_policy"
NLDS_USER_ACCESS_POLICY_CONFIG = "nlds_user"
GROUP_ACCESS_POLICY_CONFIG = "group"

access_policies = CFG.load_config()[OBJECT_STORE_ACCESS_POLICY_CONFIG]


def _get_linux_group_name(group_name: str, prefix="gws_") -> str:
    """Get a true Linux group name from the passed in group name.
    The Linux group name may not be a straight match but should contain the passed
    in group name."""
    # get all the groups
    groups = [g.gr_name for g in grp.getgrall()]
    for g in groups:
        if group_name in g and prefix in g:
            return g
    return ""

def _create_bucket_policy_template(bucket_name: str):
    bucket_policy = {
        "Version": "2008-10-17",
        "Id": f"{bucket_name} policy",
        "Statement": [],
    }
    return bucket_policy

def _edit_nlds_user_policy(bucket_policy: dict):
    statements = bucket_policy["Statement"]
    for s in statements:
        try:
            principal = s["Principal"]
            if "nlds" in principal["user"]:
                # delete the nlds entry
                statements.remove(s)
        except KeyError:
            # not found - is okay
            pass

    # create the NLDS user statement
    try:
        nlds_statement = access_policies[NLDS_USER_ACCESS_POLICY_CONFIG]
        # edit the user to nlds
        nlds_statement["Principal"]["user"] = ["nlds"]
    except KeyError:
        raise KeyError(
            f"Could not find the {NLDS_USER_ACCESS_POLICY_CONFIG} key "
            f"in the {OBJECT_STORE_ACCESS_POLICY_CONFIG} section of the "
            f"config file"
        )
    statements.append(nlds_statement)

def _edit_group_policy(bucket_policy: dict, old_group: str, new_group: str):
    statements = bucket_policy["Statement"]
    for s in statements:
        try:
            principal = s["Principal"]
            if old_group in principal["group"]:
                # remove the old group
                statements.remove(s)
        except KeyError:
            # not found - is okay, create remains True
            pass

    try:
        group_statement = access_policies[GROUP_ACCESS_POLICY_CONFIG]
        group_statement["Principal"]["group"] = [new_group]
    except KeyError:
        raise KeyrError(
            f"Could not find the {self.GROUP_ACCESS_POLICY_CONFIG} key "
            f"in the {self.OBJECT_STORE_ACCESS_POLICY_CONFIG} section of the "
            f"config file"
        )
    statements.append(group_statement)

def fix_bucket_policy(
    rpc_publisher: RabbitMQRPCPublisher,
    user: Optional[str] = "",
    group: Optional[str] = "",
    holding_id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    limit: Optional[int] = None,
    json: Optional[bool] = False,
):
    # connect to object store
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
            # get which group to use.  This should be the linux / LDAP group
            group = h['group']
            use_group = _get_linux_group_name(group)
            # default to the passed in group
            if not use_group:
                use_group = group

            click.echo(f"{'':<1}{h['user']:<16}{group:<16}{h['id']:<6}")

            for t in h[MSG.TRANSACTIONS]:
                # get the policy fro the bucket
                policy = s3_client.get_bucket_policy(get_bucket_name(t))
                # copy or create then edit the policy
                if policy == {}:
                    new_policy = _create_bucket_policy_template(bucket_name)
                else:
                    new_policy = copy.copy(policy)

                # edit the user policy
                _edit_nlds_user_policy(new_policy)
                # edit the group policy
                _edit_group_policy(new_policy, group, use_group)

                print(f"{'':<5}Edited: {get_bucket_name(t)} {decode_policy(new_policy)}")
                #s3_client.set_bucket_policy(new_policy)
    else:
        msg = "Catalog service could not be reached in time."
        raise RuntimeError(msg)
