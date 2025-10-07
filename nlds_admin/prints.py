import click
from datetime import datetime

from nlds_admin.rabbit.state import State


def integer_permissions_to_string(intperm):
    octal = oct(intperm)[2:]
    result = ""
    value_letters = [(4, "r"), (2, "w"), (1, "x")]
    # Iterate over each of the digits in octal
    for digit in [int(n) for n in str(octal)]:
        # Check for each of the permissions values
        for value, letter in value_letters:
            if digit >= value:
                result += letter
                digit -= value
            else:
                result += "-"
    return result


def pretty_size(size):
    """Returns file size in human readable format"""

    suffixes = [
        ("B", 1),
        ("K", 1000),
        ("M", 1000000),
        ("G", 1000000000),
        ("T", 1000000000000),
    ]
    level_up_factor = 2000.0
    for suf, multipler in suffixes:
        if float(size) / multipler > level_up_factor:
            continue
        else:
            return round(size / float(multipler), 2).__str__() + suf
    return round(size / float(multipler), 2).__str__() + suf


def _tags_to_str(tags):
    tags_str = ""
    for t in tags:
        tags_str += f"{t} : {tags[t]}\n{'':22}"
    return tags_str


def get_transaction_state(transaction: dict):
    """Get the overall state of a transaction in a more convienent form by
    querying the sub-transactions and determining if the overall transaction
    is complete.
    transaction: a dictionary for a single transaction.  Note that
      monitor_transactions returns a dictionary of transactions
    Transaction dictionary looks like this:
    {
        'id': 2,
        'transaction_id': 'a06ec7b3-e83c-4ac7-97d8-2545a0b8d317',
        'user': 'nrmassey',
        'group': 'cedaproc',
        'api_action': 'getlist',
        'creation_time': '2022-12-06T15:45:43',
        'sub_records': [
            {
                'id': 2,
                'sub_id': '007075b2-8c79-4cfa-a1e5-0aaa65892454',
                'state': 'COMPLETE',
                'last_updated': '2022-12-06T15:45:44',
                'failed_files': []
            }
        ]
    }

    possible values of state are:
        INITIALISING = -1
        ROUTING = 0
        SPLITTING = 1
        INDEXING = 2
        CATALOG_PUTTING = 3
        TRANSFER_PUTTING = 4
        CATALOG_GETTING = 10
        ARCHIVE_GETTING = 11
        TRANSFER_GETTING = 12
        TRANSFER_INIT = 13
        ARCHIVE_INIT = 20
        ARCHIVE_PUTTING = 21
        ARCHIVE_PREPARING = 22
        CATALOG_DELETING = 30
        CATALOG_UPDATING = 31
        CATALOG_ARCHIVE_UPDATING = 32
        CATALOG_REMOVING = 33
        COMPLETE = 100
        FAILED = 101
        COMPLETE_WITH_ERRORS = 102
        COMPLETE_WITH_WARNINGS = 103
        SPLIT = 110
        SEARCHING = 1000
    The overall state is the minimum of these
    """
    state_mapping = {
        "INITIALISING": -1,
        "ROUTING": 0,
        "SPLITTING": 1,
        "INDEXING": 2,
        "CATALOG_PUTTING": 3,
        "TRANSFER_PUTTING": 4,
        "CATALOG_GETTING": 10,
        "ARCHIVE_GETTING": 11,
        "TRANSFER_GETTING": 12,
        "TRANSFER_INIT": 13,
        "ARCHIVE_INIT": 20,
        "ARCHIVE_PUTTING": 21,
        "ARCHIVE_PREPARING": 22,
        "CATALOG_DELETING": 30,
        "CATALOG_UPDATING": 31,
        "CATALOG_ARCHIVE_UPDATING": 32,
        "CATALOG_REMOVING": 33,
        "COMPLETE": 100,
        "FAILED": 101,
        "COMPLETE_WITH_ERRORS": 102,
        "COMPLETE_WITH_WARNINGS": 103,
        "SPLIT": 110,
        "SEARCHING": 1000,
    }
    state_mapping_reverse = {v: k for k, v in state_mapping.items()}

    min_state = 200
    min_time = datetime(1970, 1, 1)
    error_count = 0
    for sr in transaction["sub_records"]:
        sr_state = sr["state"]
        d = datetime.fromisoformat(sr["last_updated"])
        if d > min_time:
            min_time = d
        if state_mapping[sr_state] < min_state:
            min_state = state_mapping[sr_state]
        if sr_state == "FAILED":
            error_count += 1

    if min_state == 200:
        return None, None

    if min_state == state_mapping["COMPLETE"] and error_count > 0:
        min_state = state_mapping["COMPLETE_WITH_ERRORS"]

    # see if any warnings were given
    warning_count = 0
    if "warnings" in transaction:
        warning_count = len(transaction["warnings"])
    if min_state == state_mapping["COMPLETE"] and warning_count > 0:
        min_state = state_mapping["COMPLETE_WITH_WARNINGS"]

    return state_mapping_reverse[min_state], min_time


def _get_url_from_file(f):
    url = None
    for s in f["locations"]:
        if s["storage_type"] == "OBJECT_STORAGE":
            url = s["url"]
    return url


def print_single_list(response: dict):
    h = response[0]
    click.echo(f"{'':<4}{'user':<16}: {h['user']}")
    click.echo(f"{'':<4}{'group':<16}: {h['group']}")
    click.echo(f"{'':<4}{'id':<16}: {h['id']}")
    click.echo(f"{'':<4}{'label':<16}: {h['label']}")
    click.echo(f"{'':<4}{'ingest time':<16}: {h['date'].replace('T',' ')[0:19]}")
    if "transactions" in h:
        trans_str = ""
        for t in h["transactions"]:
            trans_str += t + f"\n{'':<22}"
        click.echo(f"{'':<4}{'transaction id':<16}: {trans_str[:-23]}")
    if "tags" in h and len(h["tags"]) > 0:
        tags_str = _tags_to_str(h["tags"])
        click.echo(f"{'':<4}{'tags':<16}: {tags_str[:-23]}")


def print_multi_list(response: dict):
    for h in response:
        click.echo(
            f"{'':<4}{h['user']:<16}{h['group']:<16}"
            f"{h['id']:<6}{h['label']:<32}{h['date'].replace('T',' ')[0:19]:<32}"
        )


def print_single_file(response, print_url=False):
    """Print (full) details of one file"""
    # NRM - note: still using loops over dictionary keys as its
    # 1. easier than trying to just use the first key
    # 2. a bit more robust - in case more than one file matches, for example, in
    # separate holdings
    for hkey in response:
        h = response[hkey]
        for tkey in h["transactions"]:
            t = h["transactions"][tkey]
            time = t["ingest_time"].replace("T", " ")
            for f in t["filelist"]:
                click.echo(f"{'':<4}{'path':<16}: {f['original_path']}")
                click.echo(f"{'':<4}{'type':<16}: {f['path_type']}")
                if f["link_path"]:
                    click.echo(f"{'':<4}{'link path':<16}: {f['link_path']}")
                size = pretty_size(f["size"])
                click.echo(f"{'':<4}{'size':<16}: {size}")
                click.echo(f"{'':<4}{'user uid':<16}: {f['user']}")
                click.echo(f"{'':<4}{'group gid':<16}: {f['group']}")
                click.echo(
                    f"{'':<4}{'permissions':<16}: "
                    f"{integer_permissions_to_string(f['permissions'])}"
                )
                click.echo(f"{'':<4}{'ingest time':<16}: {time[0:19]}")
                # locations
                stls = " "
                url = _get_url_from_file(f)
                for s in f["locations"]:
                    stls += s["storage_type"] + ", "

                click.echo(f"{'':<4}{'storage location':<16}:{stls[0:-2]}")
                if url is not None and print_url:
                    click.echo(f"{'':<4}{'url':<16}: {url}")


def print_simple_file(response, print_url=False):
    for hkey in response:
        h = response[hkey]
        for tkey in h["transactions"]:
            t = h["transactions"][tkey]
            for f in t["filelist"]:
                url = _get_url_from_file(f)
                if print_url and url:
                    click.echo(url)
                else:
                    click.echo(f"{f['original_path']}")


def print_multi_file(response, print_url):
    for hkey in response:
        h = response[hkey]
        for tkey in h["transactions"]:
            t = h["transactions"][tkey]
            time = t["ingest_time"].replace("T", " ")
            for f in t["filelist"]:
                size = pretty_size(f["size"])
                url = _get_url_from_file(f)
                if url and print_url:
                    path_print = _get_url_from_file(f)
                else:
                    path_print = f["original_path"]
                click.echo(
                    f"{'':4}{h['user']:<16}"
                    f"{h['holding_id']:<6}{h['label']:<16}"
                    f"{size:<8}{time[:11]:<12}{path_print}"
                )


def print_single_stat(response: dict):
    """Print a single status in more detail, with a list of failed files if
    necessary"""
    # still looping over the keys, just in case more than one state returned
    for tr in response:
        state, _ = get_transaction_state(tr)
        if state == None:
            continue
        click.echo(f"{'':<4}{'id':<16}: {tr['id']}")
        click.echo(f"{'':<4}{'user':<16}: {tr['user']}")
        click.echo(f"{'':<4}{'group':<16}: {tr['group']}")
        click.echo(f"{'':<4}{'action':<16}: {tr['api_action']}")
        click.echo(f"{'':<4}{'transaction id':<16}: {tr['transaction_id']}")
        if "label" in tr:
            click.echo(f"{'':<4}{'label':<16}: {tr['label']}")
        click.echo(
            f"{'':<4}{'creation time':<16}: {(tr['creation_time']).replace('T',' ')}"
        )
        click.echo(f"{'':<4}{'state':<16}: {state}")
        if "warnings" in tr:
            warn_str = ""
            for w in tr["warnings"]:
                warn_str += w + f"\n{'':<22}"
            click.echo(f"{'':<4}{'warnings':<16}: {warn_str[:-23]}")

        click.echo(f"{'':<4}{'sub records':<16}->")
        for sr in tr["sub_records"]:
            click.echo(f"{'':4}{'+':<4} {'id':<13}: {sr['id']}")
            click.echo(f"{'':<9}{'sub_id':<13}: {sr['sub_id']}")
            click.echo(f"{'':<9}{'state':<13}: {sr['state']}")
            click.echo(
                f"{'':<9}{'last update':<13}: {(sr['last_updated']).replace('T',' ')}"
            )

            if len(sr["failed_files"]) > 0:
                click.echo(f"{'':<9}{'failed files':<13}->")
                for ff in sr["failed_files"]:
                    click.echo(f"{'':<9}{'+':<4} {'filepath':<8} : {ff['filepath']}")
                    click.echo(f"{'':<9}{'':>4} {'reason':<8} : {ff['reason']}")


def print_multi_stat(response: dict):
    """Print a multi-line set of status"""
    for tr in response:
        state, time = get_transaction_state(tr)
        if state == None:
            continue
        time = time.isoformat().replace("T", " ")
        if "label" in tr:
            label = tr["label"]
        else:
            label = ""
        if "job_label" in tr and tr["job_label"]:
            job_label = tr["job_label"]
        else:
            job_label = ""  # tr['transaction_id'][0:8]
        click.echo(
            f"{'':<4}{tr['user'][:15]:<16}{tr['group'][:15]:<16}"
            f"{str(tr['id'])[:11]:<12}{tr['api_action'][:15]:<16}{job_label[:15]:16}"
            f"{label[:15]:16}{state[:22]:<23}{time[:19]:<20}"
        )


def construct_header_string(details, meta, time, simple=False, url=False):
    """
    Constructs a string based on the inputs and prints the response.
    """
    header = []

    if details.get("user_query"):
        header.append(f"user: {details['user_query']}")
    elif details.get("groupall") or details["user"] == "nlds":
        header.append("All users")
    else:
        header.append(f"user: {details['user']}")

    if details.get("group"):
        header.append(f"group: {details['group']}")

    if simple:
        header.append("simple view")
    elif url:
        header.append("url view")

    if details.get("id"):
        header.append(f"id: {details['id']}")
    if details.get("transaction_id"):
        header.append(f"transaction_id: {details['transaction_id']}")
    if details.get("job_label"):
        header.append(f"job_label: {details['job_label']}")
    if details.get("state"):
        state = State(details["state"]).name
        header.append(f"state: {state}")
    if details.get("sub_id"):
        header.append(f"sub_id: {details['sub_id']}")
    if details.get("label"):
        header.append(f"label: {details['label']}")
    if details.get("holding_id"):
        header.append(f"holding_id: {details['holding_id']}")
    if details.get("tag"):
        header.append(f"tag: {details['tag']}")
    if details.get("path"):
        header.append(f"path: {details['path']}")
    if meta.get("api_action"):
        header.append(f"api action: {meta['api_action']}")

    req_details = ", ".join(header)

    # Add order to the request details
    if time:
        req_details += ", order: descending"
    else:
        req_details += ", order: ascending"

    return req_details


def print_table_headers(api_action):
    """
    Prints table headers for multi-holding cases.
    """
    common = f"{'':<4}{'user':<16}{'group':<16}"
    headers = {
        "list": f"{common}{'id':<6}{'label':<32}{'ingest time':<32}",
        "find": f"{common}{'h-id':<6}{'h-label':<16}{'size':<8}{'date':<12}{'path'}",
        "stat": (
            f"{common}{'id':<12}{'action':<16}{'job label':<16}"
            f"{'label':<16}{'state':<23}{'last update':<20}"
        ),
    }
    click.echo(headers.get(api_action, ""))


def print_action(response: dict, req_details, req_meta, time, simple=False, url=None):
    header = construct_header_string(req_details, req_meta, time)
    api_action = req_details["api_action"]

    if simple:
        print_simple_file(response, url)
        return

    n_holdings = len(response)
    if n_holdings == 0:
        click.echo(f"No transactions found for {header}")
        return

    if n_holdings == 1:
        action_messages = {
            "list": "Listing holding for",
            "find": "Listing files for holding",
            "stat": "State of transaction for",
        }
        click.echo(f"{action_messages.get(api_action)} {header}")

        action_functions = {
            "list": print_single_list,
            "find": lambda res: print_single_file(res, url),
            "stat": print_single_stat,
        }
        action_functions[api_action](response)
        return

    action_messages = {
        "list": "Listing holdings for",
        "find": "Listing files for holdings",
        "stat": "State of transactions for",
    }
    click.echo(f"{action_messages.get(api_action)} {header}")

    print_table_headers(api_action)

    action_functions = {
        "list": print_multi_list,
        "find": lambda res: print_multi_file(res, url),
        "stat": print_multi_stat,
    }
    action_functions[api_action](response)
