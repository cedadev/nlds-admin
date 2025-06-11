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

from nlds_processors.catalog.catalog import Catalog
from nlds_processors.catalog.catalog_models import Storage, File, Location
from nlds.details import PathDetails
from connect import connect_to_catalog, connect_to_s3


def _remove_location_from_file(
    file: File,
    location: Location,
    loc_type: Storage,
    force: bool,
    delete: bool,
    s3_client,
    nlds_cat: Catalog,
    debug: bool,
):
    delloc = (
        location.url_scheme == "" and location.url_netloc == "" and location.root == ""
    ) or force
    if location.storage_type == loc_type:
        if delloc:
            if delete and s3_client is not None:
                pd = PathDetails.from_filemodel(file)
                # delete from object storage - if not in debug mode
                if not debug:
                    s3_client.remove_object(pd.bucket_name, pd.object_name)
                click.echo(f"Deleted object: {pd.get_object_store().url}")
            # delete from catalog - if not in debug mode
            if not debug:
                nlds_cat.delete_location(file, loc_type)
            click.echo(f"Removed {loc_type} location for {file.original_path}")
            if loc_type == Storage.TAPE and location.aggregation_id is not None:
                agg = nlds_cat.get_aggregation(location.aggregation_id)
                if not debug:
                    nlds_cat.delete_aggregation(agg)
                click.echo(f"Removed TAPE aggregation for {file.original_path}")
        else:
            click.echo(
                f"Location URL details not empty for the file {file.original_path} and "
                f"force not set in command line options.  Skipping."
            )
    if not debug:
        nlds_cat.save()


@click.command()
@click.option(
    "-u", "--user", default=None, type=str, help="The username to reset holdings for."
)
@click.option(
    "-g", "--group", default=None, type=str, help="The group to reset holdings for."
)
@click.option(
    "-i",
    "--holding_id",
    default=None,
    type=int,
    help="The numeric id of the holding to reset Storage Location entries for.",
)
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    help="Force the deletion of the Storage Location record",
)
@click.option(
    "-d",
    "--delete",
    default=False,
    is_flag=True,
    help="Delete the associated object(s) from the object storage",
)
@click.option(
    "-l",
    "--location",
    default=None,
    type=str,
    help="Storage Location type to delete records for.  OBJECT_STORAGE|TAPE",
)
@click.option(
    "-L",
    "--limit",
    default=None,
    type=int,
    help=("Limit the number reset so that a sub-set can be fetched from tape, and a "
          "sub-set fetched directly from object store"),
)
@click.option(
    "-S",
    "--settings",
    default="",
    type=str,
    help="The location of the settings file for NLDS.",
)
@click.option(
    "-D",
    "--debug",
    default=False,
    is_flag=True,
    help="Run in debug / test mode.  No commits to the database are made"
)
def reset_storage_status(
    user: str,
    group: str,
    holding_id: int,
    force: bool,
    delete: bool,
    location: str,
    limit: int,
    settings: str,
    debug: bool,
) -> None:
    """Reset the tape status of a file by deleting a STORAGE LOCATION associated
    with a file, if the details in the STORAGE LOCATION are empty.
    """
    if user is None:
        raise click.UsageError("Error - user not specified")
    if group is None:
        raise click.UsageError("Error - group not specified")
    if holding_id is None:
        raise click.UsageError("Error - holding id not specified")
    if location is None:
        raise click.UsageError("Error - location not specified")
    else:
        if location == "OBJECT_STORAGE":
            loc_type = Storage.OBJECT_STORAGE
        elif location == "TAPE":
            loc_type = Storage.TAPE
        else:
            raise click.UsageError(
                f"Error - unknown location type {location}.  Choices are OBJECT_STORAGE"
                " or TAPE"
            )

    # only need to contact S3 if deleting from object storage
    if delete and loc_type == Storage.OBJECT_STORAGE:
        s3_client = connect_to_s3(settings)
    else:
        s3_client = None

    if settings != "":
        nlds_cat = connect_to_catalog(settings)
    else:
        nlds_cat = connect_to_catalog()

    nlds_cat.start_session()
    holding = nlds_cat.get_holding(user=user, group=group, holding_id=holding_id)[0]

    # get the locations
    for t in holding.transactions:
        if not limit:
            this_limit = len(t.files)
        else:
            this_limit = limit
        for f in t.files[:this_limit]:
            for l in f.locations:
                # first check whether a deletion will leave no locations left
                if len(f.locations) == 1 and loc_type == l.storage_type:
                    click.echo(
                        f"Deleting this location would leave no storage locations for "
                        f"the file {f.original_path}.  Skipping."
                    )
                else:
                    _remove_location_from_file(
                        f, l, loc_type, force, delete, s3_client, nlds_cat, debug
                    )

    nlds_cat.end_session()


if __name__ == "__main__":
    reset_storage_status()
