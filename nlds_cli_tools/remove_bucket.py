#! /usr/bin/env python

import click
from connect import connect_to_s3

@click.command()
@click.option(
    "-b", 
    "--bucket", 
    default=None, 
    type=str, 
    help="The bucket to delete",
)
@click.option(
    "-f",
    "--force",
    default=False,
    is_flag=True,
    help="Force the deletion of the Storage Location record",
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
    help="Run in debug / test mode.  No objects or buckets are removed."
)
def remove_bucket(
    bucket: str,
    force: bool,
    settings: str,
    debug: bool
):
    client = connect_to_s3(settings=settings)
    # get the bucket
    for o in client.list_objects(bucket, recursive=True):
        if not debug:
            client.remove_object(o.bucket_name, o.object_name)
        click.echo(f"Deleted object: {o.bucket_name}/{o.object_name}")

    if not debug:
        client.remove_bucket(bucket)
    click.echo(f"Deleted bucket: {bucket}")

if __name__ == "__main__":
    remove_bucket()