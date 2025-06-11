from nlds_processors.catalog.catalog import Catalog
from nlds_processors.monitor.monitor import Monitor
import nlds.server_config as CFG
from nlds.nlds_setup import CONFIG_FILE_LOCATION

import minio

import os

def connect_to_monitor(settings: str = CONFIG_FILE_LOCATION):
    """Connects to the monitor database"""
    config = CFG.load_config(settings)
    db_engine = config["monitor_q"]["db_engine"]
    db_options = config["monitor_q"]["db_options"]
    db_options["echo"] = False

    nlds_monitor = Monitor(db_engine=db_engine, db_options=db_options)
    nlds_monitor.connect(create_db_fl=False)
    nlds_monitor.start_session()

    return nlds_monitor


def connect_to_catalog(settings: str = CONFIG_FILE_LOCATION):
    config = CFG.load_config(settings)

    db_engine = config["catalog_q"]["db_engine"]
    db_options = config["catalog_q"]["db_options"]
    db_options["echo"] = False
    nlds_cat = Catalog(db_engine=db_engine, db_options=db_options)
    nlds_cat.connect(create_db_fl=False)
    return nlds_cat


def connect_to_s3(settings: str = CONFIG_FILE_LOCATION):
    # get the tenancy from the server config
    config = CFG.load_config(settings)
    access_key = config["cronjob_publisher"]["access_key"]
    secret_key = config["cronjob_publisher"]["secret_key"]
    tenancy = config["cronjob_publisher"]["tenancy"]
    client = minio.Minio(
        tenancy,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
    print(f"Connected to {tenancy}")
    return client

if __name__ == "__main__":
    config_file_path = os.path.expanduser(
        "~/Coding/nlds-hacking/server_config_production"
    )

#    print(connect_to_monitor(settings=config_file_path))
#    print(connect_to_catalog(settings=config_file_path))
    client=(connect_to_s3(settings=config_file_path))
    lb = client.list_buckets()
    for x in lb:
        print(x)
