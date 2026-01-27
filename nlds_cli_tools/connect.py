from nlds_processors.catalog.catalog import Catalog
from nlds_processors.monitor.monitor import Monitor
import nlds.server_config as CFG
from nlds.nlds_setup import CONFIG_FILE_LOCATION

from sqlalchemy import text

import minio
import time

import os

def connect_to_monitor(settings: str = CONFIG_FILE_LOCATION):
    """Connects to the monitor database"""
    config = CFG.load_config(settings)
    db_engine = config["monitor_q"]["db_engine"]
    db_options = config["monitor_q"]["db_options"]
    db_options["echo"] = False

    nlds_monitor = Monitor(db_engine=db_engine, db_options=db_options)
    nlds_monitor.connect(create_db_fl=False)
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

def get_sql_statement():
    SQL = """
    SELECT EXISTS (select badger.id
    from (select file.id, file.original_path
    from file,transaction
    where file.transaction_id = transaction.id
    and transaction.holding_id = '36') as badger
    where badger.original_path =
    '/gws/pw/j07/ncas_obs_vol1/iao/raw_data/ncas-sunphotometer-3/incoming/Spectra/20211119/2021-11-19_08-18-00_SSIM_Spectrum_SN155.csv')
    """
    return text(SQL)

if __name__ == "__main__":
#    config_file_path = os.path.expanduser(
#        "~/Coding/nlds-hacking/server_config_production"
#    )
#
#    print(connect_to_monitor(settings=config_file_path))
    config_file_path = os.path.expanduser(
        "/etc/nlds/server_config_production"
    )
    nlds_cat = connect_to_catalog(settings=config_file_path)
    nlds_cat.start_session()
    sql = get_sql_statement()
    s = time.perf_counter()
    x = nlds_cat.session.execute(sql)
    e = time.perf_counter()
    print(e-s)
    nlds_cat.end_session()
#    client=(connect_to_s3(settings=config_file_path))
#    lb = client.list_buckets()
#    for x in lb:
#        print(x)
