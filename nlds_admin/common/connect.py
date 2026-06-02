import nlds_admin.common.config as CFG
import nlds_admin.rabbit.routing_keys as RK
import minio


def connect_to_object_store(settings: str = CFG.CONFIG_FILE_LOCATION):
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
    return client
