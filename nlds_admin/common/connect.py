import nlds_admin.common.config as CFG
import minio
import json


class ObjectStoreConnection:

    def __init__(self, settings=CFG.CONFIG_FILE_LOCATION):
        # get the tenancy from the server config
        config = CFG.load_config(settings)
        access_key = config["cronjob_publisher"]["access_key"]
        secret_key = config["cronjob_publisher"]["secret_key"]
        tenancy = config["cronjob_publisher"]["tenancy"]
        self.client = minio.Minio(
            tenancy, access_key=access_key, secret_key=secret_key, secure=False
        )

    def list_buckets(self) -> list:
        return self.client.list_buckets()

    def list_objects(self, bucket: str) -> list:
        return self.client.list_objects(bucket)

    def get_bucket_policy(self, bucket: str) -> dict:
        policy_raw = self.client.get_bucket_policy(bucket)
        policy = json.loads(policy_raw)
        return policy
