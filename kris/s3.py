import os
import hashlib

import boto3

import toml


class Config:
    def __init__(self, path=None):
        if path is None:
            path = self._get_default_path()
        self._dict = toml.load(path)

    @property
    def buckets(self):
        return self._dict["buckets"]

    @staticmethod
    def _get_default_path():
        return os.path.expanduser(
            os.path.join("~", ".config", "kris", "config.toml"))



config = Config()


class Bucket:
    def __init__(self, alias="default"):
        if alias not in config.buckets:
            raise RuntimeError("Bucket \"{alias}\" doesn't exists")
        self._properties = config.buckets[alias]

    def upload_local_file(self, path):
        path = os.path.abspath(os.path.expanduser(path))
        checksum = file_checksum(path)
        s3_path = f"kris/{checksum}_" + os.path.basename(path)

        session = boto3.session.Session()
        s3_client = session.client(
            service_name="s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            endpoint_url=self.endpoint_url,
        )
        s3_client.upload_file(path, self.bucket_id, s3_path)

        return self.make_path(s3_path)

    def __getattr__(self, name):
        if name in self._properties:
            return self._properties[name]
        raise AttributeError(name)

    @property
    def endpoint_url(self):
        return "https://{}.s3pd02.sbercloud.ru".format(self.namespace)

    def make_path(self, path):
        return Path(f"s3://{self.bucket_id}/{path}")


class Path:
    def __init__(self, path):
        if not self.is_correct(path):
            raise RuntimeError("Path should start with \"s3://\"")

        parts = path[len("s3://"):].split("/")
        first_part = parts[0]

        # Suppose that first part is bucket alias
        if first_part in config.buckets:
            self.bucket = Bucket(first_part)
            self.parts = parts[1:]
            return

        # Suppose that first part is known bucket id
        for alias, properties in config.buckets.items():
            if properties.get("bucket_id") == first_part:
                self.bucket = Bucket(alias)
                self.parts = parts[1:]
                return

        # Suppose that first part is unknown bucket id
        if first_part.endswith("bucket") and len(first_part) \
                == len("00000000-0000-0000-0000-000000000000-bucket"):
            raise RuntimeError(f"Path {path} is from unknown bucket")

        # Suppose that bucket identificator is ommited, return default bucket
        self.bucket = Bucket()
        self.parts = parts

    def __repr__(self):
        tail = "/".join(self.parts)
        return f"s3://{self.bucket.bucket_id}/{tail}"

    def to_nfs(self):
        tail = "/".join(self.parts)
        return f".kris/s3/{self.bucket.bucket_id}/{tail}"

    @staticmethod
    def is_correct(path):
        return path.startswith("s3://")


def file_checksum(path):
    algo = hashlib.md5()
    with open(path, "rb") as inp:
        algo.update(inp.read())
    return algo.hexdigest()
