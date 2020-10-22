import os
import shutil
import tempfile

import boto3

import toml


class Config:
    def __init__(self, path="config.toml"):
        self._dict = toml.load(path)

    @property
    def buckets(self):
        return self._dict["buckets"]


config = Config()


class Bucket:
    def __init__(self, alias="default"):
        if alias not in config.buckets:
            raise RuntimeError("Bucket \"{alias}\" doesn't exists")
        self._properties = config.buckets[alias]

    def upload_local_file(self, path):
        session = boto3.session.Session()
        s3_client = session.client(
            service_name="s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            endpoint_url=self.endpoint_url,
        )
        s3_client.upload_file(path, self.bucket_id, "kris/" + os.path.basename(path))

    def __getattr__(self, name):
        if name in self._properties:
            return self._properties[name]
        raise AttributeError(name)

    @property
    def endpoint_url(self):
        return "https://{}.s3pd02.sbercloud.ru".format(self.namespace)


class Path:
    def __init__(self, path):
        if not self.is_correct(path):
            raise RuntimeError("Path should start with \"s3://\"")

        parts = path[len("s3://"):].split("/")
        first_part = path[0]

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

    @staticmethod
    def is_correct(path):
        return path.startswith("s3://")


def send_from_local_to_s3(path, bucket):
    with tempfile.TemporaryDirectory() as tmp:
        archive_path = os.path.join(tmp, "archive")
        shutil.make_archive(archive_path, "gztar", path)
        bucket.upload_local_file(archive_path)


