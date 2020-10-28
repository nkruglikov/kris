import datetime
import hashlib
import itertools
import json
import logging
import shutil
import tempfile
from copy import deepcopy

import backoff
import click
import keyring
import os
import requests

from . import s3


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
logger.addHandler(handler)
s3.logger.addHandler(handler)


class UserData:

    DATA_FIELDS = [
        "email",
        "password",
        "api_key",
        "access_token",
    ]
    KEYRING = "kris"

    def __getattr__(self, name):
        if name in self.DATA_FIELDS:
            return keyring.get_password(self.KEYRING, name)
        raise AttributeError(name)

    def __setattr__(self, name, value):
        if name in self.DATA_FIELDS:
            keyring.set_password(self.KEYRING, name, value)
            return
        raise AttributeError(name)

    def __delattr__(self, name):
        if name in self.DATA_FIELDS:
            keyring.delete_password(self.KEYRING, name)
            return
        raise AttributeError(name)


class Client:

    API_URL = "https://api.aicloud.sbercloud.ru/public/v1"

    def __init__(self):
        self.user_data = UserData()

    @property
    def is_authorized(self):
        return self.user_data.email is not None

    def auth(self, email, password, api_key):
        self.user_data.email = email
        self.user_data.password = password
        self.user_data.api_key = api_key
        self._get_access_token()

    def build_image(self, requirements_path):
        body = {
            "from_image": "registry.aicloud.sbcp.ru/horovod-tf2",
            # FIXME: this will cause problems with separators on windows
            "requirements_file": f"/home/jovyan/{requirements_path}",
        }
        return self._api("POST", "/service/image", body=body)

    def list_jobs(self, service=False):
        prefix = "/service" if service else ""
        r = self._api("GET", f"{prefix}/jobs")
        return r["jobs"]

    def list_nfs_files(self, path):
        job_info = self._api("POST", "/service/storage/list",
                             body={"path": path})
        job_name = job_info["job_name"]
        self.wait_for_job(job_name, service=True)
        return self._api("GET", f"/service/storage/list/{job_name}/json")

    def status(self, job_id, service=False):
        prefix = "/service" if service else ""
        return self._api("GET", f"{prefix}/jobs/{job_id}")

    def logs(self, job_id, service=False, image=False):
        if image:
            prefix = "/service/image"
        elif service:
            prefix = "/service/jobs"
        else:
            prefix = "/jobs"
        return self._api("GET", f"{prefix}/{job_id}/logs", stream=True)

    def run(self, script, base_image=None, n_workers=1, n_gpus=1, warm_cache=False):
        if base_image is None:
            base_image = "registry.aicloud.sbcp.ru/horovod-tf2"
        body = {
            "script": f"/home/jovyan/{script}",
            "base_image": base_image,
            "n_workers": n_workers,
            "n_gpus": n_gpus,
            "warm_cache": False,
            "flags": {"please": "don't"},
        }
        r = self._api("POST", "/jobs", body=body)
        return r

    def transfer_file(self, src, dst):
        src_is_s3 = s3.Path.is_correct(src)
        dst_is_s3 = s3.Path.is_correct(dst)
        if src_is_s3 and not dst_is_s3:
            src = s3.Path(src)
            s3_path = src
        elif dst_is_s3 and not src_is_s3:
            dst = s3.Path(dst)
            s3_path = dst
        else:
            raise RuntimeError("Exactly one of (src, dst) should be S3 path")

        self._set_s3_settings(s3_path.bucket)
        body = {"src": str(src), "dst": str(dst)}
        return self._api("POST", "/s3/copy", body=body)

    @backoff.on_predicate(backoff.expo, max_value=10)
    def wait_for_job(self, job_id, service=False):
        job_status = self.status(job_id, service)
        if service and job_status["status"] in ("Complete", "Failed"):
            return job_status
        if not service and job_status.get("completed_at", 0) > 0:
            return True
        return False

    @backoff.on_predicate(backoff.expo, max_value=10)
    def wait_for_logs(self, job_id, service=False):
        logs = self.logs(job_id, service)
        first_line = next(logs)
        if first_line.startswith("Job in queue."):
            return False
        return itertools.chain([first_line], logs)

    def _get_access_token(self):
        body = {
            "email": self.user_data.email,
            "password": self.user_data.password,
        }
        r = self._api("POST", "/auth", body=body)
        self.user_data.access_token = r["token"]["access_token"]

    @backoff.on_exception(
        backoff.fibo,
        requests.exceptions.RequestException,
        max_time=60,
        giveup=lambda e: (
            # retry server errors and Too Many Requests
            400 <= e.response.status_code < 500
            and e.response.status_code != 429
        ),
        logger=logger,
    )
    def _api(self, verb, method, *, headers=None, body=None, stream=False):
        # FIXME: this method is a mess

        # Construct headers
        default_headers = {
            "X-Api-Key": self.user_data.api_key,
        }
        if method != "/auth":
            default_headers["Authorization"] = self.user_data.access_token
        if headers is not None:
            for name, value in default_headers.items():
                headers[name] = value
        else:
            headers = default_headers

        # Send request
        print_headers = self._censor(headers, ["X-Api-Key", "Authorization"])
        if body is None:
            print_body = "<empty body>"
        else:
            print_body = self._censor(body, ["email", "password",
                                             "access_key_id", "security_key"])
        logger.debug(f"> {verb} {method} {print_headers} {print_body}")

        r = requests.request(verb, self.API_URL + method,
                             headers=headers, json=body, stream=stream)

        if method == "/auth":
            print_response = self._censor(r.json(),
                                          ["access_token", "refresh_token"])
        else:
            print_response = r.text
        logger.debug(f"< {r.status_code} {print_response}")

        # Return result
        if r.status_code == requests.codes.ok:
            if stream:
                return self._stream_iterator(r)
            return r.json()

        # Handle errors
        if r.json().get("error_message") == "access_token expired":
            self._get_access_token()
            return self._api(verb, method, body=body, headers=headers)
        r.raise_for_status()

    def _stream_iterator(self, r):
        if r.encoding is None:
            r.encoding = "utf-8"
        for line in r.iter_lines(decode_unicode=True):
            if line:
                yield line + "\n"

    def _set_s3_settings(self, bucket):
        body = {
            "s3_namespace": bucket.namespace,
            "access_key_id": bucket.access_key_id,
            "security_key": bucket.secret_access_key,
        }
        return self._api("POST", "/s3/credentials", body=body)

    @classmethod
    def _censor(cls, obj, censored_names):
        if isinstance(obj, dict):
            result = {}
            for name, item in obj.items():
                if name in censored_names:
                    result[name] = 5 * "*"
                else:
                    result[name] = cls._censor(item, censored_names)
        elif isinstance(obj, list):
            result = []
            for item in obj:
                result.append(cls._censor(item, censored_names))
        else:
            result = deepcopy(obj)
        return result


class ImageCache:
    def __init__(self, path=None):
        if path is None:
            path = self._get_default_path()
        self.path = path
        if not os.path.exists(path):
            self._dump_cache({})

    def has(self, path):
        checksum = self._calc_checksum(path)
        cache = self._load_cache()
        return checksum in cache

    def get(self, path):
        checksum = self._calc_checksum(path)
        cache = self._load_cache()
        return cache[checksum]

    def put(self, path, image_id):
        checksum = self._calc_checksum(path)
        cache = self._load_cache()
        cache[checksum] = image_id
        self._dump_cache(cache)

    def _load_cache(self):
        with open(self.path) as inp:
            return json.load(inp)

    def _dump_cache(self, cache):
        with open(self.path, "w") as out:
            json.dump(cache, out)

    def _calc_checksum(self, path):
        return s3.file_checksum(path)

    @staticmethod
    def _get_default_path():
        return os.path.expanduser(
            os.path.join("~", ".config", "kris", "image_cache.json"))


def human_time(timestamp):
    return datetime.datetime.fromtimestamp(timestamp) \
                            .isoformat(" ", "seconds")


def upload_local_to_nfs(local_path):
    bucket = s3.Bucket()

    # Make archive if directory and upload to S3
    if os.path.isdir(local_path):
        with tempfile.TemporaryDirectory() as tmp:
            archive_path = os.path.join(tmp, "archive")
            logger.debug(f"Compressing \"{local_path}\"...")
            shutil.make_archive(archive_path, "zip", local_path)
            logger.debug(f"Uploading \"{local_path}\" to S3...")
            s3_path = bucket.upload_local_file(archive_path + ".zip")
    else:
        logger.debug(f"Uploading \"{local_path}\" to S3...")
        s3_path = bucket.upload_local_file(local_path)

    # Transfer from S3 to NFS
    nfs_path = s3_path.to_nfs()
    logger.debug(f"Transfering from S3 to NFS: {nfs_path}...")

    if nfs_file_exists(f"/home/jovyan/{nfs_path}"):
        # Don't transfer if exists
        logger.debug(f"{nfs_path} found in cache")
        return nfs_path

    job_info = client.transfer_file(str(s3_path), nfs_path)
    job_info = client.wait_for_job(job_info["job_name"], service=True)
    if job_info["status"] == "Complete":
        logger.debug(f"Upload succeeded: {nfs_path}")
        return nfs_path
    elif job_info["status"] == "Failed":
        logger.debug(f"Upload failed: {nfs_path}")
    else:
        logger.debug(f"Upload job finished with unknown status: "
                     + job_info["status"])
        logger.debug(f"Upload failed: {nfs_path}")


def nfs_file_exists(path):
    files = client.list_nfs_files(path)["ls"]
    if len(files) == 1 and files[0]["size"] == "No":
        return False
    return True


def _build_image(requirements_path):
    if image_cache.has(requirements_path):
        image = image_cache.get(requirements_path)
        logger.debug(f"Image was found in cache: {image}")
        return image
    nfs_path = upload_local_to_nfs(requirements_path)
    job_info = client.build_image(nfs_path)
    image = job_info["image"]
    client.wait_for_job(job_info["job_name"], service=True)
    image_cache.put(requirements_path, image)
    return image


client = Client()
image_cache = ImageCache()


@click.group()
@click.option("--debug", is_flag=True, help="enable debug output")
def main(debug):
    if debug:
        handler.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)


@main.command()
@click.option("-f", "--force", is_flag=True, help="force rewrite credentials")
def auth(force):
    if client.is_authorized and not force:
        click.secho(
            "You are already authorized.\n"
            "Use -f option to rewrite credentials.",
            bold=True
        )
        return

    click.secho(
        "Enter email and password you use to login on Christofari.",
        bold=True
    )
    email = click.prompt("email")
    password = click.prompt("password", hide_input=True)

    click.secho(
        "Enter your client API key.\n"
        "You can get it by running "
        + click.style("echo $GWAPI_KEY", reverse=True),
        bold=True
    )
    click.secho("inside a Christofari terminal.", bold=True)
    api_key = click.prompt("api key")

    client.auth(email, password, api_key)


@main.command()
@click.option("--service", is_flag=True)
def list_jobs(service):
    jobs = client.list_jobs(service)
    if len(jobs) == 0:
        click.secho("No jobs", bold=True)
        return
    click.secho("started              status\tname", fg="yellow", bold=True)
    click.secho("-" * 79, fg="yellow", bold=True)
    for job in sorted(jobs, key=lambda x: x.get("created_at", 0)):
        if not service:
            started = human_time(job["created_at"])
        else:
            started = "?" * 16
        status = job["status"]
        name = job["job_name"]
        click.secho(f"{started}  {status}\t{name}", fg="yellow", bold=True)


@main.command()
@click.argument("job_id")
@click.option("--service", is_flag=True)
def status(job_id, service):
    status = client.status(job_id, service)
    if status["error_message"] != "":
        click.secho(f"Error: {status['error_message']}", fg="red", bold=True)
        return
    click.secho(f"ID:        ", fg="yellow", bold=True, nl=False)
    click.secho(status["job_name"], bold=True)
    if service:
        click.secho("Status:    ", fg="yellow", bold=True, nl=False)
        click.secho(status["status"], bold=True)
        return
    for stage in ["created", "pending", "running", "completed"]:
        if status.get(stage + "_at") != 0:
            timestamp = human_time(status[stage + "_at"])
            stage = stage.title() + ":"
            click.secho(f"{stage:10} ", fg="yellow", bold=True, nl=False)
            click.secho(timestamp, bold=True)


@main.command()
@click.argument("job_id")
@click.option("--service", is_flag=True)
@click.option("--image", is_flag=True)
def logs(job_id, service, image):
    click.echo_via_pager(client.logs(job_id, service, image))


@main.command()
@click.argument("executable")
@click.argument("args", nargs=-1)
@click.option("--image")
@click.option("--requirements")
@click.option("--root")
def run(executable, args, image, requirements, root):
    executable = os.path.abspath(executable)

    # detect root folder
    if root is None:
        root = os.path.dirname(executable)
    else:
        root = os.path.abspath(root)

    # build image
    if requirements is not None:
        if image is not None:
            click.secho("Don't set --image and --requirements together",
                        bold=True, fg="red")
            return  # FIXME: exit code
        click.secho(f"Building image...", bold=True)
        image = _build_image(requirements)

    click.secho("Uploading script...", bold=True)
    # upload executable
    archive_nfs_path = upload_local_to_nfs(root)

    # upload agent
    click.secho("Uploading agent...", bold=True)
    agent_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "agent.py",
    )
    agent_nfs_path = upload_local_to_nfs(agent_path)

    # handle args
    nargs = len(args)
    args = [str(nargs)] + list(args)

    # run job
    click.secho("Launching job...", bold=True)
    executable_name = os.path.basename(executable)
    job_info = client.run(
        f"{agent_nfs_path} {archive_nfs_path} "
        + executable_name + " " + " ".join(args),
        base_image=image,
    )
    click.secho(f"Job launched: {job_info['job_name']}", bold=True, fg="green")

    click.secho("Waiting for logs...", bold=True)
    # print logs
    for line in client.wait_for_logs(job_info["job_name"]):
        print(line, end="")


@main.command()
@click.argument("src")
@click.argument("dst")
def transfer(src, dst):
    job_info = client.transfer_file(src, dst)
    client.wait_for_job(job_info["job_name"], service=True)


@main.command()
@click.argument("local_path")
@click.argument("nfs_path")
def upload(local_path, nfs_path):
    nfs_path = upload_local_to_nfs(local_path)
    click.secho(f"Uploaded {local_path} to NFS: {nfs_path}")


@main.command()
@click.argument("requirements")
def build_image(requirements):
    click.secho(f"Building image...", bold=True)
    image = _build_image(requirements)
    click.secho(f"Image was built successfully. Identifier: {image}",
                bold=True, fg="green")
