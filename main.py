import datetime
import logging

import backoff
import click
import keyring
import requests

import s3


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
logger.addHandler(handler)


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

    def list_jobs(self, service=False):
        prefix = "/service" if service else ""
        r = self._api("GET", f"{prefix}/jobs")
        return r["jobs"]

    def status(self, job_id, service=False):
        prefix = "/service" if service else ""
        return self._api("GET", f"{prefix}/jobs/{job_id}")

    def logs(self, job_id, service=False):
        prefix = "/service" if service else ""
        return self._api("GET", f"{prefix}/jobs/{job_id}/logs", stream=True)

    def run(self):
        body = {
            "script": "/home/jovyan/kris/test.py hello from kris",
            "base_image": "registry.aicloud.sbcp.ru/horovod-tf2",
            "n_workers": 1,
            "n_gpus": 1,
            "warm_cache": False,
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
        if service and job_status["status"] == "Complete":
            return True
        if not service and job_status.get("completed_at", 0) > 0:
            return True
        return False

    def _get_access_token(self):
        body = {
            "email": self.user_data.email,
            "password": self.user_data.password,
        }
        r = self._api("POST", "/auth", body=body)
        self.user_data.access_token = r["token"]["access_token"]

    @backoff.on_exception(backoff.fibo,
                          requests.exceptions.RequestException,
                          max_time=60,
                          giveup=lambda e: (
                              400 <= e.response.status_code < 500
                              and e.response.status_code != 429
                          ),
                          logger=logger,
    )
    def _api(self, verb, method, *, headers=None, body=None, **kwargs):
        # Construct headers
        default_headers = {
            "X-Api-Key": self.user_data.api_key,
        }
        if method != "auth":
            default_headers["Authorization"] = self.user_data.access_token
        if headers is not None:
            for name, value in default_headers.items():
                headers[name] = value
        else:
            headers = default_headers

        # Send request
        logger.debug(f"> {verb} {method} {headers} {body}")
        r = requests.request(verb, self.API_URL + method,
                             headers=headers, json=body, **kwargs)
        logger.debug(f"< {r.status_code} {r.text}")

        # Return result
        if r.status_code == requests.codes.ok:
            if "logs" in method:
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


def human_time(timestamp):
    return datetime.datetime.fromtimestamp(timestamp) \
                            .isoformat(" ", "seconds")



client = Client()


@click.group()
@click.option("--debug", is_flag=True, help="enable debug output")
def main(debug):
    if debug:
        logger.setLevel(logging.DEBUG)


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
def list(service):
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
def logs(job_id):
    click.echo_via_pager(client.logs(job_id))


@main.command()
def run():
    print(client.run())


@main.command()
@click.argument("src")
@click.argument("dst")
def transfer(src, dst):
    job_info = client.transfer_file(src, dst)
    client.wait_for_job(job_info["job_name"], service=True)


if __name__ == "__main__":
    main()
