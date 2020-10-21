import click
import keyring
import requests

import datetime
import logging


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

    def list_jobs(self):
        r = self._api("GET", "/jobs")
        return r["jobs"]

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

    def status(self, job_id):
        return self._api("GET", f"/jobs/{job_id}")

    def logs(self, job_id):
        return self._api("GET", f"/jobs/{job_id}/logs", stream=True)

    def _get_access_token(self):
        body = {
            "email": self.user_data.email,
            "password": self.user_data.password,
        }
        r = self._api("POST", "/auth", body=body)
        self.user_data.access_token = r["token"]["access_token"]

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
        else:
            raise RuntimeError(f"API Error: {r}")

    def _stream_iterator(self, r):
        if r.encoding is None:
            r.encoding = "utf-8"
        for line in r.iter_lines(decode_unicode=True):
            if line:
                yield line + "\n"


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
def list():
    jobs = client.list_jobs()
    if len(jobs) == 0:
        click.secho("No jobs", bold=True)
        return
    click.secho("started              status\tname", fg="yellow", bold=True)
    click.secho("-" * 79, fg="yellow", bold=True)
    for job in sorted(jobs, key=lambda x: x["created_at"]):
        started = human_time(job["created_at"])
        status = job["status"]
        name = job["job_name"]
        click.secho(f"{started}  {status}\t{name}", fg="yellow", bold=True)


@main.command()
@click.argument("job_id")
def status(job_id):
    status = client.status(job_id)
    if status["status"] != "ok":
        click.secho(f"Error: {status['error_message']}", fg="red", bold=True)
        return
    click.secho(f"ID:        ", fg="yellow", bold=True, nl=False)
    click.secho(status["job_name"], bold=True)
    for stage in ["created", "pending", "running", "completed"]:
        if status.get(stage + "_at") != 0:
            time = human_time(status[stage + "_at"])
            stage = stage.title() + ":"
            click.secho(f"{stage:10} ", fg="yellow", bold=True, nl=False)
            click.secho(time, bold=True)


@main.command()
@click.argument("job_id")
def logs(job_id):
    click.echo_via_pager(client.logs(job_id))


@main.command()
def run():
    print(client.run())


if __name__ == "__main__":
    main()
