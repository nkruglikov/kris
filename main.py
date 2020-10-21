import click
import keyring
import requests


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

    def _get_access_token(self):
        body = {
            "email": self.user_data.email,
            "password": self.user_data.password,
        }
        r = self._api("POST", "/auth", body=body)
        self.user_data.access_token = r["token"]["access_token"]

    def _api(self, verb, method, *, body=None, headers=None):
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

        r = requests.request(verb, self.API_URL + method, json=body, headers=headers)
        r = r.json()

        # Reauthorize
        if r["error_code"] != 0 and r["error_message"] == "access_token expired":
            self._get_access_token()
            return self._api(verb, method, body=body, headers=headers)

        return r


client = Client()


@click.group()
def main():
    pass


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
        click.secho("No jobs", color="yellow", bold=True)
        return
    click.secho("Jobs:", color="yellow", bold=True)
    for job in jobs:
        click.secho(job, bold=True)


if __name__ == "__main__":
    main()
