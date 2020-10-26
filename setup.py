import pathlib
from setuptools import setup


here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="kris",
    version="0.0.2",
    description="A cli tool for interaction with Christofari",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/chit-chat/kris",
    author="Nikolai Kruglikov",
    author_email="nnkruglikov@sberbank.ru",
    python_requires=">=3.6",
    install_requires=[
        "backoff>=1.10.0",
        "boto3>=1.16.2",
        "click>=7.1.2",
        "colorama>=0.4.4",
        "keyring>=21.4.0",
        "requests>=2.24.0",
        "toml>=0.10.1",
    ],
    entry_points={
        "console_scripts": [
            "kris=kris.main:main",
        ],
    },
)
