# kris

`kris` is a CLI tool for interaction with Christofari.

## Installation

To install and update, use:
```bash
pip install --force-reinstall git+https://gitlab.com/chit-chat/kris.git
```

Then run `kris auth` to authorize at Christofari and `kris add-bucket` to add
your default bucket.

Notice that you need to add bucket from here: https://portal.sbercloud.ru/client/ai-clouds/,
not your usual bucket.
Bucket name must not contain underscore (`_`) symbols.

This limitation applies only to your first bucket. You can add more buckets and use underscores
in their names.

## Usage
See `kris --help` for details.

## Development
```bash
git clone git+https://gitlab.com/chit-chat/kris.git
cd kris
pip install -e .
```
