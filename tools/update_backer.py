#!/usr/bin/env python3
r"""Add or edit github users in the backers file
"""

import sys
import logging
import requests
from pathlib import Path
from ruamel.yaml import YAML  # pip install ruamel.yaml

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def fetch_user(username):
    logger.info("fetching %s", username)
    resp = requests.get(
        f"https://api.github.com/users/{username}",
        headers={"Accept": "application/vnd.github.v3+json"},
    )
    resp.raise_for_status()
    return resp.json()


def get_user_data(data):
    """
    Get the data to save from the request data
    """
    out = {
        "username": data["login"],
        "avatar": data["avatar_url"],
        "name": data["name"],
    }
    if data["blog"]:
        website = data["blog"]
        if not website.startswith("http"):
            website = "http://" + website

        out["website"] = website

    return out


def add_entry(opt, filedata, username):
    userdata = get_user_data(fetch_user(username))
    if opt.top:
        userdata["tier"] = "top"

    filedata.append(userdata)


def update_entry(opt, filedata, entry):
    # entry is an username or an user entry daat
    if isinstance(entry, str):
        username = entry
        entry = [e for e in filedata if e["username"] == username]
        if not entry:
            raise Exception(f"{username} not found")
        entry = entry[0]
    else:
        username = entry["username"]

    userdata = get_user_data(fetch_user(username))
    for k, v in userdata.items():
        if entry.get("keep_" + k):
            continue
        entry[k] = v


def main():
    opt = parse_cmdline()
    logger.info("reading %s", opt.file)
    yaml = YAML(typ="rt")
    filedata = yaml.load(opt.file)

    for username in opt.add or ():
        add_entry(opt, filedata, username)

    for username in opt.update or ():
        update_entry(opt, filedata, username)

    if opt.update_all:
        for entry in filedata:
            update_entry(opt, filedata, entry)

    # yamllint happy
    yaml.explicit_start = True
    logger.info("writing %s", opt.file)
    yaml.dump(filedata, opt.file)


def parse_cmdline():
    from argparse import ArgumentParser

    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--file",
        help="the file to update [default: %(default)s]",
        default=Path(__file__).parent.parent / "BACKERS.yaml",
        type=Path,
    )
    parser.add_argument(
        "--add",
        metavar="USERNAME",
        nargs="+",
        help="add USERNAME to the backers",
    )

    parser.add_argument(
        "--top",
        action="store_true",
        help="add to the top tier",
    )

    parser.add_argument(
        "--update",
        metavar="USERNAME",
        nargs="+",
        help="update USERNAME data",
    )

    parser.add_argument(
        "--update-all",
        action="store_true",
        help="update all the existing backers data",
    )

    opt = parser.parse_args()

    return opt


if __name__ == "__main__":
    sys.exit(main())
