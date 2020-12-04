#!/usr/bin/env python3
r"""Add a github user to the backers file

Print the tag to represent an user.

Hint: to reprocess the list of users you can use:

    grep 'github.com.*"100"' BACKERS.md \
        | sed 's|\(.*github.com/\)\([^"]\+\)\(.*\)|\2|' \
        | xargs ./tools/add_backer.py --big

    grep 'github.com.*"60"' BACKERS.md \
        | sed 's|\(.*github.com/\)\([^"]\+\)\(.*\)|\2|' \
        | xargs ./tools/add_backer.py
"""

import sys
import html
import logging
import requests

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)


def main():
    opt = parse_cmdline()
    tags = []
    for username in opt.username:
        logger.info("fetching %s", username)
        resp = requests.get(
            f"https://api.github.com/users/{username}",
            headers={"Accept": "application/vnd.github.v3+json"},
        )
        size = 100 if opt.big else 60
        resp.raise_for_status()
        data = resp.json()
        tags.append(
            f"""<a href="{data['html_url']}">"""
            f"""<img src="{data['avatar_url']}" """
            f"""title="{html.escape(data['name'])}" """
            f"""width="{size}" height="{size}" """
            f"""style="border-radius: 50%"></a>"""
        )
    for tag in tags:
        print(tag)


def parse_cmdline():
    from argparse import ArgumentParser

    parser = ArgumentParser(description=__doc__)
    parser.add_argument("username", nargs="*", help="github user to add")
    parser.add_argument("--big", action="store_true", help="make them larger")

    opt = parser.parse_args()

    return opt


if __name__ == "__main__":
    sys.exit(main())
