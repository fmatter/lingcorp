import logging
import sys
from pathlib import Path
import click
import colorlog
from lingcorp.clitools import parse_csvs


handler = colorlog.StreamHandler(None)
handler.setFormatter(
    colorlog.ColoredFormatter("%(log_color)s%(levelname)-7s%(reset)s %(message)s")
)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.propagate = False
log.addHandler(handler)


sys.path.append(str(Path.cwd()))
PIPELINE = "conf.py"


@click.group()
def main():
    pass


@main.command()
@click.option("--limit", default=None, type=int)
@click.option("--text", default=None)
def cli(limit, text):
    from conf import FILTER
    from conf import INPUT_FILE  # pylint: disable=import-outside-toplevel,import-error
    from conf import OUTPUT_FILE
    from conf import pipeline
    from conf import pos_list

    parse_csvs(pipeline, OUTPUT_FILE, FILTER, pos_list)


@main.command()
def web():
    from lingcorp.server import run_server
    run_server()
