import logging
import sys
from pathlib import Path

import click
import colorlog
from cookiecutter.exceptions import OutputDirExistsException
from cookiecutter.main import cookiecutter

import lingcorp
from lingcorp.config import INPUT_DIR, OUTPUT_DIR
from lingcorp.helpers import get_pos, load_data, run_pipeline

handler = colorlog.StreamHandler(None)
handler.setFormatter(
    colorlog.ColoredFormatter("%(log_color)s%(levelname)-7s%(reset)s %(message)s")
)
global log
log = logging.getLogger()
log.setLevel(logging.INFO)
log.propagate = True
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
    from conf import config, pipeline, pos_list

    parse_csvs(
        pipeline,
        config.get("output_file", "parsed.csv"),
        config.get("filter", {}),
        pos_list,
    )


@main.command()
def web():
    from lingcorp.server import run_server

    run_server()


@main.command()
@click.argument("name")
def new(name):
    try:
        cookiecutter(
            str(Path(lingcorp.__file__).parent / "project_template"),
            output_dir=name,
        )
    except OutputDirExistsException as e:
        print(e)
        print("Run with --force option to overwrite!")
        raise ValueError()


def parse_csvs(pipeline, out_f, filter_params=None, pos_list=None):
    fields = {x["key"]: x for x in pipeline if isinstance(x, dict)}
    data = load_data(
        fields=fields,
        filter_params=filter_params,
    )
    annotations = {}
    data = run_pipeline(data, annotations, pipeline, pos_list=pos_list or [])
    for col in ["ana", "anas", "audio"]:
        if col in data.columns:
            data.drop(columns=[col], inplace=True)
    for col, field in fields.items():
        if (
            field["lvl"] == "word"
            and col in data.columns
            and isinstance(data[col].iloc[0], list)
        ):
            if isinstance(data[col].iloc[0][0], list):
                data[col] = data[col].apply(lambda x: [",".join(y) for y in x])
            data[col] = data[col].apply(lambda x: "\t".join(x))
    for key, field_data in fields.items():
        if key in data.columns and "label" in field_data:
            data.rename(columns={key: field_data["label"]}, inplace=True)
    data.to_csv(OUTPUT_DIR / out_f, index=False)

    # nested_recs = []
    # for rec in records:
    #     word_cols = [x for x, v in rec.items() if isinstance(v, list)]
    #     rec["Words"] = []
    #     for i in range(0, len(rec[word_cols[0]])):
    #         rec["Words"].append({col_dict.get(x, x): rec[x][i] for x in word_cols})
    #     for c in word_cols:
    #         del rec[c]
    #     nested_recs.append(rec)
    # dump(nested_recs, OUTPUT_DIR / "test.json")
