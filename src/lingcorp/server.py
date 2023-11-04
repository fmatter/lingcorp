import json
import logging
import re
from pathlib import Path

import pandas as pd
import pygraid
from conf import AUDIO_PATH, pipeline, pos_list
from flask import Flask, render_template, request, send_from_directory
from flask_bootstrap import Bootstrap5
from writio import dump

from lingcorp.annotator import UniParser
from lingcorp.config import OUTPUT_DIR
from lingcorp.helpers import (
    add_wid,
    get_pos,
    insert_pos_rec,
    load_annotations,
    load_data,
    render_graid,
    run_pipeline,
)
from lingcorp.search import CorpusFrame

AUDIO_PATH = Path(AUDIO_PATH)

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


app = Flask(__name__, static_url_path="/static")
bootstrap = Bootstrap5(app)
wlog = logging.getLogger("werkzeug")
wlog.setLevel(logging.ERROR)


def parse_graid(df, aligned_fields, target="all"):
    open_clause = False
    clause_initial = []
    current_main = ""
    current_subr = ""
    for i, (r_id, ex) in enumerate(df.iterrows()):
        if ex["graid"] == "" or ex["graid"] is None:
            ex["graid"] = ["##"] + ([""] * (len(ex["srf"]) - 1))
        if "refind" in ex:
            if ex["refind"] == "" or ex["refind"] is None:
                ex["refind"] = [""] * (len(ex["srf"]))
        if "graid" in ex and ex["graid"]:
            x = 0
            while ex["graid"][x] is None:
                x += 1
            if ex["graid"][x].startswith("##"):
                clause_initial.append(i)
    for i, (r_id, ex) in enumerate(df.iterrows()):
        if (
            i + 1 in clause_initial
            or i + 1 >= len(df)
            or df.iloc[i + 1]["txt"] != ex["txt"]
        ):
            initial = True
        else:
            initial = False
        if target != "all" and target != r_id:
            continue
        ex = render_graid(
            ex,
            initial=initial,
            aligned_fields=aligned_fields,
            empty=None,
            special_empty={"anas": {}},
            open_clause=open_clause,
            current_main=current_main,
            current_subr=current_subr,
        )
        yield ex


fields = {x["key"]: x for x in pipeline if isinstance(x, dict)}

data = load_data(
    rename={
        "Primary_Text": "ort",
        "Translated_Text": "oft",
        "Speaker_ID": "spk",
        "Text_ID": "txt",
    }
)

texts = None
if data is not None:
    uniparser = None
    for p in pipeline:
        if isinstance(p, UniParser):
            uniparser = p

    annotations = {}
    data = run_pipeline(data, annotations, pipeline, pos_list)
    data.index = data["ID"]
    audios = []
    for x in AUDIO_PATH.iterdir():
        audios.append(x.stem)
    data["audio"] = data["ID"].apply(lambda x: x in audios)
    splitcols = [
        "obj",
        "gls",
        # "grm",
        "graid",
        "refind",
        # "lex",
        # "mid",
        "pos",
        # "wid",
        # "ana",
        # "anas",
        "srf",
    ]
    aligned_fields = [x for x in splitcols if x not in []]
    texts = {}
    if "graid" in data.columns:
        data = pd.DataFrame.from_dict(parse_graid(data, aligned_fields))
    for text_id, textdata in data.groupby("txt"):
        texts[text_id] = list(textdata.index)


def save():
    for key, field in fields.items():
        if "file" not in field:
            continue
        dump(annotations[key], field["file"])


def defill(rec):
    for target in splitcols:
        if target not in rec or not rec[target]:
            continue
        rec[target] = [x for x in rec[target] if x is not None]
    return rec


def reparse(ex_id, target):
    log.debug(f"Reparsing {ex_id}")
    if target == "ort":
        for parser in pipeline:
            if isinstance(parser, dict):
                continue
            data.loc[ex_id] = parser.parse(data.loc[ex_id])
        data.loc[ex_id] = insert_pos_rec(data.loc[ex_id], pos_list=pos_list)
        data.loc[ex_id] = add_wid(data.loc[ex_id])
        load_annotations(key="graid", field=fields["graid"], data=data, rec_id=ex_id)
    if target in ["ort", "graid"]:
        res = list(parse_graid(data, target=ex_id))
        return res[0]
    return data.loc[ex_id]


@app.route("/example")
def example():
    if data is None:
        return "None"
    exid = request.args.get("id")
    ex = data.loc[exid]
    field_data = {"precord": {}, "record": {}, "word": {}, "translations": {}}
    for key, field in fields.items():
        if key not in ex:
            continue
        field_data.setdefault(field["lvl"], {})
        field_data[field["lvl"]][key] = field
    return render_template("record.html", ex=ex, fields=field_data, top_align="ann")


@app.route("/graid")
def graid_string():
    return pygraid.to_string(request.args.get("annotation"))


@app.route("/audio/<path:filename>")
def audio(filename):
    return send_from_directory(AUDIO_PATH, filename)


@app.route("/data")
def get_output():
    res = []
    for f in Path(OUTPUT_DIR).iterdir():
        if f.suffix == ".csv":
            res.append(f.name)
    return res


@app.route("/texts")
def get_texts():
    if texts is not None:
        return list(texts.keys())
    return "None"


@app.route("/textrecords")
def textrecords():
    if texts is not None:
        text_id = request.args.get("textID")
        if not text_id:
            return "None"
        return texts[text_id]
    return "None"


@app.route("/export")
def export():
    defilled_data = data.apply(defill, axis=1)
    defilled_data.drop(columns=["ann", "audio", "ana", "anas"], inplace=True)
    for col in splitcols:
        if col in defilled_data.columns:
            if isinstance(defilled_data[col].iloc[0][0], list):
                defilled_data[col] = defilled_data[col].apply(
                    lambda x: [",".join(y) for y in x]
                )
            defilled_data[col] = defilled_data[col].apply(lambda x: "\t".join(x))
    for key, field_data in fields.items():
        if key in defilled_data.columns and "label" in field_data:
            defilled_data.rename(columns={key: field_data["label"]}, inplace=True)
    defilled_data.to_csv("output/test.csv", index=False)
    return {}


def set_up_choice(rec, orig_pos, shifted_pos, choice):
    log.debug(f"Shifting {rec['ID']} from {orig_pos} to {shifted_pos}")
    print(rec)
    if rec["anas"][int(orig_pos)][choice] != "?":
        for field in ["obj", "gls", "lex", "grm", "mid"]:
            rec[field][int(orig_pos)] = rec["anas"][int(orig_pos)][choice].get(
                field, ""
            )
        rec["pos"][int(orig_pos)] = get_pos(rec["grm"][int(orig_pos)], pos_list)
        uniparser.register_choice(
            rec["ID"], orig_pos, rec["anas"][int(orig_pos)][choice]["srf"], choice
        )
    else:
        for field in ["gls", "lex", "grm", "mid", "pos"]:
            rec[field][int(orig_pos)] = "?"
        uniparser.discard_choice(rec["ID"], orig_pos)
    rec["ana"][int(orig_pos)] = choice


@app.route("/pick")
def pick():
    choice = request.args.get("choice")
    target = request.args.get("target")
    values = target.split("_")
    r_id, key, orig_pos, shifted_pos = values
    # print("Picking", choice, r_id, key, orig_pos, shifted_pos)
    set_up_choice(data.loc[r_id], orig_pos, shifted_pos, choice)
    ex = data.loc[r_id]
    field_data = {}
    for key, field in fields.items():
        if key not in ex:
            continue
        field_data.setdefault(field["lvl"], {})
        field_data[field["lvl"]][key] = field
    return render_template("record.html", ex=ex, fields=field_data, top_align="ann")


@app.route("/update")
def update():
    value = request.args.get("value")
    target = request.args.get("target")
    values = target.split("_")
    if len(values) == 1:
        raise ValueError(target)
    elif len(values) == 2:
        # print("updating", value, target, values)
        r_id, key = values
        data.at[r_id, key] = value
        if value:
            # print("setting", key, "annotation for", r_id, "to", value)
            annotations[key][r_id] = value
        elif r_id in annotations[key]:
            # print("empty value, deleting", key, "for", r_id)
            del annotations[key][r_id]
        else:
            log.debug(f"{r_id} is not in {annotations[key]}")
            raise ValueError(r_id)
        data.loc[r_id] = defill(data.loc[r_id])
        data.loc[r_id] = reparse(r_id, target=key)
    elif len(values) == 3:
        r_id, key, pos = values
        pos = int(pos)
        data.loc[r_id] = defill(data.loc[r_id])
        data.at[r_id, key][pos] = value
        if value:
            ref_value = data.at[r_id, fields[key]["ref"]][pos]
            annotations[key].setdefault(r_id, {})
            annotations[key][r_id].setdefault(pos, {})
            annotations[key][r_id][pos][ref_value] = value
        elif key in annotations and pos in annotations[key][r_id]:
            del annotations[key][r_id][pos]
        data.loc[r_id] = reparse(r_id, target=key)
    save()
    return {"updated": r_id}


def build_example_div(ex_ids, audio=None):
    ex = data.loc[ex_ids]
    field_data = {}
    for key, field in fields.items():
        if key not in ex:
            continue
        field_data.setdefault(field["lvl"], {})
        field_data[field["lvl"]][key] = field
    print(field_data)
    return render_template(
        "index.html", exes=ex.to_dict("records"), fields=field_data, top_align="ann"
    )


@app.route("/annotation")
def index():
    return render_template("annotation.html")


@app.route("/annotation/<text_id>")
def text_view(text_id):
    return render_template("annotation.html", text_id=text_id)


conc_fields = {
    x["key"]: x for x in pipeline if isinstance(x, dict) and x["lvl"] == "word"
}


def resolve_regex(s):
    if not s:
        return s
    for cand in ["*", "!", "^", "$"]:
        if cand in s:
            return re.compile(s)
    return s


@app.route("/concordance")
def concordance():
    return render_template("concordance.html", content="")


@app.route("/fields")
def get_conc_fields():
    return conc_fields


@app.route("/search")
def search():
    print(request.args.get("query"))
    print(request.args.get("filename"))
    query = json.loads(request.args.get("query"))
    filename = json.loads(request.args.get("filename"))
    df = CorpusFrame(f"output/{filename}", list_cols=["mid", "grm"])
    return df.query(query, name=None, mode="rich")


def run_server():
    app.run(debug=True, port=5001)
