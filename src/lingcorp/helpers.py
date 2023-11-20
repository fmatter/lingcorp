import logging
import re
from collections import Counter

import pandas as pd
import pygraid
import questionary
from humidifier import humidify
from pyigt import IGT
from tqdm import tqdm
from writio import load

from lingcorp.config import INPUT_DIR

SEC_JOIN = ","
SEP = "\t"

uniparser_fields = {
    "wf": "srf",
    "wfGlossed": "obj",
    "gloss": "gls",
    "lemma": "lex",
    "gramm": "grm",
    "id": "mid",
    "ana": "ana",
    "anas": "anas",
}


log = logging.getLogger(__name__)

ud_pos = ["v"]


def load_data(fields={}, filter_params={}):
    log.info("Loading data...")
    dfs = []
    filelist = list(INPUT_DIR.glob("*.csv"))
    for file in tqdm(filelist, "Scanning input directory"):
        df = load(file, index_col="ID")
        df["filename"] = file.name
        dfs.append(df)
    if not dfs:
        return None
    data = pd.concat(dfs)
    for k, v in filter_params.items():
        if isinstance(v, list):
            data = data[data[k] == v[0]]
        else:
            data = data[data[k] == v]
    for key, field_data in fields.items():
        if field_data.get("label", None) in data.columns:
            data.rename(columns={field_data["label"]: key}, inplace=True)
            if field_data.get("lvl", None) == "word":
                data[key] = data[key].apply(lambda x: x.split(SEP))
        else:
            if field_data.get("lvl") in ["translations", "record"]:
                data[key] = ""
    data["ID"] = data.index
    return data


def insert_pos_rec(rec, pos_list):
    rec["pos"] = []
    for grm in rec["grm"]:
        res = get_pos(grm, pos_list=pos_list)
        rec["pos"].append(res or "?")
    assert len(rec["grm"]) == len(rec["pos"])
    return rec


def add_wid(rec):
    rec["wid"] = []
    i = 0
    while i < len(rec["obj"]):
        clitics = [
            (obj, gls)
            for (obj, gls) in zip(rec["obj"][i].split("="), rec["gls"][i].split("="))
        ]
        rec["wid"].append(
            "=".join(
                [
                    humidify(obj.replace("-", "").replace("∅", "") + "-" + gls)
                    for obj, gls in clitics
                ]
            )
        )
        i += 1
    return rec


def load_annotations(key, field, data, rec_id=None):
    field_annotations = {}
    if "file" not in field:
        return data, field_annotations
    print(f"Loading annotations from {field['file']}")
    if key not in data:
        if field.get("split"):
            data[key] = data.apply(lambda x: [""] * (len(x["srf"])), axis=1)
        else:
            data[key] = ""
    file_data = load(field["file"]) or {}
    if field["lvl"] in ["record", "precord", "translations"]:
        if rec_id:
            data.at[rec_id, key] = file_data[rec_id]
        else:
            for r_id, value in file_data.items():
                if r_id in data.index:
                    data.at[r_id, key] = value
                field_annotations[r_id] = value
    elif field["lvl"] == "word":
        if rec_id:
            data.at[rec_id, key] = [""] * (len(data.loc[rec_id]["srf"]))
            if "ref" in field:
                for idx, item_data in file_data[rec_id].items():
                    for ref, value in item_data.items():
                        if (
                            not field.get("split")
                            and data.loc[rec_id][field["ref"]][idx] == ref
                            and value
                        ):
                            data.loc[rec_id][key][idx] = value
                        elif field.get(
                            "split"
                        ):  # and ref in data.loc[rec_id][field["ref"]][idx].split(" ") and value:
                            # data.loc[rec_id][key][idx] = value
                            print(ref, value)
                            print(data.loc[rec_id][key][idx][field["ref"]])
                            exit()
                        else:
                            print(
                                "ALERT:",
                                ref,
                                "does not match",
                                field["ref"],
                                # "'" + data.loc[rec_id][field["ref"]][idx] + "'",
                                "in record",
                                rec_id,
                            )
        else:
            for r_id, item in file_data.items():
                field_annotations[r_id] = {}
                if r_id in data.index:
                    data.at[r_id, key] = [""] * (len(data.loc[r_id]["srf"]))
                    if "ref" in field:
                        for idx, item_data in item.items():
                            if field.get("split"):
                                values = []
                                for ref, value in item_data.items():
                                    if ref in data.loc[r_id][field["ref"]][idx].split(
                                        " "
                                    ):
                                        values.append(value)
                                        field_annotations[r_id].setdefault(idx, {})
                                        field_annotations[r_id][idx][ref] = value
                                    else:
                                        log.warning(
                                            f'{ref} not found in {field["ref"]} annotation {data.loc[r_id][field["ref"]][idx].split(" ")} in record {r_id}, skipping.'
                                        )
                                        # exit()
                                data.loc[r_id][key][idx] = " ".join(values)
                            else:
                                for ref, value in item_data.items():
                                    if (
                                        data.loc[r_id][field["ref"]][idx]
                                        == ref
                                        # and value
                                    ):
                                        data.loc[r_id][key][idx] = value
                                        field_annotations[r_id].setdefault(idx, {})
                                        field_annotations[r_id][idx][ref] = value
                                    else:
                                        log.warning(
                                            f"'{ref}' does not match {field['ref']} '{data.loc[r_id][field['ref']][idx]}' in record {r_id}, skipping."
                                        )
                                        # exit()
    return data, field_annotations


def run_pipeline(data, annotations, pipeline, pos_list):
    for item in pipeline:
        if isinstance(item, dict):
            data, field_annotations = load_annotations(item["key"], item, data)
            annotations[item["key"]] = field_annotations
        else:
            res = []
            for x in tqdm(data.to_dict("records")):
                res.append(item.parse(x))
            item.save()
            data = pd.DataFrame.from_dict(res)
            data.index = data["ID"]
    if "grm" in data.columns and "pos" not in data.columns:
        data = data.apply(lambda x: insert_pos_rec(x, pos_list=pos_list), axis=1)
        data = data.apply(lambda x: add_wid(x), axis=1)
    return data


def printdict(d):
    for k, v in d.items():
        print(f"{k}: {v}")


def favorite(array):
    return Counter(array).most_common(1)[0][0]


def choose_from_list(answers, prompt):
    if isinstance(answers, dict):
        andic = {answer: i for i, answer in answers.items()}
    else:
        andic = {x: x for x in answers}
    choice = questionary.select(
        prompt,
        choices=answers,
    ).ask()
    return andic[choice]


def listify(x):
    if isinstance(x, list):
        return x
    return [x]


def pad_ex(*lines, sep=" ", as_tuple=False):
    out = {}
    lines = [
        [",".join(x) if x and isinstance(x[0], list) else x for x in line]
        for line in lines
    ]
    for glossbundle in zip(*lines):
        glossbundle = list(
            SEC_JOIN.join(x) if isinstance(x, list) else x for x in glossbundle
        )
        longest = len(max(glossbundle, key=len))
        for i, obj in enumerate(glossbundle):
            diff = longest - len(obj)
            out.setdefault(i, [])
            out[i].append(obj + " " * diff)
    for k in out.copy():
        out[k] = sep.join(out[k])
    if as_tuple:
        return tuple(out.values())
    else:
        return "\n".join(out.values())


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def highlight(s):
    return bcolors.OKGREEN + s + bcolors.ENDC


def highlight_list(string_list, pos):
    return [x if i != pos else highlight(x) for i, x in enumerate(string_list)]


def atoi(text):
    return int(text) if text.isdigit() else text


def human_sort(text):
    """
    alist.sort(key=human_sort) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    """
    return [atoi(c) for c in re.split(r"(\d+)", text)]


def print_record(rec, translation=True, highlight_pos=None):
    print_vals = ["srf", "obj", "gls", "pos", "grm", "graid"]
    val_list = [y for x, y in rec.items() if x in print_vals]
    if highlight_pos is not None:
        highlighted = []
        for l in val_list:
            templist = []
            for i, item in enumerate(l):
                if isinstance(item, list):
                    item = SEC_JOIN.join(item)
                if i == highlight_pos:
                    item = highlight(item)
                templist.append(item)
            highlighted.append(templist)
        print(pad_ex(*highlighted))
    else:
        print(pad_ex(*val_list))
    if translation:
        print("‘" + rec["ftr"] + "’")


def get_pos(tagset, mode="UD", sep=",", pos_list=None):
    """Extracts a POS tag from a tag bundle."""
    if not pos_list:
        if mode == "UD":
            pos_list = ud_pos
        else:
            pos_list = []
    if isinstance(tagset, list):
        tagset = ",".join(tagset)
    clitics = tagset.split("=")
    res = []
    for cltagset in clitics:
        for tag in cltagset.split(sep):
            if tag in pos_list:
                res.append(tag)
                continue
    if not res:
        return ""
    return "=".join(res)


def get_morph_id(id_list, id_dic, obj, gloss="", mode="morphs"):
    """Identifies which ID belongs to a given morph.

    :param id_list: a list of ID strings, one of which is thought to
    belong to the morph
    :type id_list: list
    :param id_dic: a dict mapping ID strings to strings of
    the format <obj:morph>
    :type id_dic: dict
    :param obj: the string representation of the morph's form
    :type obj: str
    :param gloss: the string representation of the morph's gloss
    :type gloss: str
    ...
    :raises [ErrorType]: [ErrorDescription]
    ...
    :return: [ReturnDescription]
    :rtype: [ReturnType]
    """
    test_str = f"{obj}:{gloss}".strip(":")
    log.debug(f"searching {test_str} with {id_list}")
    for m_id in id_list:
        log.debug(f"testing id {m_id}")
        if m_id not in id_dic:
            raise ValueError(f"ID {m_id} not found in id_dic")
        if test_str in id_dic[m_id]:
            if mode == "morphs":
                return id_dic[m_id][test_str]
            if mode == "morphemes":
                return m_id
            raise ValueError(f"Invalid mode '{mode}'")
    return None


def sort_uniparser_ids(id_list, obj, gloss, id_dic, mode="morphs"):
    """Used for sorting the unsorted ID annotations by`uniparser
    <https://uniparser-morph.readthedocs.io/en/latest/paradigms.html#morpheme-ids>`_.
    There will be a glossed word form with segmented object and gloss lines, as
    well as an unordered list of IDs.
    This method uses a dictionary matching IDs to <"form:gloss"> strings to
    sort this ID list, based on the segmented object and glossing lines.

    """
    igt = IGT(phrase=obj, gloss=gloss)
    sorted_ids = []
    for w in igt.glossed_words:
        for m in w.glossed_morphemes:
            try:
                sorted_ids.append(
                    get_morph_id(id_list, id_dic, m.morpheme, m.gloss, mode)
                )
            except ValueError as e:
                log.error(e)
                log.error(id_list)
                log.error(f"{obj} '{gloss}'")
    log.debug(sorted_ids)
    return sorted_ids


def pprint_uniparser(wf):
    if not wf.wfGlossed:
        return f"err: {wf.wf}"
    return f"""obj: {wf.wfGlossed}
gls: {wf.gloss}
lex: {wf.lemma}
grm: {wf.gramm}
ids: {wf.to_json().get("id", None)}"""


def render_boundary(
    ann,
    open_clause=False,
    open_subr=False,
    main_label="C",
    subr_label="NC",
    empty="&nbsp;",
):
    if ann["type"] == "main_clause":
        if open_clause:
            return f"<b>]</b><sub>{main_label}</sub> <b>[</b>"
        else:
            return "<b>[</b>"
    if ann["type"] == "subr_clause":
        return "["
    if ann["type"] == "subr_end" or open_subr:
        return f"]<sub>{subr_label}</sub>"
    return empty


def render_annotation(ann, pos=None, empty="&nbsp;"):
    res = []
    for item in ann:
        if item.get("func") == "pred":
            if item.get("pred") == "v":
                if pos == "vi":
                    res.append("Vi")
                elif pos == "vt":
                    res.append("Vt")
                else:
                    log.error(f"Unknown predicate type: {pos}")
                    res.append(pos)
            elif item.get("pred") == "vother":
                res.append("Vother")
            else:
                res.append("<sub>PRED</sub>")
        if item.get("func") == "aux":
            res.append("<sub>AUX</sub>")
        if item.get("func") == "cop":
            res.append("<sub>COP</sub>")
        if item.get("func") == "vother":
            res.append("V")
        if item.get("type") == "ref":
            if item.get("syn") != "l":
                for val, show in {
                    "p": "O",
                    "s": "S",
                    "a": "A",
                    "l": "L",
                    "g": "G",
                }.items():
                    if item["syn"] == val:
                        res.append(show)
                # else
                #     res.append(item["syn"].upper())
            else:
                res.append("N")
        if item.get("func") == "adp":
            res.append("PP")
        if item.get("type") == "nc":
            res.append(empty)
        if item.get("type") == "other" and item.get("ref") == "np":
            res.append("N")
    if res:
        return "-".join([x for x in res if x])
    if len(ann) == 1:
        if not ann[0]:
            return empty
        if ann[0]["type"] in ["other"]:
            return empty
    return empty


def render_graid(
    ex,
    aligned_fields,
    initial=False,
    open_clause="",
    open_subr="",
    current_main="",
    current_subr="",
    empty="&nbsp;",
    special_empty={},
    pos="pos",
):
    if "graid" in ex:
        if ex["graid"] == "":
            ex["graid"] = ["##"] + ([""] * (len(ex["srf"]) - 1))
        if "refind" in ex and ex["refind"] == "":
            ex["refind"] = [""] * (len(ex["srf"]))
        modified_lines = {"ann": [], "graid": []}
        for col in aligned_fields:
            modified_lines[col] = []
            # if graid is None or graid == "":
            #     for col in aligned_fields:
            #         if col in ex and col != "graid":
            #             modified_lines[col].append(ex[col][p_counter])
            #         else:
            #             print(ex, col, p_counter)
            #             # modified_lines[col].append("")
        for p_counter, graid in enumerate(ex["graid"]):
            if graid is None:
                res = {"pre": [], "data": [], "post": []}
            else:
                res = pygraid.parse_annotation(graid, mode="structured")
            for pre in res.get("pre", []):
                for col in aligned_fields:
                    modified_lines[col].append(special_empty.get(col, empty))
                modified_lines["ann"].append(
                    render_boundary(
                        pre,
                        open_clause=open_clause,
                        main_label=current_main.upper(),
                        subr_label=current_subr.upper(),
                        empty=empty,
                    )
                )
                if pre["type"] == "main_clause":
                    current_main = pre["clause_tag"] or "c"
                    open_clause = True
                if pre["type"] == "subr_clause":
                    current_subr = pre["clause_tag"] or "sc"
                    open_subr = True
            for col in aligned_fields:
                if col in ex:
                    modified_lines[col].append(ex[col][p_counter])
            # modified_lines["graid"].append(graid)
            modified_lines["ann"].append(
                render_annotation(res["data"], pos=ex[pos][p_counter], empty=empty)
            )
            for post in res.get("post", []):
                for col in aligned_fields:
                    modified_lines[col].append(special_empty.get(col, empty))
                modified_lines["ann"].append(
                    render_boundary(
                        post,
                        open_clause=open_clause,
                        open_subr=open_subr,
                        main_label=current_main.upper(),
                        subr_label=current_subr.upper(),
                    )
                )
        if initial:
            modified_lines["ann"].append(f"<b>]</b><sub>{current_main.upper()}</sub>")
            for col in aligned_fields:
                modified_lines[col].append(special_empty.get(col, empty))
        for col, values in modified_lines.items():
            ex[col] = values
    return ex
