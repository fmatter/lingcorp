import logging
import re
import time
from pathlib import Path

import pandas as pd
from segments import Profile, Tokenizer
from writio import dump, load

from lingcorp.config import ID_KEY
from lingcorp.helpers import uniparser_fields

log = logging.getLogger(__name__)
# log.setLevel(logging.DEBUG)


class Annotator:
    def __init__(self, name="unnamed", **kwargs):
        """The parse method takes a text record, does something to it, then returns it."""
        self.name = name
        self.annotated_path = Path(f"{self.name}.yaml")
        self.annotated = load(self.annotated_path)

    def parse(self, record):
        """The parse method takes a text record, does something to it, then returns it."""
        return record

    def save(self):
        pass


class Tokenizer(Annotator):
    def __init__(self, name="tokenizer", parse_col="srf", output_col="srf", **kwargs):
        self.name = name
        self.output_col = output_col
        self.parse_col = parse_col

    def parse(self, record):
        record[self.output_col] = record[self.parse_col].strip(" ").split(" ")
        return record


def ortho_strip(ortho_str, replace={}, strip=[]):
    for k, v in replace.items():
        ortho_str = ortho_str.replace(k, v)
    for p in strip:
        ortho_str = ortho_str.replace(p, "")
    return ortho_str.lower()


class Cleaner(Annotator):
    def __init__(
        self,
        name="cleaner",
        parse_col="ort",
        output_col="srf",
        strip=[],
        replace={},
        **kwargs,
    ):
        self.name = name
        self.src = parse_col
        self.output_col = output_col
        self.strip = strip
        self.replace = replace

    def parse(self, rec):
        rec[self.output_col] = ortho_strip(
            rec[self.src], replace=self.replace, strip=self.strip
        )
        return rec


class UniParser(Annotator):
    def __init__(
        self,
        analyzer,
        name="morpho",
        parse_col="srf",
        srf_strip=[",", ".", "!", "?", "¿"],
        use_cache=True,
        mask_ambiguity=False,
        **kwargs,
    ):
        self.name = name
        self.analyzer = analyzer
        self.parse_col = parse_col
        self.annotated_path = f"{name}.yaml"
        self.mask_ambiguity = mask_ambiguity
        self.annotated = load(self.annotated_path) or {}
        if len(analyzer.g.paradigms) == 0:
            self.analyzer.load_grammar()
        self.srf_strip = srf_strip
        if use_cache:
            start = time.perf_counter()
            self.cache_path = f"{name}_cache.pickle"
            self.cache = load(self.cache_path) or {}
            end = time.perf_counter()
            log.info(f"Loaded cache in {end - start:0.4f} seconds")

        else:
            self.cache = None
        self.unresolved = []

    def add_analysis(self, record, analysis, anas, ana, wf):
        if "," in wf:
            print(wf)
            exit()
        unparsable = analysis and analysis["wfGlossed"] == ""
        for field_name, target in uniparser_fields.items():
            if field_name == "wf":
                if not analysis or unparsable:
                    record[target].append(wf)
                else:
                    record[target].append(
                        ortho_strip(analysis.get(field_name, ""), strip=self.srf_strip)
                    )
            elif field_name == "wfGlossed":
                if not analysis or unparsable:
                    record[target].append(wf)
                else:
                    record[target].append(analysis.get(field_name, ""))
            elif field_name == "gramm":  # this is a list
                if not analysis or unparsable:
                    record[target].append("")
                else:
                    record[target].append(",".join((analysis.get(field_name, [""]))))
            elif field_name == "gloss":
                if not analysis:
                    record[target].append("?")
                elif unparsable:
                    record[target].append("***")
                else:
                    record[target].append(analysis.get(field_name, ""))
            elif field_name == "anas":
                record[target].append(anas)
            elif field_name == "ana":
                record[target].append(ana)
            else:
                if analysis:
                    record[target].append(analysis.get(field_name, ""))
                else:
                    record[target].append("")

    def parse(self, record):
        for field_name in ["obj", "gls", "lex", "grm", "mid", "ana", "anas"]:
            record[field_name] = []
        if self.cache and record[ID_KEY] in self.cache:
            all_analyses = self.cache[record[ID_KEY]]
        else:
            all_analyses = self.analyzer.analyze_words(record[self.parse_col])
            all_analyses = [[x.to_json() for x in y] for y in all_analyses]
            self.cache[record[ID_KEY]] = all_analyses
        record[self.parse_col] = []
        for w_idx, wf_analysis in enumerate(all_analyses):
            analysis = None
            if len(wf_analysis) > 1:
                if len(wf_analysis) == 2 and wf_analysis[0] == wf_analysis[1]:
                    log.error("Your parsing is creating copies of the same analysis")
                    log.erro(wf_analysis)
                    exit()
                anas = {"?": "?"}
                ana = "?"
                srf = ortho_strip(wf_analysis[0]["wf"], strip=self.srf_strip)
                for potential_analysis in wf_analysis:
                    anas[potential_analysis["gloss"]] = {
                        uniparser_fields[k]: v
                        for k, v in potential_analysis.items()
                        if k in uniparser_fields
                    }
                    if record[ID_KEY] in self.annotated:
                        if potential_analysis["gloss"] == self.annotated[
                            record[ID_KEY]
                        ].get(w_idx, {srf: None}).get(srf):
                            log.debug(
                                f"""Disambiguated: analysis {potential_analysis} in {record[ID_KEY]}"""
                            )
                            ana = potential_analysis["gloss"]
                            analysis = potential_analysis
            elif len(wf_analysis) == 1:
                log.debug(f"Using unambiguous analysis {wf_analysis[0]}")
                analysis = wf_analysis[0]
                srf = ortho_strip(analysis["wf"], strip=self.srf_strip)
                ana = ""
                anas = {}
            else:
                print(wf_analysis)
                input("OH OH")
            if not analysis:
                if self.mask_ambiguity:
                    analysis = wf_analysis[0]
                else:
                    log.debug(
                        f"Unresolved analytical ambiguity for {srf} in {record[ID_KEY]}"
                    )
                self.unresolved.append(
                    {"rec": record[ID_KEY], "form": srf, "txt": record["txt"]}
                )
            self.add_analysis(record, analysis, anas, ana, srf)
        return record

    def register_choice(self, record_id, pos, obj, choice):
        self.annotated.setdefault(record_id, {})
        self.annotated[record_id].setdefault(int(pos), {})
        self.annotated[record_id][int(pos)][
            ortho_strip(obj, strip=self.srf_strip)
        ] = choice
        dump(self.annotated, self.annotated_path)

    def discard_choice(self, record_id, pos):
        pos = int(pos)
        if record_id in self.annotated:
            if pos in self.annotated[record_id]:
                del self.annotated[record_id][pos]
                dump(self.annotated, self.annotated_path)
            else:
                log.warning(f"{pos} not found in {record_id}")
        else:
            log.warning(f"{record_id} not found in annotations.")

    def save(self):
        if self.cache is not None:
            start = time.perf_counter()
            dump(self.cache, self.cache_path)
            end = time.perf_counter()
            print(f"Dumped cache in {end - start:0.4f} seconds")
        if self.unresolved is not None:
            dump(pd.DataFrame.from_dict(self.unresolved), f"{self.name}_unresolved.csv")


class Segmentizer(Annotator):
    def __init__(
        self,
        segments=None,
        file=None,
        profile=None,
        tokenizer=None,
        ignore=[],
        delete=[],
        target="IPA",
        tokenize=True,
        name="segmentizer",
        word_sep=" ",  # how to separate words?
        parse_col="Orthographic",  # what field should be parsed?
        output_col="IPA",  # what field should be written to?
        complain=True,  # complain about untransliterable segments
        **kwargs,
    ):
        self.name = name
        self.ignore = ignore
        self.delete = delete
        if file:
            self.segments = load(kwargs["file"])
        elif segments:
            self.segments = segments
        if not self.segments:
            self.profile = profile
        elif profile:
            self.segments = []
            self.profile = Profile(
                *self.segments
                + [{"Grapheme": ig, self.convert_col: ig} for ig in self.ignore]
                + [{"Grapheme": de, self.convert_col: ""} for de in self.delete]
            )
        if tokenizer:
            self.tokenizer = tokenizer
        else:
            self.tokenizer = Tokenizer(self.profile)

    def parse_string(self, input_str):
        if self.tokenize:
            res = re.sub(" +", " ", self.tokenizer(input_str, column=self.target))
            if self.complain and "�" in res:
                log.warning(f"Could not convert {input_str}: {res}")
            return res
        res = self.tokenizer(
            input_str,
            column=self.convert_col,
            segment_separator="",
            separator=self.word_sep,
        )
        if self.complain and "�" in res:
            log.warning(f"Could not convert {input_str}: {res}")
        return res

    def parse(self, record):
        if isinstance(record[self.parse_col], list):
            record[self.parse_col] = [
                self.parse_string(x) for x in record[self.parse_col]
            ]
        else:
            record[self.output_col] = self.parse_string(record[self.parse_col])
        return record
