import logging
from pathlib import Path

import pygraid
import questionary
from humidifier import humidify
from writio import dump, load

from lingcorp.annotator import Annotator
from lingcorp.config import GRAID_KEY, ID_KEY
from lingcorp.helpers import (
    choose_from_list,
    favorite,
    highlight_list,
    human_sort,
    pad_ex,
    print_record,
)

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


def repr_wf(wf):
    return f"{wf['wf']} '{wf['gloss']}'"


class CliAnnotator:
    fieldnames = {
        "id": ID_KEY,
        "obj": "obj",
        "gloss": "gls",
        "gramm": "grm",
        "pos": "pos",
        "trans": "ftr",
        "text": "txt",
    }
    files = {"cache": {}}

    def __init__(self, name="unnamed", interactive=True, **kwargs):
        self.name = name
        self.interactive = interactive
        self.annotated_path = Path(f"{self.name}.yaml")
        self.annotated = load(self.annotated_path)
        self.data_setup(**kwargs)

    def parse(self, record):
        """Placeholder function. Returns a record with added data"""
        return record

    def data_setup(self, **kwargs):
        for col, val in self.fieldnames.items():
            setattr(self, col + "_key", kwargs.get(col + "_key", val))

        for f, default in self.files.items():
            fpath = f"{f}_path"
            if fpath in kwargs:
                setattr(self, fpath, Path(kwargs[fpath]))
            else:
                setattr(self, fpath, Path(f"{self.name}_{f}.yaml"))
            if f in kwargs:
                setattr(self, f, kwargs[f])
            else:
                if getattr(self, fpath).is_file():
                    setattr(self, f, type(default)(load(getattr(self, fpath))))
                else:
                    setattr(self, f, default)

    def delete_annotation(self, record_id):
        if record_id in self.annotated:
            del self.annotated[record_id]
            dump(self.annotated, self.annotated_path)
        else:
            log.warning(f"Annotator {self.name}: no annotation found for {record_id}")


class RecordAnnotator(CliAnnotator):
    target = None

    def __init__(self, fix=False, target=None, **kwargs):
        self.fix = fix
        self.path = Path(f"{self.filename}.yaml")
        if self.path.is_file():
            self.data = load(self.path)
        else:
            self.data = {}
        if target:
            self.target = target
        self.data_setup()

    def print_record(self, record, **kwargs):
        print_record(record, **kwargs)

    def parse(self, record):
        data = self.data.get(record[ID_KEY], None)
        if data is None or self.fix:
            self.print_record(record)
            data = questionary.text(self.prompt, default=data or "").ask()
            self.data[record[ID_KEY]] = data
        record[self.target] = data or ""
        dump(self.data, self.path)
        return record

    def write(self):
        dump(self.data, self.path)


class WordAnnotator(CliAnnotator):
    files = {"ignore": set(), "cache": {}, "annotated": {}, "skip": []}

    def __init__(self, fix=False, interactive=True, parse_col="obj", **kwargs):
        self.data_setup(**kwargs)
        self.parse_col = parse_col
        self.fix = fix
        self.interactive = interactive

    def save(self):
        sorted_dic = sorted(
            self.annotated.items(), key=lambda item: human_sort(item[0])
        )
        sorted_dic = {k: v for (k, v) in sorted_dic if v}
        dump(sorted_dic, self.annotated_path)

    def identify(self, values):
        """A method for generating word identifiers"""
        return values[0] + ":" + values[1]

    def cache_suggestion(self, value):
        if value in self.cache:
            return favorite(self.cache[value])
        return ""

    def is_target(self, s):
        if s in self.ignore:
            return False
        return True

    def suggestion(self, wf):
        """A method for filling in some information into the prompt field"""
        return ""

    def find_suggestion(self, value, rec):
        return self.cache_suggestion(value) or self.suggestion(rec)

    def prompt_at_position(self, ex, pos, prompt, pre_fill=""):
        print_record(ex, highlight_pos=pos)
        # for i in range(0, pos):
        #     lines = [len(line[i]) for key, line in ex.items() if key in ["obj", "gls", "pos", "grm"]]
        #     padding += " " * max(lines) + " "
        # res = input(padding + prompt)
        res = questionary.text(prompt, default=pre_fill).ask()
        return res

    def prompt_at_position_old(self, ex, pos, prompt):
        print_record(ex, highlight_pos=pos)
        padding = ""
        for i in range(0, pos):
            lines = [
                len(line[i])
                for key, line in ex.items()
                if key in ["obj", "gls", "pos", "grm"]
            ]
            padding += " " * max(lines) + " "
        res = input(padding + prompt)
        return res

    def parse(self, rec):
        self.annotated.setdefault(rec[ID_KEY], {})
        rec[self.output_col] = ["" for x in range(0, len(rec[self.parse_col]))]
        for i, values in enumerate(zip(*rec.values())):
            wf_id = self.identify(values)
            if wf_id in self.ignore:
                rec[self.output_col][i] = ""
                continue
            retrieved = False
            if i in self.annotated[rec[ID_KEY]]:
                if wf_id in self.annotated[rec[ID_KEY]][i]:
                    print(f"oh hello i just found {wf_id} in my annotations!")
                    answer = self.annotated[rec[ID_KEY]][i][wf_id]
                    rec[self.output_col][i] = answer
                    retrieved = True
                    if self.fix:
                        answer = self.prompt_at_position(
                            rec, i, prompt=f"Annotate?", pre_fill=answer
                        )
            if not retrieved:
                if self.is_target(wf_id):
                    answer = self.prompt_at_position(
                        rec,
                        i,
                        prompt=f"Annotate?",
                        pre_fill=self.find_suggestion(wf_id, rec),
                    )
                    if answer == "ignore":  # annotate nothing and never ask again
                        self.ignore.add(wf_id)
                    if answer not in ["ignore", "skip"]:
                        self.cache.setdefault(wf_id, [])
                        self.cache[wf_id].append(answer)
                        self.annotated[rec[ID_KEY]][i].setdefault({})
                        self.annotated[rec[ID_KEY]][i][wf_id] = answer
                        rec[self.output_col][i] = answer
                    else:
                        rec[self.output_col][i] = ""
        dump(self.annotated, self.annotated_path)  # keep updates
        dump(self.cache, self.cache_path)  # keep updates
        dump(list(self.ignore), self.ignore_path)  # keep updates
        return rec


class GraidAnnotator(WordAnnotator):
    pass


class RefINDAnnotator(WordAnnotator):
    ref_count = {}

    files = {"entities": {}, "cache": {}}

    def __init__(
        self,
        name: str = "refind",
        graid_key: str = "graid",
        output_col: str = "refind",
        **kwargs,
    ):
        self.output_col = output_col
        self.name = name
        # self.entities_path = Path(f"{self.name}_entities.yaml")
        # self.entities = load(self.entities_path)
        # self.text_key = "txt"
        self.data_setup(**kwargs)
        for k in self.entities.keys():
            self.ref_count[k] = 0
        self.ref_count[""] = 0
        self.annotated_path = "refind.yaml"
        self.annotated = load(self.annotated_path)

    def sort(self, entities, graid):
        if graid in self.cache:
            sort_order = self.ref_count.copy()
            # print(sort_order)
            sort_order[favorite(self.cache[graid])] = 100000
            sorted_entities = {}
            for k, v in sorted(
                entities.items(),
                key=lambda val: sort_order.get(val[0], 0),
                reverse=True,
            ):
                sorted_entities[k] = v
            return sorted_entities
        return entities

    def parse(self, rec):
        if GRAID_KEY not in rec:
            log.error(f"No field '{GRAID_KEY}'. Please add GRAID annotations first.")
            exit()
        self.annotated.setdefault(rec[ID_KEY], {})
        rec[self.output_col] = []
        for i, p_annotation in enumerate(rec[GRAID_KEY]):
            answers = []
            for ann in p_annotation.split(" "):
                if pygraid.is_referential(ann):
                    retrieved = False
                    if i in self.annotated[rec[ID_KEY]]:
                        if ann in self.annotated[rec[ID_KEY]][i]:
                            answer = self.annotated[rec[ID_KEY]][i][ann]
                            retrieved = True
                    if not retrieved:
                        if rec[self.text_key] in self.entities:
                            sorted_entities = self.sort(
                                self.entities[rec[self.text_key]], ann
                            )
                        else:
                            sorted_entities = []
                            self.entities[rec[self.text_key]] = {}
                        log.warning(sorted_entities)
                        print_record(rec, highlight_pos=i)
                        answer = choose_from_list(
                            list(sorted_entities) + ["new entity", "nonreferential"],
                            f"Identify referent in {ann} ({rec['obj'][i]} '{rec['gls'][i]}'):",
                        )
                        if answer == "new entity":
                            ent_name = input("Name?")
                            ent_id = humidify(ent_name, key="entities")
                            user_id = input(f"Abbreviation? (default: {ent_id})")
                            ent_id = (
                                user_id or ent_id
                            )  # f"{rec[self.text_key]}-{user_id or ent_id}"
                            self.entities[rec[self.text_key]][ent_id] = ent_name
                            answer = ent_id
                        elif answer == "nonreferential":
                            answer = ""
                        self.annotated[rec[ID_KEY]].setdefault(i, {})
                        self.annotated[rec[ID_KEY]][i][ann] = answer
                        self.save()
                    else:
                        self.annotated[rec[ID_KEY]].setdefault(i, {})
                        self.annotated[rec[ID_KEY]][i][ann] = answer
                    answers.append(answer)
                    self.cache.setdefault(ann, [])
                    self.cache[ann].append(answer)
                    self.ref_count.setdefault(answer, 0)
                    self.ref_count[answer] += 1
            rec[self.output_col].append(" ".join(answers))
        return rec


class UniParser(CliAnnotator):
    files = {"annotated": {}, "cache": {}, "disambiguation": {}}

    punctuation: ['"', ","]

    def __init__(
        self,
        name="unnamed",
        handle_ambiguity=None,
        interactive=True,
        use_cache=False,
        **kwargs,
    ):
        self.name = name
        self.interactive = interactive
        self.handle_ambiguity = handle_ambiguity
        self.trans_key = kwargs.get("trans_key", "Translation")
        self.use_cache = use_cache
        if not use_cache:
            del self.files["cache"]
        self.analyzer = kwargs.get("analyzer", ".")
        self.word_sep = kwargs.get("word_sep", " ")
        self.parse_col = kwargs.get(
            "parse_col", "transcription"
        )  # the field to be parsed
        self.justify_choices: bool = kwargs.get("justify_choices", False)
        if "punctuation" in kwargs:
            self.punctuation = kwargs["punctuation"]
        if "uniparser_fields" in kwargs:
            self.uniparser_fields = kwargs["uniparser_fields"]
        self.data_setup(**kwargs)
        self.unparsable = []
        self.unparsable_path = Path(f"{self.name}_unparsable.txt")
        self.frequency_counts = {}
        for pos in self.annotated.values():
            for form, gloss in pos.items():
                self.step_freq_counter(form, gloss)
        self.disamb_path = kwargs.get("disamb_path", self.name + "_disamb.yaml")
        self.disamb_path = Path(self.disamb_path)
        if self.disamb_path.is_file():
            self.disamb_answers = load(self.disamb_path)
        if not self.unparsable_path:
            self.unparsable_path = self.name + "_unparsable.txt"
        if isinstance(
            self.analyzer, str
        ):  # initialize a uniparser analyser from a putative path
            ana_path = Path(self.analyzer)
            self.analyzer = Analyzer()
            for att, filename in [
                ("lexFile", "lexemes.txt"),
                ("paradigmFile", "paradigms.txt"),
                ("delAnaFile", "bad_analyses.txt"),
                ("cliticFile", "clitics.txt"),
            ]:
                filepath = ana_path / filename
                if filepath:
                    setattr(self.analyzer, att, filepath)
            self.analyzer.load_grammar()

    def _get_field(self, wf, field):
        field_dic = {
            "wf": wf["wf"],
            "wfGlossed": wf["wfGlossed"],
            "gloss": wf["gloss"],
            "lemma": wf["lemma"],
            "gramm": wf["gramm"],
        }
        if field not in field_dic:
            for f, v in wf.otherData:
                if f == field:
                    return v
            return ""
        return field_dic[field]

    def _compare_ids(self, analysis_list):
        id_list = []
        for analysis in analysis_list:
            ids = sorted(ID_KEY.split(","))
            if len(id_list) == 0:
                id_list = [ids]
            elif ids not in id_list:
                return False
        return True

    def parse_word(self, word, **kwargs):
        return self.analyzer.analyze_words(word, **kwargs)

    def write(self):
        unparsable_counts = [
            (i, len(list(c))) for i, c in groupby(sorted(self.unparsable))
        ]
        unparsable_counts = sorted(unparsable_counts, key=lambda x: x[1], reverse=True)
        # self.unparsable = [f"{x}\t{y}" for x, y in unparsable_counts]
        dump(
            "\n".join([f"{x}\t{y}" for x, y in unparsable_counts]), self.unparsable_path
        )
        # dump("\n\n".join(self.ambiguous), self.ambiguous_path)
        if self.interactive:
            dump(self.annotated, self.annotated_path)
        if self.use_cache:
            dump(self.cache, self.cache_path)
        if self.justify_choices:
            dump(self.disamb_answers, self.disamb_path)

    def step_freq_counter(self, word_form, objgloss):
        log.debug(f"stepping it up for {word_form}", objgloss)
        self.frequency_counts.setdefault(word_form, [])
        self.frequency_counts[word_form].append(objgloss)

    def get_freq_suggestion(self, word_form):
        if word_form not in self.frequency_counts:
            return None
        return favorite(self.frequency_counts[word_form])

    def parse(self, record):
        log.debug(f"""Parsing {record[self.parse_col]} ({record[ID_KEY]})""")
        if self.trans_key not in record:
            log.debug(f"No column {self.trans_key}, adding")
            record[self.trans_key] = ""
        for field_name in self.uniparser_fields:
            if field_name in record:
                log.error(f"Field '{field_name}' already exists")
                exit()
        added_fields = {}
        for field_name in self.uniparser_fields:
            added_fields[field_name] = []
        unparsable = []
        ambiguous = {}
        annotated_analyses = {}
        parse_target = record[self.parse_col].strip(self.word_sep).split(self.word_sep)
        if not self.use_cache or not record[ID_KEY] in self.cache:
            all_analyses = self.parse_word(parse_target)
            print("ANALYSES", all_analyses)
            all_analyses = [[x.to_json() for x in y] for y in all_analyses]
        if self.use_cache:
            if record[ID_KEY] in self.cache:
                all_analyses = self.cache[record[ID_KEY]]
            else:
                self.cache[record[ID_KEY]] = all_analyses
        for word_count, wf_analysis in enumerate(all_analyses):
            past_choice = None
            if len(wf_analysis) > 1:
                found_past = False
                word_form = wf_analysis[0]["wf"]
                ambiguous[word_form] = []
                for potential_analysis in wf_analysis:
                    ambiguous[word_form].append(str(potential_analysis))
                    if record[ID_KEY] in self.annotated:
                        if potential_analysis["gloss"] == self.annotated[
                            record[ID_KEY]
                        ].get(word_count, {word_form: None}).get(word_form):
                            log.debug(
                                f"""Disambiguated: analysis {repr_wf(potential_analysis)} in {record[ID_KEY]}"""
                            )
                            if self.handle_ambiguity == "keep":
                                past_choice = potential_analysis["gloss"]
                            analysis = potential_analysis
                            found_past = True
                if not found_past:
                    if self.interactive:
                        obj_choices = []
                        gloss_choices = []
                        suggestion = self.get_freq_suggestion(word_form)
                        reordered_analyses = []
                        best_guess = None
                        for cand_ana in wf_analysis:
                            if (
                                f"{cand_ana['wfGlossed']}:{cand_ana['gloss']}"
                                == suggestion
                            ):
                                best_guess = cand_ana
                            else:
                                reordered_analyses.append(cand_ana)

                        if best_guess:
                            reordered_analyses = [best_guess] + reordered_analyses
                        for analysis in reordered_analyses:
                            potential_obj = added_fields["wfGlossed"] + [
                                analysis["wfGlossed"]
                            ]
                            potential_gloss = added_fields["gloss"] + [
                                analysis["gloss"]
                            ]
                            obj_choices.append(potential_obj)
                            gloss_choices.append(potential_gloss)
                        answers = []
                        for i, (obj, gloss) in enumerate(
                            zip(
                                obj_choices,
                                gloss_choices,
                            )
                        ):
                            pad_obj, pad_gloss = pad_ex(obj, gloss, as_tuple=True)
                            answers.append(
                                f"({i+1}) " + pad_obj + "\n       " + pad_gloss
                            )
                        answers.append("I'd rather not choose.")
                        andic = {answer: i for i, answer in enumerate(answers)}
                        print(
                            self.word_sep.join(
                                highlight_list(parse_target, word_count)
                            ),
                            f"‘{record[self.trans_key]}’",
                            sep="\n",
                        )
                        choice = questionary.select(
                            "",
                            choices=answers,
                        ).ask()
                        if choice == "I'd rather not choose.":
                            analysis = Wordform(self.analyzer.g).to_json()
                            analysis["wf"] = reordered_analyses[0]["wf"]
                        else:
                            analysis = reordered_analyses[andic[choice]]
                            annotated_analyses[word_count] = [
                                word_form,
                                analysis["gloss"],
                            ]
                            self.step_freq_counter(word_form, analysis["gloss"])
                            if self.justify_choices:
                                # print("you chose", analysis)
                                # print("in the sentence", record[ID_KEY])
                                # print("at position", word_count)
                                # print("instead of", wf_analysis)
                                motivation = questionary.text("Why?").ask()
                                self.disamb_answers.setdefault(analysis["wf"], {})
                                self.disamb_answers[analysis["wf"]][
                                    f"{record[ID_KEY]}-{word_count}"
                                ] = {
                                    "choice": analysis,
                                    "alternatives": [
                                        ana
                                        for ana in reordered_analyses
                                        if ana["gloss"] != analysis["gloss"]
                                    ],
                                    "motivation": motivation,
                                }
                    elif self.handle_ambiguity is None:
                        only_polysemy = self._compare_ids(wf_analysis)
                        analysis = Wordform(self.analyzer.g).to_json()
                        analysis["id"] = ""
                        analysis["wf"] = wf_analysis[0]["wf"]
                        for field_name in self.uniparser_fields:
                            analysis[field_name] = "?"
                        if only_polysemy:
                            analysis["wfGlossed"] = wf_analysis[0]["wfGlossed"]
                    elif self.handle_ambiguity == "hide":
                        suggestion = self.get_freq_suggestion(word_form)
                        analysis = wf_analysis[0]
                    else:
                        analysis = None
            elif len(wf_analysis) == 1:
                log.debug(f"Using unambiguous analysis {repr_wf(wf_analysis[0])}")
                analysis = wf_analysis[0]
            else:
                print(word_count)
                print(record)
            if past_choice or not analysis:
                if not analysis:
                    for field_name in self.uniparser_fields:
                        if field_name in ["wf", "wfGlossed"]:
                            added_fields[field_name].append(wf_analysis[0]["wf"])
                        elif field_name in ["gramm"]:  # this is a list
                            added_fields[field_name].append([""])
                        elif field_name == "anas":
                            res = {"?": "?"}
                            for wf in wf_analysis:
                                res[wf["gloss"]] = {
                                    self.uniparser_fields[k]: v
                                    for k, v in wf.items()
                                    if k in self.uniparser_fields
                                }
                            added_fields["anas"].append(res)
                        elif field_name == "ana":
                            added_fields[field_name].append("?")
                        else:
                            added_fields[field_name].append("")
                else:
                    for field_name in self.uniparser_fields:
                        if field_name == "ana":
                            added_fields[field_name].append(past_choice)
                        elif field_name == "anas":
                            res = {"?": "?"}
                            for wf in wf_analysis:
                                res[wf["gloss"]] = {
                                    self.uniparser_fields[k]: v
                                    for k, v in wf.items()
                                    if k in self.uniparser_fields
                                }
                            added_fields["anas"].append(res)
                        else:
                            added_fields[field_name].append(
                                analysis.get(field_name, "")
                            )
            elif analysis["wfGlossed"] == "":
                unparsable.append(analysis["wf"])
                for field_name in self.uniparser_fields:
                    if field_name in ["wf", "wfGlossed"]:
                        added_fields[field_name].append(analysis["wf"])
                    elif field_name in ["gramm"]:  # this is a list
                        added_fields[field_name].append(["***"])
                    elif field_name in ["anas"]:
                        added_fields[field_name].append({})
                    else:
                        added_fields[field_name].append("***")
                    if field_name not in analysis:
                        analysis[field_name] = ""
            else:
                for field_name in self.uniparser_fields:
                    added_fields[field_name].append(analysis.get(field_name, ""))
        # pretty_record = (
        #     pad_ex(
        #         added_fields["wfGlossed"],
        #         added_fields["gloss"],
        #         [",".join(x) for x in added_fields["gramm"]],
        #     )
        #     + "\n"
        #     + "‘"
        #     + record[self.trans_key]
        #     + "’\n"
        # )
        # print(pretty_record)
        # if len(ambiguous) > 0:
        #     self.ambiguous.append(
        #         "\n".join(
        #             [
        #                 pretty_record,
        #                 "\nAmbiguities:",
        #                 "\n".join(
        #                     [
        #                         f"{wf}:\n {' '.join(forms)}"
        #                         for wf, forms in ambiguous.items()
        #                     ]
        #                 ),
        #             ]
        #         )
        #     )
        # if len(unparsable) > 0:
        #     log.warning(
        #         f"Unparsable: {', '.join(unparsable)} in {record[ID_KEY]}:\n{pretty_record}"
        #     )
        #     self.unparsable.extend(unparsable)
        for field_name, output_col in self.uniparser_fields.items():
            record[output_col] = added_fields[field_name]
        if self.interactive and annotated_analyses:
            self.annotated.setdefault(record[ID_KEY], {})
            for i, data in annotated_analyses.items():
                self.annotated[record[ID_KEY]][i] = data
            dump(self.annotated, self.annotated_path)
            if self.justify_choices:
                dump(self.disamb_answers, self.disamb_path)
        return record
