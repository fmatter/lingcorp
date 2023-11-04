from lingcorp.config import INPUT_DIR, OUTPUT_DIR
from lingcorp.helpers import get_pos, load_data, run_pipeline


def parse_df(parser_list, out_f, df, interactive):
    full_df = pd.read_csv(OUTPUT_DIR / out_f, index_col=ID_COL, keep_default_na=False)
    parsed_dfs = [full_df]
    for parser in parser_list:
        output = []
        parser.interactive = interactive
        for idx, record in df.iterrows():
            if idx in full_df.index:
                log.warning(f"Overwriting existing analysis of record {idx}")
                parser.clear(idx)
                full_df.drop([idx], inplace=True)
            output.append(parser.parse(record))
        parsed_dfs.append(pd.DataFrame.from_dict(output))
        parser.write()
    df = pd.concat(parsed_dfs)
    df = df.fillna("")
    df.index.name = ID_COL
    df.to_csv(OUTPUT_DIR / out_f)


def parse_csvs(pipeline, out_f, filter_params=None, pos_list=None):
    fields = {x["key"]: x for x in pipeline if isinstance(x, dict)}
    data = load_data(
        rename={
            "Primary_Text": "ort",
            "Translated_Text": "oft",
            "Speaker_ID": "spk",
            "Text_ID": "txt",
        },
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
