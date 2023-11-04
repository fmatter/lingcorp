from pathlib import Path

from lingcorp import Cleaner, Parser, Tokenizer, UniParser

# You can define your own parser classes here, inheriting from Parser

# e.g. morpho = UniParser(analyzer="path/to/analyzer", name="a_name")

# Add fields and parsers to the pipeline
pipeline = []

# Configuration
REC_LINK = "http://url/to/your/records/{rec_id}"
INPUT_FILE = "all"
OUTPUT_FILE = f"parsed.csv"
FILTER = {}
AUDIO_PATH = Path("path/to/your/audio/folder")
