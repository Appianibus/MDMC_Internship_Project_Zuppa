import hashlib
import logging
import os
import time
from pathlib import Path
from types import FrameType
from typing import Any

import anthropic
import pandas as pd
import regex
import wordninja
import yaml
from dotenv import load_dotenv
from spellchecker import SpellChecker

load_dotenv()

_DOI_LIST_CACHE = None
_CONFIG_CACHE = None
_CLASSIFICATION_SCHEMA_DAS_CACHE = None
_CLASSIFICATION_SCHEMA_CAS_MCA_CACHE = None
_CLASSIFICATION_SCHEMA_CAS_ETA_CACHE = None
_CAS_PROMPT_CACHE = None



def load_doi_list():
    global _DOI_LIST_CACHE

    if _DOI_LIST_CACHE is None:
        config = load_config()
        path = Path(__file__).resolve().parent
        while not (path / "pyproject.toml").exists():
            path = path.parent
        document_registry_csv = path / config["paths"]["registries"] / "document_registry.csv"

        try:
            df_document = pd.read_csv(document_registry_csv)
            _DOI_LIST_CACHE = set(df_document["doc_doi"].dropna())
        except FileNotFoundError:
            raise FileNotFoundError(
                f"{document_registry_csv} not found - ensure it's the correct document registry path"
            )

    return _DOI_LIST_CACHE


def load_config():

    global _CONFIG_CACHE

    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    path = Path(__file__).resolve().parent

    while not (path / "pyproject.toml").exists():
        if path.parent == path:
            raise FileNotFoundError("Could not find pyproject.toml")

        path = path.parent

    config_path = path / "src" / "config" / "config.yaml"

    with open(config_path) as f:
        _CONFIG_CACHE = yaml.safe_load(f)

    return _CONFIG_CACHE

def load_classification_schema_DAS():

    global _CLASSIFICATION_SCHEMA_DAS_CACHE

    if _CLASSIFICATION_SCHEMA_DAS_CACHE is not None:
        return _CLASSIFICATION_SCHEMA_DAS_CACHE
    
    path = Path(__file__).resolve().parent

    while not (path / "pyproject.toml").exists():
        if path.parent == path:
            raise FileNotFoundError("Could not find pyproject.toml")

        path = path.parent

    classification_schema_path = path / "src" / "config" / "classification_schema_DAS.yaml"

    with open(classification_schema_path) as f:

        _CLASSIFICATION_SCHEMA_DAS_CACHE = yaml.safe_load(f)

    return _CLASSIFICATION_SCHEMA_DAS_CACHE     

def load_classification_schema_CAS_MCA():

    global _CLASSIFICATION_SCHEMA_CAS_MCA_CACHE

    if _CLASSIFICATION_SCHEMA_CAS_MCA_CACHE is not None:
        return _CLASSIFICATION_SCHEMA_CAS_MCA_CACHE
    
    path = Path(__file__).resolve().parent

    while not (path / "pyproject.toml").exists():
        if path.parent == path:
            raise FileNotFoundError("Could not find pyproject.toml")

        path = path.parent

    classification_schema_MCA_path = path / "src" / "config" / "classification_schema_CAS_MCA.yaml"

    with open(classification_schema_MCA_path) as f:

        _CLASSIFICATION_SCHEMA_CAS_MCA_CACHE = yaml.safe_load(f)

    return _CLASSIFICATION_SCHEMA_CAS_MCA_CACHE

def load_classification_schema_CAS_ETA():

    global _CLASSIFICATION_SCHEMA_CAS_ETA_CACHE

    if _CLASSIFICATION_SCHEMA_CAS_ETA_CACHE is not None:
        return _CLASSIFICATION_SCHEMA_CAS_ETA_CACHE
    
    path = Path(__file__).resolve().parent

    while not (path / "pyproject.toml").exists():
        if path.parent == path:
            raise FileNotFoundError("Could not find pyproject.toml")

        path = path.parent

    classification_schema_ETA_path = path / "src" / "config" / "classification_schema_CAS_ETA.yaml"

    with open(classification_schema_ETA_path) as f:

        _CLASSIFICATION_SCHEMA_CAS_ETA_CACHE = yaml.safe_load(f)

    return _CLASSIFICATION_SCHEMA_CAS_ETA_CACHE


def load_CAS_prompt_template():
    global _CAS_PROMPT_CACHE

    if _CAS_PROMPT_CACHE is not None:
        return _CAS_PROMPT_CACHE

    path = Path(__file__).resolve().parent
    while not (path / "pyproject.toml").exists():
        if path.parent == path:
            raise FileNotFoundError("Could not find pyproject.toml")
        path = path.parent

    prompt_path = path / "src" / "config" / "CAS_prompt.txt"
    _CAS_PROMPT_CACHE = prompt_path.read_text(encoding="utf-8")

    return _CAS_PROMPT_CACHE


"""UTILS FOR DATA CLEANING"""


def split_glued_words(text: str):

    parts = regex.split(r"(\W+)", text)

    unglued_text = []

    for part in parts:
        if regex.match(r"(\w+)", part):
            unglued_text.append(" ".join(wordninja.split(part)))

        else:
            unglued_text.append(part)

    return "".join(unglued_text)


def remove_hyphen(text: str):

    pattern = r"(\w*)-\s*\n*\s*(\w*)"

    spell = SpellChecker()

    def replace_hyphen(match: regex.Match[str]):

        first_half = match.group(1)

        second_half = match.group(2)

        joined_word = first_half + second_half

        if joined_word.lower() in spell or len(joined_word) < 3:
            return joined_word

        hyphenated = f"{first_half}-{second_half}"

        if hyphenated.lower() in spell:
            return hyphenated

        return joined_word

    return regex.sub(pattern, replace_hyphen, text)


def add_period(text: str):

    text = text.rstrip()

    if not text.endswith("."):
        text += "."

    return text.strip()


def single_line_text(text: str):

    text = regex.sub(r"\s+", " ", text)

    return text.strip()


def mask_hyperlink(text: str):

    hyperlink = regex.compile(r"https?://\S+")

    urls = []

    def replace_hyperlink(match: regex.Match[str]):

        urls.append(match.group(0))

        return f"{len(urls) - 1}"

    masked_text = hyperlink.sub(replace_hyperlink, text)

    return masked_text, urls


def unmask_hyperlink(masked_text: str, urls: list):

    unmasked_text = masked_text

    for i, url in enumerate(urls):
        unmasked_text = unmasked_text.replace(f"{i}", url)

    return unmasked_text


def repair_hyperlink(text: str):

    hyperlink = regex.compile(r"(https?://\S*?)\s+(\S+)")

    def test_continuation_and_repair(match: regex.Match[str]):

        base = match.group(1)

        continuation = match.group(2)

        if (
            continuation.startswith("/")
            or continuation.startswith("?")
            or continuation.startswith("#")
            or base.endswith("/")
        ):
            full_link = base + continuation

            return full_link

        return match.group(0)

    repaired_link_text = hyperlink.sub(test_continuation_and_repair, text)

    return repaired_link_text


def remove_ligatures(text: str):

    config = load_config()

    ligature_map = config["data_cleaning"]["ligatures"]

    for ligature, replacement in ligature_map.items():
        text = text.replace(ligature, replacement)

    return text

def remove_leading_whitespace(text: str) -> str:

    return text.lstrip()


def remove_leading_period(text: str) -> str:

    if text.startswith("."):

        return text[1:].lstrip()
    
    return text

# the following function compute hashes for a specific file
# hashes are unique to each files and are used to build a stable index


def compute_hashes(filename: str | Path, salt: str | bool = False):

    h = hashlib.sha1()

    if salt:
        h.update(salt.encode())

    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)

    return h.hexdigest()


"""utils to load and save registries"""


def load_document_registry(
    path: str | Path | None = None):

    if path is None: 

        path = Path(__file__).resolve().parent

        while not (path / "pyproject.toml").exists():

            if path.parent == path:

                raise FileNotFoundError("Could not find pyproject.toml")

            path = path.parent

        registries_path = path / "data/metadata/registries/document_registry.csv"

    else:
        registries_path = Path(path)

    df = pd.read_csv(registries_path)

    return df


def load_base_registry(
    path: str | Path | None = None):

    if path is None: 

        path = Path(__file__).resolve().parent

        while not (path / "pyproject.toml").exists():

            if path.parent == path:

                raise FileNotFoundError("Could not find pyproject.toml")

            path = path.parent

        registries_path = path / "data/metadata/registries/base_output_registry.csv"

    else:
        registries_path = Path(path)

    df = pd.read_csv(registries_path)

    return df


def load_raw_registry(
    path: str | Path | None = None):

    if path is None: 

        path = Path(__file__).resolve().parent

        while not (path / "pyproject.toml").exists():

            if path.parent == path:

                raise FileNotFoundError("Could not find pyproject.toml")

            path = path.parent

        registries_path = path / "data/metadata/registries/output_subregistries/raw_file_output_registry.csv"

    else:
        registries_path = Path(path)

    df = pd.read_csv(registries_path)

    return df


def load_extraction_registry(
    path: str | Path | None = None):

    if path is None: 

        path = Path(__file__).resolve().parent

        while not (path / "pyproject.toml").exists():

            if path.parent == path:

                raise FileNotFoundError("Could not find pyproject.toml")

            path = path.parent

        registries_path = path / "data/metadata/registries/output_subregistries/extraction_output_registry.csv"

    else:
        registries_path = Path(path)

    df = pd.read_csv(registries_path)

    return df


def load_embedding_registry(
    path: str | Path | None = None):

    if path is None: 

        path = Path(__file__).resolve().parent

        while not (path / "pyproject.toml").exists():

            if path.parent == path:

                raise FileNotFoundError("Could not find pyproject.toml")

            path = path.parent

        registries_path = path / "data/metadata/registries/output_subregistries/embedding_output_registry.csv"

    else:
        registries_path = Path(path)

    df = pd.read_csv(registries_path)

    return df


def load_DAS_classification_registry(
    path: str | Path | None = None):

    if path is None: 

        path = Path(__file__).resolve().parent

        while not (path / "pyproject.toml").exists():

            if path.parent == path:

                raise FileNotFoundError("Could not find pyproject.toml")

            path = path.parent

        registries_path = path / "data/metadata/registries/output_subregistries/DAS_classification_output_registry.csv"

    else:
        registries_path = Path(path)

    df = pd.read_csv(registries_path)

    return df

def load_CAS_classification_registry(
    path: str | Path | None = None):

    if path is None: 

        path = Path(__file__).resolve().parent

        while not (path / "pyproject.toml").exists():

            if path.parent == path:

                raise FileNotFoundError("Could not find pyproject.toml")

            path = path.parent

        registries_path = path / "data/metadata/registries/output_subregistries/CAS_classification_output_registry.csv"

    else:
        registries_path = Path(path)

    df = pd.read_csv(registries_path)

    return df

def save_registry(df: pd.DataFrame, path: Path | str):

    df.to_csv(path, index=False)

    return df 

"""general utils"""


def timeout_handler(signum: int, frame: FrameType | None):

    raise TimeoutError("PDFparsing timed out)")


# function to add output_shas from one table to another based on doi mapping
# only reading shas for extraction outputs for that specific dois
# checking for possible duplicates
# used to associate an output with a known doi in one table, with its corresponding sha from another table


def add_extraction_shas(
    df_sha: pd.DataFrame,
    df_no_shas: pd.DataFrame,
    sha_column_name: str,
    doi_column_name: str,
    registry_path: str,
    version_object: Any,
):

    df_sha = df_sha.loc[
        (df_sha["output_type"] == "extracted section") & (df_sha["pipeline_version"] == version_object.pipeline_version)
    ]

    dupes = df_sha[df_sha.duplicated(subset=[doi_column_name], keep=False)]

    if len(dupes) > 0:
        print(f"Warning: {len(dupes)} dulpicate dois found in extraction registry")

    sha_map = df_sha.set_index(doi_column_name)[sha_column_name].to_dict()

    df_no_shas[sha_column_name] = df_no_shas[doi_column_name].map(sha_map)

    save_registry(df_no_shas, registry_path)

    return df_no_shas

section_reparation_logger = logging.getLogger()

"""UTILS FOR SECTION REPARATION"""

def quick_interruption_check(text: str):

    # with the following function we check if there are signs the section was interrupted
    # this is a first check that if passed, triggers a bunch of other computationally heavy functions
    # i.e. API calls to a LLM for section reparation

    config = load_config()

    prepositions = config["section_interruptors"]["prepositions"]

    conjunctions = config["section_interruptors"]["conjunctions"]

    context_window = 200

    context_text = text[-context_window:]

    if regex.search(r"\b123\b\s*\n\s*\n.*?Eur\. Phys", text):
        return True 
    
    if regex.search(r"\d+\s*\n\s*\n\s*\d+\s*\n\s*Page\s+\d+\s+of\s+\d+", text):
        return True

    if regex.search(r"[.!?]\s*(?:\]|\))[\s\n]*$", context_text):
        return False

    # trailing hyphen
    # unclosed (, [, or ""

    if regex.search(r"\w+-\s*$", context_text):
        return True

    if context_text.count("(") > context_text.count(")"):
        return True

    if context_text.count("[") > context_text.count("]"):
        return True

    if context_text.count('"') % 2 != 0:
        return True

    for interruption in prepositions + conjunctions:
        if regex.search(interruption, context_text, regex.IGNORECASE):
            return True

    # check if any of these words occur right after lowercase letters
    # check if any capitalized word occurs next to a lowercase word
    # check if there's any page break

    if regex.search(r"[a-z]+(?:FIG\.|TABLE|FIGURE)", text):
        return True

    if regex.search(r"[a-z]{3,}[A-Z]{3,}", text):
        return True

    if regex.search(r"---Page\d+---|Page\s*\d+\s*of\s*\d+", text):
        return True
    


    # here we might have added an additional check to see if there's sentence-ending punctuation
    # thing is, a large portion of extracted section do not have final period pre-data cleaning

    return False

def section_reparation_llm(section: str, doc_doi: str, api_key: str = None, model: str = "claude-haiku-4-5-20251001", retries: int = 3) -> str:

    if not quick_interruption_check(section):
        return section

    print("Entered interruption check")

    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")

    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not found")
    
    client = anthropic.Anthropic(api_key=api_key)

    for attempt in range(retries):

        try:

            response = client.messages.create(
                model=model ,
                max_tokens=200,
                temperature=0,
                messages=[
                    {
                        "role": "user",
                        "content": f"""The following data or code availability statement section is likely interrupted by a page break. The page break might include various artifacts like page numbers, footnotes, journal titles, etc. 
                        Please:
                        1. Identify if there's an actual interruption (missing text in the middle)
                        2. If yes, repair the section by removing the junk and reconnecting the broken parts
                        3. If no interruption, return the text as-is

                        Text: "{section}"
                        
                        ONLY respond with the repaired section or the text as-is. Do not include anything else in the reply. Do not use words that are not in the original text for reparation.
                        Use EXCLUSIVELY words that are present in the text you are being fed.""",
                    }
                ]
            )

            repaired_section = response.content[0].text.strip()

            return repaired_section
        
        except anthropic.RateLimitError:
            wait = 2 ** attempt
            section_reparation_logger.warning(f"Rate limit on attempt {attempt+1}/{retries}, retrying in {wait}s...")
            if attempt < retries - 1:
                time.sleep(wait)
            else:
                section_reparation_logger.warning("Max retries exceeded after rate limits, returning original section")
                return section

        except anthropic.APIError as e:
            section_reparation_logger.warning(f"API error on attempt {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                section_reparation_logger.warning("Max retries exceeded, returning original section")
                return section

        except Exception as e:
            section_reparation_logger.error(f"Unexpected error on attempt {attempt+1}/{retries}: {e}")
            return section
    
    return section

def create_manifest(
    primary_df: pd.DataFrame,
    df_base: pd.DataFrame,
    df_document: pd.DataFrame,
    primary_columns: list[str] | None = None,
    base_columns: list[str] | None = None,
    document_columns: list[str] | None = None,
) -> pd.DataFrame:

    primary_columns = list(dict.fromkeys(primary_columns or primary_df.columns.tolist()))
    base_columns = list(dict.fromkeys(base_columns or df_base.columns.tolist()))
    document_columns = list(dict.fromkeys(document_columns or df_document.columns.tolist()))

    df_base_manifest = df_base[base_columns].drop_duplicates("output_sha", keep="last")
    df_document_manifest = df_document[document_columns].drop_duplicates("doc_doi", keep="last")

    df_manifest = (
        primary_df[primary_columns]
        .merge(df_base_manifest, on="output_sha", how="left")
        .merge(df_document_manifest, on="doc_doi", how="left")
    )

    return df_manifest

def mask_author_comment(match: regex.Match) -> str:
    return "".join("\n" if char == "\n" else "x" for char in match.group())
