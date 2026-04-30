import json
import logging
import os
import signal
import time
from datetime import datetime
from hashlib import sha1
from pathlib import Path
from string import Template

import anthropic
import fitz
import httpx
import pandas as pd
import regex
import yaml
from tqdm import tqdm

# from PyPDF2 import PdfReader
from application.utils import (
    add_period,
    compute_hashes,
    load_base_registry,
    load_CAS_prompt_template,
    load_classification_schema_CAS_ETA,
    load_classification_schema_CAS_MCA,
    load_classification_schema_DAS,
    load_config,
    load_document_registry,
    load_doi_list,
    load_extraction_registry,
    mask_hyperlink,
    remove_hyphen,
    remove_ligatures,
    remove_leading_period,
    remove_leading_whitespace,
    repair_hyperlink,
    section_reparation_llm,
    split_glued_words,
    timeout_handler,
    unmask_hyperlink,
)
from config.config import RECORDS_DIR, REGISTRIES_DIR, SUBREGISTRIES_DIR, VersionObject, repo_relative_path


def data_ingestion(
    df_document: pd.DataFrame,
    df_base: pd.DataFrame,
    df_raw: pd.DataFrame,
    version_object: VersionObject,
    date_start: str = "2025-01-01",
    date_end: str = "2025-12-31"
):
    if datetime.fromisoformat(date_start) > datetime.fromisoformat(date_end):
        return
    r = httpx.get(
        f"https://repo.scoap3.org/api/records/?publication_year__gte={date_start}&publication_year__lte={date_end}"
    )

    metadata_pbar = tqdm(desc="Saving record metadata", unit="file")

    scanned_pbar = tqdm(desc="Scanning records", unit="file")

    pdf_download_pbar = tqdm(desc="Downloading PDF files", unit="file")

    xml_download_pbar = tqdm(desc="Scanning XML files", unit="file")

    json_data = r.json()

    doi_set = set(load_doi_list())

    today_str = datetime.now().strftime("%Y-%m-%d")

    existing_hashes = set(df_base["output_sha"])

    while True:

        for record in json_data["hits"]["hits"]:
            record_metadata = record.get("metadata", {})

            doi_data = record_metadata.get("dois", [])

            doc_doi = doi_data[0]["value"] if doi_data else None

            if doc_doi is None:
                print("Record with no DOI found, skipping")

                scanned_pbar.update(1)

                continue

            if doc_doi in doi_set:

                print("DOI already present, skipping")
                continue

            else:
                folder_doi = doc_doi.replace("/", "_")

                doi_set.add(doc_doi)

                record_dir = Path(
                    RECORDS_DIR
                    / version_object.software_version
                    / "pipeline_version"
                    / version_object.pipeline_version
                    / folder_doi
                )

                record_dir.mkdir(parents=True, exist_ok=True)

                files = record_metadata.get("_files", [])

                title_data = record_metadata.get("titles", [])

                title = title_data[0]["title"] if title_data else None

                pdf_file = next((f for f in files if f.get("filetype") == "pdf"), None)

                if pdf_file:
                    pdf_url = pdf_file["file"]

                    pdf_filename = pdf_file["key"]

                    r1 = httpx.get(pdf_url, timeout=httpx.Timeout(5.0, read=30.0))

                    if r1.status_code == 200:
                        pdf_path = f"{record_dir}/{pdf_filename}.pdf"

                        with open(pdf_path, "wb") as f:
                            f.write(r1.content)

                            pdf_download_pbar.update(1)

                        output_sha = compute_hashes(pdf_path)

                        base_output_row = {
                            "output_sha": output_sha,
                            "doc_doi": doc_doi,
                            "output_type": "raw file",
                            "pipeline_version": version_object.pipeline_version,
                            "software_version": version_object.software_version,
                            "creation_date": today_str,
                            "dependencies": None,
                        }

                        raw_file_row = {
                            "output_sha": output_sha,
                            "file_type": "pdf",
                            "file_path": repo_relative_path(pdf_path),
                            "file_size": os.path.getsize(pdf_path),
                        }

                        if output_sha not in existing_hashes:
                            df_base = pd.concat(
                                [df_base, pd.DataFrame([base_output_row])], ignore_index=True
                            )

                            df_base.to_csv(REGISTRIES_DIR / "base_output_registry.csv", index=False)

                            df_raw = pd.concat(
                                [df_raw, pd.DataFrame([raw_file_row])], ignore_index=True
                            )

                            df_raw.to_csv(SUBREGISTRIES_DIR / "raw_file_output_registry.csv", index=False)

                            existing_hashes.add(output_sha)

                xml_file = next((f for f in files if f.get("filetype") == "xml"), None)

                if xml_file:
                    xml_url = xml_file["file"]

                    xml_filename = xml_file["key"]

                    r2 = httpx.get(xml_url, timeout=httpx.Timeout(5.0, read=30.0))

                    if r2.status_code == 200:
                        xml_path = f"{record_dir}/{xml_filename}.xml"

                        with open(xml_path, "wb") as f:
                            f.write(r2.content)

                            xml_download_pbar.update(1)

                        output_sha = compute_hashes(xml_path)

                        base_output_row = {
                            "output_sha": output_sha,
                            "doc_doi": doc_doi,
                            "output_type": "raw file",
                            "pipeline_version": version_object.pipeline_version,
                            "software_version": version_object.software_version,
                            "creation_date": today_str,
                            "dependencies": None,
                        }

                        raw_file_row = {
                            "output_sha": output_sha,
                            "file_type": "xml",
                            "file_path": repo_relative_path(xml_path),
                            "file_size": os.path.getsize(xml_path),
                        }

                        if output_sha not in existing_hashes:
                            df_base = pd.concat(
                                [df_base, pd.DataFrame([base_output_row])], ignore_index=True
                            )

                            df_base.to_csv(REGISTRIES_DIR / "base_output_registry.csv", index=False)

                            df_raw = pd.concat(
                                [df_raw, pd.DataFrame([raw_file_row])], ignore_index=True
                            )

                            df_raw.to_csv(SUBREGISTRIES_DIR / "raw_file_output_registry.csv", index=False)

                            existing_hashes.add(output_sha)

                author_data = record_metadata.get("authors", [])

                author = author_data[0] if author_data else {}

                author_affiliations = author.get("affiliations")

                country = author_affiliations[0].get("country") if author_affiliations else None

                publication_list = record_metadata.get("publication_info", [])

                publication_data = publication_list[0] if publication_list else {}

                publication_year = publication_data.get("year")

                journal_title = publication_data.get("journal_title")

                arxiv_data = record.get("arxiv_eprints", [])

                arxiv_eprints_category = arxiv_data[0].get("categories") if arxiv_data else None

                document_row = {
                    "doc_doi": doc_doi,
                    "pdf_filename": pdf_filename if pdf_file else None,
                    "pdf_url": pdf_url if pdf_file else None,
                    "doc_type": None,
                    "has_DAS": None,
                    "has_CAS": None,
                    "doc_title": title,
                    "publication_year": publication_year,
                    "journal": journal_title,
                    "arxiv_eprints_category": arxiv_eprints_category,
                    "country": country,
                    "has_XML": xml_file is not None,
                    "xml_filename": xml_filename if xml_file else None,
                    "xml_url": xml_url if xml_file else None,
                    "creation_date": today_str,
                }

                df_document = pd.concat(
                    [df_document, pd.DataFrame([document_row])], ignore_index=True
                )

                df_document.to_csv(REGISTRIES_DIR / "document_registry.csv", index=False)

                metadata_pbar.update(1)

        if json_data["next"] is None:

            break

        r = httpx.get(json_data["next"])

        json_data = r.json()

        time.sleep(0.1)

    return df_document, df_base, df_raw


extract_text_logger = logging.getLogger(name="registry_testing.extract_txt_single_pdf")

signal.signal(signal.SIGALRM, timeout_handler)

def extract_text_single_pdf(
    pdf_path: Path,
    doc_doi: str,
    folder_doi: str,
    version_object: VersionObject,
    dependency_set: set,
    force_processing: bool = False,
):

    # function to extract txt single pdf_file
    # parse pdf by page, write the file
    # save to base_output_registry and raw_file_registry

    pdf_path = Path(pdf_path)

    # t0 = time.perf_counter()

    dependency_sha = compute_hashes(pdf_path)

    # print(f"hash input:{time.perf_counter ()-t0:.3f}s")

    if force_processing or dependency_sha not in dependency_set:
        signal.alarm(30)

        try:
            # t1 = time.perf_counter ()

            doc = fitz.open(pdf_path)

            txt_path = Path(
                RECORDS_DIR
                / version_object.software_version
                / "pipeline_version"
                / version_object.pipeline_version
                / folder_doi
                / f"{pdf_path.stem}.txt"
            )
            
            txt_path.parent.mkdir(parents=True, exist_ok=True)

            with open(txt_path, "w", encoding="utf-8", errors="ignore") as f:
                text = "\n".join(page.get_text() for page in doc)

                if text:
                    f.write(text)

            signal.alarm(0)

            # print(f"pdf parsing:{time.perf_counter ()-t1:.3f}s")

        except TimeoutError:
            extract_text_logger.warning("Timed out on %s", pdf_path)

            return None, None

        except (PermissionError, FileNotFoundError, OSError):
            extract_text_logger.exception("An error occured while processing %s", pdf_path)

            return None, None

        # t2 = time.perf_counter ()

        output_sha = compute_hashes(txt_path)

        # print(f"hash output:{time.perf_counter ()-t2:.3f}s")

        base_row = {
            "output_sha": output_sha,
            "doc_doi": doc_doi,
            "output_type": "raw file",
            "pipeline_version": version_object.pipeline_version,
            "software_version": version_object.software_version,
            "creation_date": datetime.now(),
            "dependencies": dependency_sha,
        }

        raw_row = {
            "output_sha": output_sha,
            "file_type": "txt",
            "file_path": repo_relative_path(txt_path),
            "file_size": os.path.getsize(txt_path),
        }

        # concatenate outside the function for performance enhancement
        # df_base_registry = pd.concat([df_base_registry, pd.DataFrame([base_row])], ignore_index=True)
        # df_raw_registry = pd.concat([df_raw_registry, pd.DataFrame([raw_row])], ignore_index=True)

        # save registries outside the function

        # registry_pbar.update(1)
        # save registries outside the function

        extract_text_logger.info("Text extracted and output metadata saved for %s ", pdf_path)

        return base_row, raw_row

    return "skipped", None


extract_section_logger = logging.getLogger()

"""FUNCTIONS FOR CLASSIFICATION OF DAS SECTIONS"""

def extract_DAS_section_single_pdf(
    txt_path: Path,
    doc_doi: str,
    folder_doi: str,
    version_object: VersionObject,
    df_document_registry: pd.DataFrame,
    df_base_registry: pd.DataFrame,
    df_extraction_registry: pd.DataFrame,
    force_processing: bool = False,
    apply_section_reparation: bool = False,
):

    # the following function extract data availability sections (DASs)
    # use config file for regex patterns
    # use regex patterns to find where DASs start and end
    # use positions to slice original text

    config = load_config()

    start_pattern = config["start_headers_DAS"]

    end_patterns = config["end_headers_DAS"]

    author_comment_re = regex.compile(r"\[[Aa]uthor['’]?s['’]?\s*[Cc]omment:.*?\]", flags=regex.DOTALL)

    if df_document_registry is None:
        df_document_registry = load_document_registry()

    if df_base_registry is None:
        df_base_registry = load_base_registry()

    if df_extraction_registry is None:
        df_extraction_registry = load_extraction_registry()

    df_document_registry["doc_type"] = df_document_registry["doc_type"].astype(object)  # strings

    df_document_registry["has_DAS"] = df_document_registry["has_DAS"].astype("boolean")  # nullable bool

    df_document_registry["has_CAS"] = df_document_registry["has_CAS"].astype("boolean")  # nullable bool

    dependency_list = set(
        df_base_registry.loc[
            df_base_registry["output_type"].isin(
                ["extracted section", "failed DAS extraction", "failed extraction (missing DAS)"]
            ),
            "dependencies",
        ].dropna()
    )

    dependency_sha = compute_hashes(txt_path)

    failure_signature = f"{dependency_sha}_section_extraction_failed_{version_object.software_version}"

    missing_signature = f"{dependency_sha}_missing_section_{version_object.software_version}"

    # define slicing positions to extract sections
    # re.search returns match object --> entire regex-matching string
    # use match object to define slicing positions
    # for section start, we first check for a specific pattern i.e. Data Availability Statment
    # if not found, we search for alternatives

    if force_processing or dependency_sha not in dependency_list:
        
        merge_base_extraction = df_base_registry.merge(df_extraction_registry[["output_sha", "section_type"]], on="output_sha", how="inner")

        old_sha_extraction = merge_base_extraction.loc[
        (merge_base_extraction["doc_doi"]==doc_doi)
        & (merge_base_extraction["output_type"]=="extracted section")
        & (merge_base_extraction["pipeline_version"]==version_object.pipeline_version)
        & (merge_base_extraction["software_version"]==version_object.software_version)
        & (merge_base_extraction["section_type"]=="DAS"),
        "output_sha"
    ]

        old_sha_base = df_base_registry.loc[
            (df_base_registry["doc_doi"]==doc_doi)
            & (df_base_registry["pipeline_version"]== version_object.pipeline_version)
            & (df_base_registry["software_version"]== version_object.software_version)
            & (
                df_base_registry["output_type"].isin(
                ["failed DAS extraction", "failed extraction (missing DAS)"]
            )),
            "output_sha"
        ]

        old_sha = pd.concat([old_sha_extraction, old_sha_base])

        df_extraction_registry = df_extraction_registry[~df_extraction_registry["output_sha"].isin(old_sha)]

        df_base_registry = df_base_registry[~df_base_registry["output_sha"].isin(old_sha)]


        try:
            text = txt_path.read_text()

            start_match = None

            for pattern in start_pattern:
                start_match = regex.search(pattern, text)

                if start_match:
                    start = start_match.end()

                    break

            if start_match is None:
                erratum_match = regex.search(r"(?i)\b(?:Publisher\s+)?Erratum\b:?", text[:1000])

                editorial_concern_match = regex.search(
                    r"(?i)\b(?:Editorial\s+Expression\s+of\s+Concern\b)", text[:1000]
                )

                retraction_note_match = regex.search(r"\bRetraction\s+Note\s+\b", text[:1000])

                if erratum_match:
                    extract_section_logger.info("Possible erratum found: %s", txt_path)

                    df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "doc_type"] = "Erratum"

                elif editorial_concern_match:
                    extract_section_logger.info("Possible editorial concern found: %s", txt_path)

                    df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "doc_type"] = (
                        "Editorial expression of concern"
                    )

                elif retraction_note_match:
                    extract_section_logger.info("Possible retraction note found: %s", txt_path)

                    df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "doc_type"] = "Retraction note"

                else:
                    extract_section_logger.warning("No DAS title found, cannot extract section for %s", txt_path)

                    df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "doc_type"] = (
                        "Scientific study"
                    )

                    df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "has_DAS"] = False

                    output_sha = sha1(missing_signature.encode()).hexdigest()

                    base_row_missing = {
                        "output_sha": output_sha,
                        "doc_doi": doc_doi,
                        "output_type": "failed extraction (missing DAS)",
                        "pipeline_version": version_object.pipeline_version,
                        "software_version": version_object.software_version,
                        "creation_date": datetime.now(),
                        "dependencies": dependency_sha,
                    }

                    df_base_registry = pd.concat(
                        [df_base_registry, pd.DataFrame([base_row_missing])], ignore_index=True
                    )

                return df_document_registry, df_base_registry, df_extraction_registry

            earliest_end = None

            # here we start searching for the position where the section ends
            # we first mask the text from the author's comment
            # this is essential to NOT match any possible ends of the section INSIDE the author's comment
            # the comment might include regex patterns that would normally indicate the end of the section
            # e.g. [Author's comment: [...] in FIG. 1] --> "FIG. 1" matches
            # we iterate over list of possible end headers
            # we save the position of the FIRST occurring pattern

            masked_author_comment_text = author_comment_re.sub(lambda m: " " * len(m.group()), text)

            for pattern in end_patterns:
                end_match = regex.search(pattern, masked_author_comment_text[start:])

                if end_match:
                    relative_end = end_match.start()

                    end = relative_end + start

                    if earliest_end is None or end < earliest_end:
                        earliest_end = end

                        continue

            if earliest_end is None:
                output_sha = sha1(failure_signature.encode()).hexdigest()

                base_row_incomplete = {
                    "output_sha": output_sha,
                    "doc_doi": doc_doi,
                    "output_type": "failed DAS extraction",
                    "pipeline_version": version_object.pipeline_version,
                    "software_version": version_object.software_version,
                    "creation_date": datetime.now(),
                    "dependencies": dependency_sha,
                }

                df_base_registry = pd.concat([df_base_registry, pd.DataFrame([base_row_incomplete])], ignore_index=True)

                return df_document_registry, df_base_registry, df_extraction_registry

            data_availability_section = text[start:earliest_end]

            df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "doc_type"] = "Scientific study"

            # here we check if there's any signs the sections was interrupted
            # particularly relevant for cases where section is split between two pages
            # some e.g. footers/headers might trigger the regexes searching for end positions
            # IF the section was interrupted, we call a number of utility functions to repair it
            # from the interruption point, define continuation candidates,
            # score continuation candidates, save best candidate and search TRUE end of the section
            # stitch back the two halves of the interrupted section

            if apply_section_reparation:
                data_availability_section = section_reparation_llm(
                    section=data_availability_section,
                    doc_doi=doc_doi
                )

            DAS_section_path = Path(
                RECORDS_DIR
                / version_object.software_version
                / "pipeline_version"
                / version_object.pipeline_version
                / folder_doi
                / "DAS_section.txt"
            )

            DAS_section_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(DAS_section_path, "w") as f:
                f.write(data_availability_section)

            output_sha = compute_hashes(DAS_section_path, salt=f"{doc_doi}_{version_object.software_version}")

            base_row = {
                "output_sha": output_sha,
                "doc_doi": doc_doi,
                "output_type": "extracted section",
                "pipeline_version": version_object.pipeline_version,
                "software_version": version_object.software_version,
                "creation_date": datetime.now(),
                "dependencies": dependency_sha,
            }

            extraction_row = {
                "output_sha": output_sha,
                "section_type": "DAS",
                "stage": "pre-cleaning",
                "text": data_availability_section,
                "file_path": repo_relative_path(DAS_section_path),
            }

            df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "has_DAS"] = True

            df_base_registry = pd.concat([df_base_registry, pd.DataFrame([base_row])], ignore_index=True)

            df_extraction_registry = pd.concat(
                [df_extraction_registry, pd.DataFrame([extraction_row])], ignore_index=True
            )

            # better to save outside the function

            return df_document_registry, df_base_registry, df_extraction_registry

        except (PermissionError, OSError, FileNotFoundError):
            extract_section_logger.exception("An error occured during section extraction.")

            return df_document_registry, df_base_registry, df_extraction_registry

    return df_document_registry, df_base_registry, df_extraction_registry


data_cleaning_logger = logging.getLogger()


def data_cleaner_single_file(
    section_file_path: Path,
    doc_doi: str,
    folder_doi: str,
    df_base_registry: pd.DataFrame,
    df_extraction_registry: pd.DataFrame,
    version_object: VersionObject,
    dependency_set: set, 
    force_processing: bool = False,
):

    section_prefix = "DAS" if "DAS" in str(section_file_path) else "CAS"

    cleaned_section_path = Path(
        RECORDS_DIR
        / version_object.software_version
        / "pipeline_version"
        / version_object.pipeline_version
        / folder_doi
        / f"{section_prefix}_section_cleaned.txt"
    )

    data_cleaning_logger.info("Starting to clean DA section")

    dependency_sha = compute_hashes(section_file_path, salt=f"{doc_doi}_{version_object.software_version}")

    if force_processing or dependency_sha not in dependency_set:
        # delete old rows if forced processing

        old_sha = df_extraction_registry.loc[
            (df_extraction_registry["stage"] == "cleaned")
            & (
                df_extraction_registry["output_sha"].isin(
                    df_base_registry.loc[
                        (df_base_registry["doc_doi"] == doc_doi)
                        & (df_base_registry["pipeline_version"] == version_object.pipeline_version)
                        & (df_base_registry["software_version"] == version_object.software_version),
                        "output_sha",
                    ]
                )
            ),
            "output_sha",
        ]

        df_extraction_registry = df_extraction_registry[~df_extraction_registry["output_sha"].isin(old_sha)]

        df_base_registry = df_base_registry[~df_base_registry["output_sha"].isin(old_sha)]

        try:
            text = section_file_path.read_text()

            t1_text = repair_hyperlink(text)

            t2_text, urls = mask_hyperlink(t1_text)

            t3_text = remove_hyphen(t2_text)

            t4_text = remove_ligatures(t3_text)

            t5_text = split_glued_words(t4_text)

            t6_text = unmask_hyperlink(t5_text, urls)

            t7_text = t6_text if t6_text.rstrip().endswith('.') else add_period(t6_text)

            t8_text = remove_leading_whitespace(t7_text)

            t9_text = remove_leading_period(t8_text)

            cleaned_section_path.parent.mkdir(parents=True, exist_ok=True)

            with open(cleaned_section_path, "w") as f:
                f.write(t9_text)

            output_sha = compute_hashes(cleaned_section_path, salt=f"cleaned:{doc_doi}_{version_object.software_version}")

            base_row = {
                "output_sha": output_sha,
                "doc_doi": doc_doi,
                "output_type": "extracted section",
                "pipeline_version": version_object.pipeline_version,
                "software_version": version_object.software_version,
                "creation_date": datetime.now(),
                "dependencies": dependency_sha,
            }

            extraction_row = {
                "output_sha": output_sha,
                "section_type": section_prefix,
                "stage": "cleaned",
                "text": t9_text,
                "file_path": repo_relative_path(cleaned_section_path),
            }

            df_base_registry = pd.concat([df_base_registry, pd.DataFrame([base_row])], ignore_index=True)

            df_extraction_registry = pd.concat(
                [df_extraction_registry, pd.DataFrame([extraction_row])], ignore_index=True
            )

            return df_base_registry, df_extraction_registry, dependency_sha

        except (OSError, PermissionError, FileNotFoundError) as e:
            data_cleaning_logger.warning(f"Failed to process {section_file_path}: {e}")

            return df_base_registry, df_extraction_registry, dependency_sha

    return df_base_registry, df_extraction_registry, dependency_sha



classify_section_logger = logging.getLogger()

def classify_DAS_claude_single_file(
    cleaned_DAS_path: Path,
    doc_doi: str, 
    df_base_registry: pd.DataFrame, 
    df_classification_registry: pd.DataFrame,
    version_object: VersionObject, 
    dependency_set: set,
    claude_model: str = "claude-haiku-4-5-20251001", 
    force_processing: bool = False, 
    api_key: str | None = None, 
    retries: int = 3
):

    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")

    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not found")

    label_list = ["no_data", "open_access", "partial_access", "restricted_access", "no_access", "incorrect", "unclear"]

    confidence_levels = {
        "not confident at all": "the section is highly ambiguous and could arguably belong to different categories",
        "somewhat not confident": "the section has ambiguous elements, but one category seems slightly more correct than others",
        "neither confident nor not confident": "the section has some ambiguous elements but it could reasonably be classified as belonging to a category",
        "somewhat confident": "the section reasonably fits one category, but has some minor ambiguities",
        "very confident": "the section clearly satisfies a strong logical condition for classifying it into one category",
    }

    json_pattern = regex.compile(r"\{.*?\}", regex.DOTALL)

    client = anthropic.Anthropic(api_key=api_key)

    dependency_sha = compute_hashes(cleaned_DAS_path, salt=f"cleaned:{doc_doi}_{version_object.software_version}")

    classification_schema = load_classification_schema_DAS()

    if force_processing or dependency_sha not in dependency_set:

        print(f"Starting to classify DAS for {doc_doi}")

        # compute sha of previous classification
        # need it to eliminate existing rows for this document if processing is forced

        old_sha = df_classification_registry.loc[(df_classification_registry["method"]=="LLM")
                                                 & (df_classification_registry["output_sha"].isin(
                                                     df_base_registry.loc[(df_base_registry["doc_doi"] == doc_doi)
                                                     & (df_base_registry["pipeline_version"] == version_object.pipeline_version)
                                                     & (df_base_registry["software_version"] == version_object.software_version), "output_sha"],
                                                     )),
                                                     "output_sha"
                                                 ]

        df_base_registry = df_base_registry[~df_base_registry["output_sha"].isin(old_sha)]

        df_classification_registry = df_classification_registry[~df_classification_registry["output_sha"].isin(old_sha)]

        DAS_text = cleaned_DAS_path.read_text()

        prompt = f"""Your task it to classify data availability statements in scientific papers according to the classification schema provided below. Some general information about the classification task:
                    The following are the existing labels for the task: {label_list}.
                    You must only classify sections using these labels. Please provide a level of confidence in the classification alongside the classification label. 
                    The following are the levels of confidence you should use to score confidence in the classification alongisde their definitions: {confidence_levels}.
                    Use ONLY the keys as labels for the final classification output. 
                    Remember that if a section mentions information about data availability, that takes precedence over information about code availability. 
                    Therefore, if a section mentions information about data, that information should guide classification whereas information about code should be ignored. 
                    The following are priority rules you should use EXCLUSIVELY when a section mentions information about availability of multiple elements of research data under DIFFERENT access conditions 
                    (e.g. a section mentioning that some elements are openly available, while some are available upon request):
                    - If at least one element of research data is mentioned as openly available, the section should be classified as "partial_access".
                    - If at least one element of research data is mentioned as being available upon request AND no other element is mentioned as being openly available, the section should be classified as "restricted_access".
                    - If at least one element of the research study materials mention information about data availability, that should take precedence over information about code availability, which should instead be ignored.
                    - Information about availability of code/software should only be considered when NOT provided alongside information about data availability. In that case, the section should always be classified as "incorrect".
                    Remember also that information about code/software used to generate data should be considered information about code, not information about data. 
                    Remembers that a large variety of research outputs can be considered as "elements of research data". That should include every research output that can be interpreted and understood by sufficiently knowledgeable humans.
                    Examples of elements of research data are: numerical values, measurements, graphs, tables, images, equations, etc. Code should never be considered as an element of research data for this task. 
                    Inside the classification schema, you will find examples of hard positives and hard negatives for each category. Hard positives are pair of statements belonging to the SAME category that are difficult
                    to classify as part of the same category. Hard negatives are pairs of statements belonging to DIFFERENT categories that are difficult to classify as being part of different categories. In the hard negative cases,
                    the FIRST statement is the one belonging to the relevant category, while the SECOND statement is the one belonging to a different category. 
                    The following is the classification schema you should use for classifying the section. Read the classification schema carefully. Reason about how you response follows the rules provided in the schema.
                    "{classification_schema}". 
                    Respond ONLY with a JSON object with two keys "label" and "confidence"."""

        for attempt in range(retries):
            try:
                response = client.messages.create(
                    model= claude_model,
                    max_tokens=500,
                    temperature=0,
                    system=[
                        {"type": "text",
                         "text": prompt,
                         "cache_control": {"type": "ephemeral"}
                         }
                    ],
                    messages=[{"role": "user", "content": f"classify this data availability statement:{DAS_text}"}],
                )
                time.sleep(0.5)

                response_text = response.content[0].text.strip()

                json_match = json_pattern.search(response_text)
                
                if json_match:

                    response_text = json_match.group()

                else:
                    
                    classify_section_logger.info(f"No JSON found in response: {response_text}")

                    return df_base_registry, df_classification_registry, dependency_sha

                classification_result = json.loads(response_text)

                category_label = classification_result["label"]

                confidence_level = classification_result["confidence"]

                if category_label not in label_list or confidence_level not in confidence_levels:

                    classify_section_logger.info(f"Invalid classification: label={category_label}, confidence={confidence_level}")

                    return df_base_registry, df_classification_registry, dependency_sha

                else:
                
                    output_sha = compute_hashes(cleaned_DAS_path, salt=f"classified:{doc_doi}_{claude_model}_{version_object.software_version}")

                    print(f"Writing row for {doc_doi}, label={category_label}") 
                    
                    base_row = {
                        "output_sha": output_sha,
                        "doc_doi": doc_doi,
                        "output_type": "classification",
                        "pipeline_version": version_object.pipeline_version,
                        "software_version": version_object.software_version,
                        "creation_date": datetime.now(),
                        "dependencies": dependency_sha
                    }

                    classification_row = {
                        "output_sha": output_sha,
                        "label": category_label,
                        "method": "LLM",
                        "model": claude_model,
                        "confidence": confidence_level,
                        "text": DAS_text
                    }

                    df_base_registry = pd.concat([df_base_registry, pd.DataFrame([base_row])], ignore_index=True)

                    df_classification_registry = pd.concat([df_classification_registry, pd.DataFrame([classification_row])], ignore_index=True)

                    return df_base_registry, df_classification_registry, dependency_sha

            except anthropic.RateLimitError:
                wait = 2**attempt
                classify_section_logger.info(f"Rate limit hit, retrying in {wait}s...")
                time.sleep(wait)

            except anthropic.APIError as e:
                classify_section_logger.info(f"API error: {e}")

                return df_base_registry, df_classification_registry, dependency_sha

            except Exception as e:
                classify_section_logger.info(f"Unexpected error: {e}")
                print(f"Unexpected error: {e}")

                return df_base_registry, df_classification_registry, dependency_sha

            
        classify_section_logger.info("Max retries exceeded")

        return df_base_registry, df_classification_registry, dependency_sha
    
    else:
        classify_section_logger.info(f"Skipping {doc_doi}, already processed")
        return df_base_registry, df_classification_registry, dependency_sha
    


"""FUNCTIONS FOR CAS CLASSIFICATION"""

def extract_CAS_section_single_pdf(
    txt_path: Path,
    doc_doi: str,
    folder_doi: str,
    version_object: VersionObject,
    df_document_registry: pd.DataFrame,
    df_base_registry: pd.DataFrame,
    df_extraction_registry: pd.DataFrame,
    force_processing: bool = False,
    apply_section_reparation: bool = False,
):

    # the following function extract code availability statements (CAS)
    # use config file for regex patterns
    # use regex patterns to find where CASs start and end
    # use positions to slice original text

    config = load_config()

    start_pattern = config["start_headers_CAS"]

    end_patterns = config["end_headers_CAS"]

    author_comment_re = regex.compile(r"\[[Aa]uthor['’]?s['’]?\s*[Cc]omment:.*?\]", flags=regex.DOTALL)

    if df_document_registry is None:
        df_document_registry = load_document_registry()

    if df_base_registry is None:
        df_base_registry = load_base_registry()

    if df_extraction_registry is None:
        df_extraction_registry = load_extraction_registry()

    df_document_registry["doc_type"] = df_document_registry["doc_type"].astype(object)  # strings

    df_document_registry["has_DAS"] = df_document_registry["has_DAS"].astype("boolean")  # nullable bool

    df_document_registry["has_CAS"] = df_document_registry["has_CAS"].astype("boolean")  # nullable bool

    dependency_list = set(
        df_base_registry.loc[
            df_base_registry["output_type"].isin(
                ["extracted section", "failed CAS extraction", "failed extraction (missing CAS)"]
            ),
            "dependencies",
        ].dropna()
    )


    dependency_sha = compute_hashes(txt_path)

    failure_signature = f"{dependency_sha}_CAS_section_extraction_failed_{version_object.software_version}"

    missing_signature = f"{dependency_sha}_missing_CAS_section_{version_object.software_version}"

    # define slicing positions to extract sections
    # re.search returns match object --> entire regex-matching string
    # use match object to define slicing positions
    # for section start, we first check for a specific pattern i.e. Data Availability Statment
    # if not found, we search for alternatives


    if force_processing or dependency_sha not in dependency_list:

        merge_base_extraction = df_base_registry.merge(df_extraction_registry[["output_sha", "section_type"]], on="output_sha", how="inner")

        old_sha_extraction = merge_base_extraction.loc[
        (merge_base_extraction["doc_doi"]==doc_doi)
        & (merge_base_extraction["output_type"]=="extracted section")
        & (merge_base_extraction["pipeline_version"]==version_object.pipeline_version)
        & (merge_base_extraction["software_version"]==version_object.software_version)
        & (merge_base_extraction["section_type"]=="CAS"),
        "output_sha"
    ]

        old_sha_base = df_base_registry.loc[
            (df_base_registry["doc_doi"]==doc_doi)
            & (df_base_registry["pipeline_version"]== version_object.pipeline_version)
            & (df_base_registry["software_version"]== version_object.software_version)
            & (
                df_base_registry["output_type"].isin(
                ["failed CAS extraction", "failed extraction (missing CAS)"]
            )),
            "output_sha"
        ]

        old_sha = pd.concat([old_sha_extraction, old_sha_base])

        df_extraction_registry = df_extraction_registry[~df_extraction_registry["output_sha"].isin(old_sha)]

        df_base_registry = df_base_registry[~df_base_registry["output_sha"].isin(old_sha)]


        try:
            text = txt_path.read_text(encoding="utf-8", errors="replace")

            start_match = None

            for pattern in start_pattern:
                start_match = regex.search(pattern, text)

                if start_match:
                    start = start_match.end()

                    break

            if start_match is None:
                erratum_match = regex.search(r"(?i)\b(?:Publisher\s+)?Erratum\b:?", text[:1000])

                editorial_concern_match = regex.search(
                    r"(?i)\b(?:Editorial\s+Expression\s+of\s+Concern\b)", text[:1000]
                )

                retraction_note_match = regex.search(r"\bRetraction\s+Note\s+\b", text[:1000])

                if erratum_match:
                    extract_section_logger.info("Possible erratum found: %s", txt_path)

                    df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "doc_type"] = "Erratum"

                elif editorial_concern_match:
                    extract_section_logger.info("Possible editorial concern found: %s", txt_path)

                    df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "doc_type"] = (
                        "Editorial expression of concern"
                    )

                elif retraction_note_match:
                    extract_section_logger.info("Possible retraction note found: %s", txt_path)

                    df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "doc_type"] = "Retraction note"

                else:
                    extract_section_logger.warning("No CAS title found, cannot extract section for %s", txt_path)

                    df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "doc_type"] = (
                        "Scientific study"
                    )

                    df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "has_CAS"] = False

                    output_sha = sha1(missing_signature.encode()).hexdigest()

                    base_row_missing = {
                        "output_sha": output_sha,
                        "doc_doi": doc_doi,
                        "output_type": "failed extraction (missing CAS)",
                        "pipeline_version": version_object.pipeline_version,
                        "software_version": version_object.software_version,
                        "creation_date": datetime.now(),
                        "dependencies": dependency_sha,
                    }

                    df_base_registry = pd.concat(
                        [df_base_registry, pd.DataFrame([base_row_missing])], ignore_index=True
                    )

                return df_document_registry, df_base_registry, df_extraction_registry

            earliest_end = None

            # here we start searching for the position where the section ends
            # we first mask the text from the author's comment
            # this is essential to NOT match any possible ends of the section INSIDE the author's comment
            # the comment might include regex patterns that would normally indicate the end of the section
            # e.g. [Author's comment: [...] in FIG. 1] --> "FIG. 1" matches
            # we iterate over list of possible end headers
            # we save the position of the FIRST occurring pattern

            masked_author_comment_text = author_comment_re.sub(lambda m: " " * len(m.group()), text)

            for pattern in end_patterns:
                end_match = regex.search(pattern, masked_author_comment_text[start:])

                if end_match:
                    relative_end = end_match.start()

                    end = relative_end + start

                    if earliest_end is None or end < earliest_end:
                        earliest_end = end

                        continue

            if earliest_end is None:
                output_sha = sha1(failure_signature.encode()).hexdigest()

                base_row_incomplete = {
                    "output_sha": output_sha,
                    "doc_doi": doc_doi,
                    "output_type": "failed CAS extraction",
                    "pipeline_version": version_object.pipeline_version,
                    "software_version": version_object.software_version,
                    "creation_date": datetime.now(),
                    "dependencies": dependency_sha,
                }

                df_base_registry = pd.concat([df_base_registry, pd.DataFrame([base_row_incomplete])], ignore_index=True)

                return df_document_registry, df_base_registry, df_extraction_registry

            code_availability_section = text[start:earliest_end]

            df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "doc_type"] = "Scientific study"

            # here we check if there's any signs the sections was interrupted
            # particularly relevant for cases where section is split between two pages
            # some e.g. footers/headers might trigger the regexes searching for end positions
            # IF the section was interrupted, we call a number of utility functions to repair it
            # from the interruption point, define continuation candidates,
            # score continuation candidates, save best candidate and search TRUE end of the section
            # stitch back the two halves of the interrupted section

            if apply_section_reparation:
                code_availability_section = section_reparation_llm(
                    section=code_availability_section,
                    doc_doi=doc_doi
                    )

            CAS_section_path = Path(
                RECORDS_DIR
                / version_object.software_version
                / "pipeline_version"
                / version_object.pipeline_version
                / folder_doi
                / "CAS_section.txt"
            )

            CAS_section_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(CAS_section_path, "w") as f:
                f.write(code_availability_section)

            output_sha = compute_hashes(CAS_section_path, salt=f"{doc_doi}_{version_object.software_version}")

            base_row = {
                "output_sha": output_sha,
                "doc_doi": doc_doi,
                "output_type": "extracted section",
                "pipeline_version": version_object.pipeline_version,
                "software_version": version_object.software_version,
                "creation_date": datetime.now(),
                "dependencies": dependency_sha,
            }

            extraction_row = {
                "output_sha": output_sha,
                "section_type": "CAS",
                "stage": "pre-cleaning",
                "text": code_availability_section,
                "file_path": repo_relative_path(CAS_section_path),
            }

            df_document_registry.loc[df_document_registry["doc_doi"] == doc_doi, "has_CAS"] = True

            df_base_registry = pd.concat([df_base_registry, pd.DataFrame([base_row])], ignore_index=True)

            df_extraction_registry = pd.concat(
                [df_extraction_registry, pd.DataFrame([extraction_row])], ignore_index=True
            )

            # better to save outside the function

            return df_document_registry, df_base_registry, df_extraction_registry

        except (PermissionError, OSError, FileNotFoundError):
            extract_section_logger.exception("An error occured during CAS section extraction.")

            return df_document_registry, df_base_registry, df_extraction_registry

    return df_document_registry, df_base_registry, df_extraction_registry


classify_section_logger = logging.getLogger()

def classify_CAS_claude_single_file(
    cleaned_CAS_path: Path,
    doc_doi: str, 
    df_base_registry: pd.DataFrame, 
    df_CAS_classification_registry: pd.DataFrame,
    version_object: VersionObject, 
    dependency_set: set,
    claude_model: str = "claude-haiku-4-5-20251001", 
    force_processing: bool = False, 
    api_key: str | None = None, 
    retries: int = 3
):

    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")

    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not found")

    manuscript_code_labels = ["no_code", "open_access", "partial_access", "restricted_access", "no_access", "incorrect", "unclear"]

    external_tool_labels = ["no_tool", "open_tool", "mixed_tool", "proprietary_tool", "unknown_status_tool"]

    confidence_levels = {
        "not confident at all": "the section is highly ambiguous, could arguably belong to different categories, and it's very difficult to distinguish APSAs from ETSs",
        "somewhat not confident": "the section has ambiguous elements, one category seems slightly more correct than others, and it's somewhat difficult to distinguis APSAs from ETSs",
        "neither confident nor not confident": "the section has some ambiguous elements but it could reasonably be classified as belonging to a category, and it's neither diffificult nor easy to distinguish APSAs from ESTs",
        "somewhat confident": "the section reasonably fits one category but has some minor ambiguities, and it's somwewhat easy to distinguish APSAs from ETSs",
        "very confident": "the section clearly satisfies a strong logical condition for classifying it into one category and it's very easy to distinguis APSAs from ETSs",
    }

    json_pattern = regex.compile(r"\{.*?\}", regex.DOTALL)

    client = anthropic.Anthropic(api_key=api_key)

    dependency_sha = compute_hashes(cleaned_CAS_path, salt=f"cleaned:{doc_doi}_{version_object.software_version}")

    classification_schema_MCA = load_classification_schema_CAS_MCA()

    classification_schema_ETA = load_classification_schema_CAS_ETA()

    if force_processing or dependency_sha not in dependency_set:

        print(f"Starting to classify CAS for {doc_doi}")

        # compute sha of previous classification
        # need it to eliminate existing rows for this document if processing is forced

        old_sha = df_CAS_classification_registry.loc[(df_CAS_classification_registry["method"]=="LLM")
                                                 & (df_CAS_classification_registry["output_sha"].isin(
                                                     df_base_registry.loc[(df_base_registry["doc_doi"] == doc_doi)
                                                     & (df_base_registry["pipeline_version"] == version_object.pipeline_version)
                                                     & (df_base_registry["software_version"] == version_object.software_version), "output_sha"],
                                                     )),
                                                     "output_sha"
                                                 ]

        df_base_registry = df_base_registry[~df_base_registry["output_sha"].isin(old_sha)]

        df_CAS_classification_registry = df_CAS_classification_registry[~df_CAS_classification_registry["output_sha"].isin(old_sha)]

        CAS_text = cleaned_CAS_path.read_text()

        prompt_template = Template(load_CAS_prompt_template())

        prompt = prompt_template.safe_substitute(
            manuscript_code_labels = manuscript_code_labels,
            external_tool_labels = external_tool_labels,
            classification_schema_MCA = yaml.safe_dump(
                classification_schema_MCA, sort_keys=False, allow_unicode=True
            ),
            classification_schema_ETA = yaml.safe_dump(
                classification_schema_ETA, sort_keys=False, allow_unicode=True
            ),
            confidence_levels = yaml.safe_dump(
                confidence_levels, sort_keys=False, allow_unicode=True
            )
            )

        for attempt in range(retries):
            try:
                response = client.messages.create(
                    model= claude_model,
                    max_tokens=500,
                    temperature=0,
                    system=[
                        {"type": "text",
                         "text": prompt,
                         "cache_control": {"type": "ephemeral"}
                         }
                    ],
                    messages=[{"role": "user", "content": f"classify this code availability statement according to the instructions:{CAS_text}"}],
                )
                time.sleep(0.5)

                response_text = response.content[0].text.strip()

                json_match = json_pattern.search(response_text)
                
                if json_match:

                    response_text = json_match.group()

                else:
                    
                    classify_section_logger.info(f"No JSON found in response: {response_text}")

                    return df_base_registry, df_CAS_classification_registry, dependency_sha

                classification_result = json.loads(response_text)

                manuscript_code_availability_label = classification_result["manuscript_code_availability"]

                manuscript_code_availability_confidence_level = classification_result["manuscript_code_availability_confidence"]

                external_tool_availability_label = classification_result["external_tool_availability"]

                external_tool_availability_confidence_level = classification_result["external_tool_availability_confidence"]

                if manuscript_code_availability_label not in manuscript_code_labels or manuscript_code_availability_confidence_level not in confidence_levels:

                    classify_section_logger.info(f"Invalid MCA classification: label={manuscript_code_availability_label}, confidence={manuscript_code_availability_confidence_level}")

                    return df_base_registry, df_CAS_classification_registry, dependency_sha
               
                elif external_tool_availability_label not in external_tool_labels or external_tool_availability_confidence_level not in confidence_levels:

                    classify_section_logger.info(f"Invalid ETA classification: label={external_tool_availability_label}, confidence={external_tool_availability_confidence_level}")
                else:
                
                    output_sha = compute_hashes(cleaned_CAS_path, salt=f"classified:{doc_doi}_{claude_model}_{version_object.software_version}")

                    print(f"Writing row for {doc_doi}\nMCA_label={manuscript_code_availability_label}\nETA_label={external_tool_availability_label}") 
                    
                    base_row = {
                        "output_sha": output_sha,
                        "doc_doi": doc_doi,
                        "output_type": "classification",
                        "pipeline_version": version_object.pipeline_version,
                        "software_version": version_object.software_version,
                        "creation_date": datetime.now(),
                        "dependencies": dependency_sha
                    }

                    classification_row = {
                        "output_sha": output_sha,
                        "MCA_label": manuscript_code_availability_label,
                        "MCA_confidence": manuscript_code_availability_confidence_level,
                        "ETA_label": external_tool_availability_label,
                        "ETA_confidence": external_tool_availability_confidence_level,
                        "method": "LLM",
                        "model": claude_model,
                        "text": CAS_text
                    }

                    df_base_registry = pd.concat([df_base_registry, pd.DataFrame([base_row])], ignore_index=True)

                    df_CAS_classification_registry = pd.concat([df_CAS_classification_registry, pd.DataFrame([classification_row])], ignore_index=True)

                    return df_base_registry, df_CAS_classification_registry, dependency_sha

            except anthropic.RateLimitError:
                wait = 2**attempt
                classify_section_logger.info(f"Rate limit hit, retrying in {wait}s...")
                time.sleep(wait)

            except anthropic.APIError as e:
                classify_section_logger.info(f"API error: {e}")

                return df_base_registry, df_CAS_classification_registry, dependency_sha

            except Exception as e:
                classify_section_logger.info(f"Unexpected error: {e}")
                print(f"Unexpected error: {e}")

                return df_base_registry, df_CAS_classification_registry, dependency_sha

            
        classify_section_logger.info("Max retries exceeded")

        return df_base_registry, df_CAS_classification_registry, dependency_sha
    
    else:
        classify_section_logger.info(f"Skipping {doc_doi}, already processed")
        return df_base_registry, df_CAS_classification_registry, dependency_sha
