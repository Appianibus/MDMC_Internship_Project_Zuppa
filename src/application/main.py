from pathlib import Path

import pandas as pd
from tqdm import tqdm

from application.pipeline import (
    classify_CAS_claude_single_file,
    classify_DAS_claude_single_file,
    data_cleaner_single_file,
    data_ingestion,
    extract_CAS_section_single_pdf,
    extract_DAS_section_single_pdf,
    extract_text_single_pdf,
)
from application.utils import save_registry
from config.config import REGISTRIES_DIR, SUBREGISTRIES_DIR, VersionObject, resolve_registry_path


def classify_files(df_document: pd.DataFrame, 
                    df_base: pd.DataFrame, 
                    df_raw: pd.DataFrame, 
                    df_extraction: pd.DataFrame, 
                    df_DAS_classification: pd.DataFrame,
                    df_CAS_classification: pd.DataFrame, 
                    version_object: VersionObject,
                    DAS_force_processing: bool = False,
                    CAS_force_processing: bool = False)  -> None:

    """classify files on path"""

    df_base, df_raw = extract_texts(df_base, df_raw, version_object)

    df_document, df_base, df_extraction =  extract_sections(df_document, df_base, df_raw, df_extraction, version_object)

    df_base, df_extraction = normalize_sections(df_base, df_extraction, version_object)

    classify_DAS_sections(df_base, df_extraction, df_DAS_classification, version_object, force_processing=DAS_force_processing)

    classify_CAS_sections(df_base, df_extraction, df_CAS_classification, version_object, force_processing=CAS_force_processing)


# the function belows parses pdf files and transforms them in txt
# it first merges the raw files registry with the base registry to have each files' doc_doi in the merged registry
# we build a dependency set to pass to the lower-level extract_text_single_pdf function
# this is important to check which files have already been processed and skip them unless the force_processing flag is True
# dependency_set is built through the "dependencies" column for files in the raw files registry whose type is "txt"
# we extract rows in the merged df whose file type is "pdf"
# we loop through the rows and call the extract_text_single_pdf function on each
# as text is extracted, new metadata is produced and saved in base_row and raw_row for each file
# these rows are saved in a list which is then concatenated to an exisiting registry
# this way registries are updated every time new files are processed 

def extract_texts(df_base: pd.DataFrame, df_raw: pd.DataFrame, version_object: VersionObject) -> tuple[pd.DataFrame, pd.DataFrame]:
    merged_df = df_raw.merge(
    df_base[["output_sha", "doc_doi"]],
    on="output_sha",
    how="left",
)

    base_row_list = []

    raw_row_list = []

    timed_out_files = []

    dependency_set = set(
    df_base.loc[
        df_base["output_sha"].isin(df_raw.loc[df_raw["file_type"] == "txt", "output_sha"]),
        "dependencies",
    ].dropna()
)

    pdf_rows = merged_df.loc[merged_df["file_type"] == "pdf"]

    for _, row in tqdm(pdf_rows.iterrows(), total=len(pdf_rows), desc="Parsing pdf file", unit="files"):
        pdf_path = resolve_registry_path(row["file_path"])

        doc_doi = row["doc_doi"]

        folder_doi = doc_doi.replace("/", "_")

        base_row, raw_row = extract_text_single_pdf(pdf_path, doc_doi, folder_doi, version_object, dependency_set)

        if base_row is None and raw_row is None:
            timed_out_files.append(pdf_path)

        elif base_row == "skipped":
            pass

        else:
            base_row_list.append(base_row)

            raw_row_list.append(raw_row)

            dependency_set.add(base_row["dependencies"])

    if timed_out_files:
        print(f"\n{len(timed_out_files)} files timed out and were skipped:")

        for f in timed_out_files:
            print(f"  {f}")

    df_base = pd.concat([df_base, pd.DataFrame(base_row_list)], ignore_index=True)

    df_raw = pd.concat([df_raw, pd.DataFrame(raw_row_list)], ignore_index=True)

    save_registry(df_base, REGISTRIES_DIR / "base_output_registry.csv")

    save_registry(df_raw, SUBREGISTRIES_DIR / "raw_file_output_registry.csv")

    return df_base, df_raw



# perform DAS section extraction

def extract_sections(df_document: pd.DataFrame, df_base: pd.DataFrame, df_raw: pd.DataFrame, df_extraction: pd.DataFrame, version_object: VersionObject, force_processing: bool = False, section_type: list = ["CAS", "DAS"], apply_section_reparation: bool=False) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    merged_df_2 = df_raw.merge(df_base[["output_sha", "doc_doi"]], on="output_sha", how="left")

    merged_df_2.drop_duplicates()

    txt_rows = merged_df_2.loc[merged_df_2["file_type"] == "txt"]

    for _, row in tqdm(txt_rows.iterrows(), total=len(txt_rows), desc=f"Extracting {section_type} sections", unit="section(s)"):
        txt_path = resolve_registry_path(row["file_path"])

        doc_doi = row["doc_doi"]

        folder_doi = doc_doi.replace("/", "_")

        if "DAS" in section_type:

            df_document, df_base, df_extraction = extract_DAS_section_single_pdf(
            txt_path,
            doc_doi,
            folder_doi,
            version_object,
            df_document_registry=df_document,
            df_base_registry=df_base,
            df_extraction_registry=df_extraction,
            force_processing=force_processing,
            apply_section_reparation=apply_section_reparation
        )
            
        if "CAS" in section_type:

            df_document, df_base, df_extraction = extract_CAS_section_single_pdf(
                txt_path,
                doc_doi,
                folder_doi,
                version_object,
                df_document_registry=df_document,
                df_base_registry=df_base,
                df_extraction_registry=df_extraction,
                force_processing=force_processing
            )

    save_registry(df_base, REGISTRIES_DIR / "base_output_registry.csv")

    save_registry(df_extraction, SUBREGISTRIES_DIR / "extraction_output_registry.csv")

    save_registry(df_document, REGISTRIES_DIR / "document_registry.csv")

    return df_document, df_base, df_extraction


# perform data cleaning

def normalize_sections(df_base: pd.DataFrame, df_extraction: pd.DataFrame, version_object: VersionObject, section_type: list | None = None, force_processing: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    
    if section_type is None:

        section_type = ["DAS", "CAS"]
    
    merged_df_3 = df_extraction.merge(df_base[["output_sha", "doc_doi", "software_version"]], on="output_sha", how="left")

    dependency_set = set(
    df_base.loc[
        df_base["output_sha"].isin(
            df_extraction.loc[(df_extraction["stage"] == "cleaned")
                              & (df_extraction["section_type"].isin(section_type)), 
                              "output_sha"]
        ),
        "dependencies",
    ].dropna()
)

    rows = merged_df_3.loc[(merged_df_3["section_type"].isin(section_type)) & (merged_df_3["stage"] == "pre-cleaning") & (merged_df_3["software_version"] == version_object.software_version)]

    for _, row in tqdm(rows.iterrows(), total=len(rows), desc=f"Cleaning {section_type}", unit="section(s)"):
        path = resolve_registry_path(row["file_path"])

        doc_doi = row["doc_doi"]

        folder_doi = doc_doi.replace("/", "_")

        df_base, df_extraction, dependency_sha = data_cleaner_single_file(
        path,
        doc_doi,
        folder_doi,
        df_base,
        df_extraction,
        version_object,
        dependency_set,
        force_processing=force_processing,
    )

        dependency_set.add(dependency_sha)

    save_registry(df_base, REGISTRIES_DIR / "base_output_registry.csv")

    save_registry(df_extraction, SUBREGISTRIES_DIR / "extraction_output_registry.csv")
    
    return df_base, df_extraction




def classify_DAS_sections(
    df_base: pd.DataFrame,
    df_extraction: pd.DataFrame,
    df_classification: pd.DataFrame,
    version_object: VersionObject,
    force_processing: bool = False,
    checkpoint_interval: int = 50,
) -> None:
    
    merged_df_classification = df_extraction.merge(df_base[["output_sha", "doc_doi", "software_version"]], on="output_sha", how="left")

    merged_df_classification = merged_df_classification.drop_duplicates()

    dependency_set = set(df_base.loc[df_base["output_sha"].isin(
    df_classification.loc[df_classification["method"]=="LLM", "output_sha"]),
    "dependencies"
])

    cleaned_DAS_rows = merged_df_classification.loc[(merged_df_classification["section_type"]=="DAS") 
    & (merged_df_classification["stage"]=="cleaned") & 
    (merged_df_classification["software_version"]==version_object.software_version)]

    rows_since_checkpoint = 0

    for _, row in tqdm(cleaned_DAS_rows.iterrows(), total=len(cleaned_DAS_rows), desc="Classifying DAS", unit="section(s)"):
        doc_doi = row["doc_doi"]

        cleaned_DAS_path = resolve_registry_path(row["file_path"])

        df_base, df_classification, dependency_sha = classify_DAS_claude_single_file(cleaned_DAS_path,
                                                                                       doc_doi,
                                                                                       df_base,
                                                                                       df_classification,
                                                                                       version_object,
                                                                                       dependency_set,
                                                                                       force_processing=force_processing                                                                                
                                                                                       )
    
        dependency_set.add(dependency_sha)

        rows_since_checkpoint += 1

        if checkpoint_interval > 0 and rows_since_checkpoint >= checkpoint_interval:
            save_registry(df_base, REGISTRIES_DIR / "base_output_registry.csv")
            save_registry(df_classification, SUBREGISTRIES_DIR / "DAS_classification_output_registry.csv")
            rows_since_checkpoint = 0

    save_registry(df_base, REGISTRIES_DIR / "base_output_registry.csv")

    save_registry(df_classification,  SUBREGISTRIES_DIR / "DAS_classification_output_registry.csv")

    return merged_df_classification

def classify_CAS_sections(
    df_base: pd.DataFrame,
    df_extraction: pd.DataFrame,
    df_CAS_classification: pd.DataFrame,
    version_object: VersionObject,
    force_processing: bool = False,
    checkpoint_interval: int = 50,
) -> None:
    
    merged_df_classification = df_extraction.merge(df_base[["output_sha", "doc_doi", "software_version"]], on="output_sha", how="left")

    merged_df_classification = merged_df_classification.drop_duplicates()

    dependency_set = set(df_base.loc[df_base["output_sha"].isin(
    df_CAS_classification.loc[df_CAS_classification["method"]=="LLM", "output_sha"]),
    "dependencies"
])

    cleaned_CAS_rows = merged_df_classification.loc[(merged_df_classification["section_type"]=="CAS") 
    & (merged_df_classification["stage"]=="cleaned") & 
    (merged_df_classification["software_version"]==version_object.software_version)]

    rows_since_checkpoint = 0

    for _, row in tqdm(cleaned_CAS_rows.iterrows(), total=len(cleaned_CAS_rows), desc="Classifying CAS", unit="section(s)"):
        doc_doi = row["doc_doi"]

        cleaned_CAS_path = resolve_registry_path(row["file_path"])

        df_base, df_CAS_classification, dependency_sha = classify_CAS_claude_single_file(cleaned_CAS_path,
                                                                                       doc_doi,
                                                                                       df_base,
                                                                                       df_CAS_classification,
                                                                                       version_object,
                                                                                       dependency_set,
                                                                                       force_processing=force_processing                                                                                
                                                                                       )
    
        dependency_set.add(dependency_sha)

        rows_since_checkpoint += 1

        if checkpoint_interval > 0 and rows_since_checkpoint >= checkpoint_interval:
            save_registry(df_base, REGISTRIES_DIR / "base_output_registry.csv")
            save_registry(df_CAS_classification, SUBREGISTRIES_DIR / "CAS_classification_output_registry.csv")
            rows_since_checkpoint = 0

    save_registry(df_base, REGISTRIES_DIR / "base_output_registry.csv")

    save_registry(df_CAS_classification,  SUBREGISTRIES_DIR / "CAS_classification_output_registry.csv")

    return merged_df_classification


if __name__ == "__main__":
    
    # load version object and metadata registries
    # each registry saves metadata for different pipeline outputs
    # document --> metadata on each single document (SCOAP publication) ingested
    # base --> general registry for any output with generic features (e.g. sha, associated doi, software version, etc.)
    # raw --> raw files (i.e. pdf, xml, txt)
    # extraction --> extraction outputs (i.e. extracted DAS/CAS, cleaned DAS/CAS)
    # classification --> classification outputs (label, model, confidence)

    DOCUMENT_REGISTRY: Path =  REGISTRIES_DIR / "document_registry.csv"
    BASE_REGISTRY: Path = REGISTRIES_DIR / "base_output_registry.csv"
    RAW_REGISTRY: Path = SUBREGISTRIES_DIR / "raw_file_output_registry.csv"
    EXTRACTION_REGISTRY: Path = SUBREGISTRIES_DIR / "extraction_output_registry.csv"
    DAS_CLASSIFICATION_REGISTRY: Path = SUBREGISTRIES_DIR / "DAS_classification_output_registry.csv"
    CAS_CLASSIFICATION_REGISTRY: Path = SUBREGISTRIES_DIR / "CAS_classification_output_registry.csv"

    version_object = VersionObject(pipeline_version="v1.0.0", software_version="v1.0.0")
    df_document_registry = pd.read_csv(DOCUMENT_REGISTRY)
    df_base_registry = pd.read_csv(BASE_REGISTRY)
    df_raw_registry = pd.read_csv(RAW_REGISTRY)
    df_extraction_registry = pd.read_csv(EXTRACTION_REGISTRY)
    df_DAS_classification_registry = pd.read_csv(DAS_CLASSIFICATION_REGISTRY)
    df_CAS_classification_registry = pd.read_csv(CAS_CLASSIFICATION_REGISTRY)

    """function to ingest data"""
    
    # this function makes call to the SCOAP repository, downloads papers and scrapes metadata
    # allows to query data for different publiccation periods, default is papers published in 2025

    df_document_registry, df_base_registry, df_raw_registry = data_ingestion(
        df_document_registry,
        df_base_registry,
        df_raw_registry,
        version_object,
    )

    # main function running all higher-level pipeline modules defined above

    classify_files(df_document_registry, df_base_registry, df_raw_registry, df_extraction_registry, df_DAS_classification_registry, df_CAS_classification_registry, version_object)
