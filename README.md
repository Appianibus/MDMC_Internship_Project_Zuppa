# SCOAP repository analysis through Availability Statement Analyzer

The following is a python pipeline for collecting SCOAP3 papers, extracting Data Availability Statements (DAS) and Code Availability Statements (CAS) sections, cleaning the extracted sections, and classifying them according to described access conditions of data/code.

The project combines rule-based text processing with LLM-assisted classification. It was developed as part of a research workflow focused on the analysis of DAS/CAS practices in high-energy physics publications as a general indication of scientific reproducibility.

## Scope

The project currently supports:

- retrieval of SCOAP3 records and associated PDF/XML files
- structured metadata tracking at document, file, extraction, embedding, and classification level
- PDF-to-text conversion with provenance-aware output tracking
- DAS/CAS section detection using configurable header patterns and boundary rules
- text normalization for common PDF extraction artifacts
- LLM-based DAS and CAS classification using project-specific schemas
- downstream notebook-based exploration, visualization, and statistical analysis

## Project Layout

```text
src/
  application/
    main.py        # orchestration file with higher-order helpers
    pipeline.py    # stage implementations with base functions
    utils.py       # registry, hashing, cleaning, and helper utilities
  config/
    config.py
    config.yaml
    classification_schema_DAS.yaml
    classification_schema_CAS_MCA.yaml
    classification_schema_CAS_ETA.yaml
    CAS_prompt.txt

data/
  metadata/
    registries_schema.md
    registries/
      document_registry.csv
      base_output_registry.csv
      output_subregistries/
        raw_file_output_registry.csv
        extraction_output_registry.csv
        DAS_classification_output_registry.csv
        CAS_classification_output_registry.csv
        embedding_output_registry.csv
  records/
    software_version/
      <software_version>/pipeline_version/<pipeline_version>/<doi_folder>/

notebooks/         # exploratory analysis, validation, plotting
docs/              # public documentation for data and registry schemas
```

## Pipeline Overview

The main processing flow is defined in [src/application/main.py](src/application/main.py) and the stage-level implementations live in [src/application/pipeline.py](src/application/pipeline.py).

The pipeline is organized into the following stages:

1. `data_ingestion`
   Downloads SCOAP3 records for a date range, stores PDF/XML files, and records document-level metadata.
2. `extract_texts`
   Converts PDF files to txt files and creates provenance-linked registry entries.
3. `extract_sections`
   Detects and extracts DAS and CAS sections from the text files using regular expressions.
4. `normalize_sections`
   Repairs extraction artifacts (e.g. ligatures, broken hyphenation, malformed hyperlinks) though utility functions
5. `classify_DAS_sections`
   Assigns DAS categories with confidence labels using a schema-guided LLM prompt.
6. `classify_CAS_sections`
   Assigns CAS labels for manuscript code availability (MCA) and external tool availability (ETA), again with confidence labels.

## Data Model And Provenance

One of the central design choices in this project is to track every processing step through registries and hash-based identifiers.

- `document_registry.csv` stores document-level metadata such as DOI, title, journal, year, source URLs, and whether DAS/CAS/XML are present.
- `base_output_registry.csv` stores a generalized record of derived outputs, including `output_sha`, `output_type`, software version, pipeline version, creation date, and dependency links.
- `raw_file_output_registry.csv` tracks raw downloaded files such as PDFs and XMLs.
- `extraction_output_registry.csv` tracks extracted and cleaned DAS/CAS sections.
- `DAS_classification_output_registry.csv` stores DAS labels, confidence scores, model name, and classified text.
- `CAS_classification_output_registry.csv` stores MCA/ETA labels, confidence scores, model name, and classified text.

Each derived artifact is linked to its parent through the `dependencies` field, which makes it possible to reconstruct how a specific output was produced.

The source repository documents the expected `data/` layout but does not include the full research data package. Paths under `data/` are intended to resolve after the external data release has been downloaded into the repository root, or after the pipeline has recreated the same layout locally. Registry field definitions are documented in [docs/registries_schema.md](docs/registries_schema.md).

## Configuration

Project configuration lives in [src/config/config.yaml](src/config/config.yaml).

This file contains:

- paths for records and registries
- DAS and CAS header patterns used for section extraction
- section boundary patterns
- text cleaning settings such as ligature mappings

The additional file [src/config/config.py](src/config/config.py) defines:

- the class object VersionObject, used to define software/pipeline version
- importable directory constants for registries, subregistries and record paths

Classification behavior is defined separately in:

- [src/config/classification_schema_DAS.yaml](src/config/classification_schema_DAS.yaml)
- [src/config/classification_schema_CAS_MCA.yaml](src/config/classification_schema_CAS_MCA.yaml)
- [src/config/classification_schema_CAS_ETA.yaml](src/config/classification_schema_CAS_ETA.yaml)
- [src/config/CAS_prompt.txt](src/config/CAS_prompt.txt)

## Installation

The project targets Python 3.12+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Core dependencies are declared in [pyproject.toml](pyproject.toml).

## Environment Variables

LLM-based classification requires an Anthropic API key:

```bash
export ANTHROPIC_API_KEY="your-key-here"
```

The project loads environment variables via `python-dotenv`, so a local `.env` file also works.

## Minimal Usage

There is currently no dedicated CLI entry point. The main workflow is called from Python by loading the registries and passing them through the orchestration helpers.

```python
import pandas as pd

from application.main import classify_files
from application.pipeline import data_ingestion
from application.utils import (
    load_base_registry,
    load_CAS_classification_registry,
    load_DAS_classification_registry,
    load_document_registry,
    load_extraction_registry,
)
from config.config import VersionObject

version = VersionObject(pipeline_version="v1.0.0", software_version="v1.0.4")

df_document = load_document_registry()
df_base = load_base_registry()
df_raw = load_raw_registry()
df_extraction = load_extraction_registry()
df_das = load_DAS_classification_registry()
df_cas = load_CAS_classification_registry()

# optional: ingest new SCOAP3 records for a date range
df_document, df_base, df_raw = data_ingestion(
    df_document=df_document,
    df_base=df_base,
    df_raw=df_raw,
    version_object=version,
    date_start="2025-01-01",
    date_end="2025-12-31",
)

# run extraction, cleaning, and classification stages
classify_files(
    df_document=df_document,
    df_base=df_base,
    df_raw=df_raw,
    df_extraction=df_extraction,
    df_DAS_classification=df_das,
    df_CAS_classification=df_cas,
    version_object=version,
)
```

## Notebooks

The `notebooks/` directory contains exploratory and analysis notebooks for:

- metadata querying
- text extraction and section debugging
- DAS/CAS classification experiments
- embedding computation and clustering
- statistical analysis and figure generation

These notebooks are part of the research workflow, but they are not a substitute for a formal pipeline interface.

## Current Limitations

- classification depends on a proprietary LLM API
- exact reruns may vary if external models change over time
- the repository does not yet expose a stable command-line interface
- tests are currently limited and include maintenance-oriented scripts rather than a polished automated suite
- section extraction is sensitive to PDF parsing artifacts and heterogeneous journal formatting

## License

The software code and original project documentation in this repository are licensed under the MIT License. See [LICENSE](LICENSE).

Specifically, the MIT license applies to the following research artifacts:

- source code under [src/application/](src/application/)
- configuration and schema files under [src/config/](src/config/)
- original project documentation, including the README file

This license statement does not grant rights over third-party source publication files, publication text, or other materials obtained from SCOAP3, publishers, or external repositories. Those materials remain subject to their original access and reuse terms. A separate data notice should describe the status of source files, extracted text, registries, classification outputs, embeddings, plots, and other research artifacts.

## Research Context

This repository was developed in the context of analyzing how scientific papers in SCOAP3 report data and code availability, with special attention to computational reproducibility and FAIR-aligned research data management.

The software therefore serves two roles:

- a processing pipeline for collecting and transforming publication data
- a research instrument for studying DAS/CAS presence, content, and reproducibility implications
