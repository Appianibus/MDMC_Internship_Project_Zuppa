# Registries Documentation Schema

The following file describes the fields used by the metadata registries expected under `data/metadata/registries/`.

In the public source-code repository, the `data/` directory is not bundled. Registry paths documented here become resolvable after the external data release has been downloaded into the repository root, or after the pipeline has recreated the same `data/` layout locally.

## Document Registry

The registry stores metadata about each publication and can be accessed at `data/metadata/registries/document_registry.csv`.

- `doc_doi`: unique DOI associated with the publication
- `pdf_filename`: name of the PDF file associated with the publication
- `pdf_url`: URL of the PDF file associated with the publication
- `doc_type`: type of scientific publication, such as scientific study, erratum, or retraction note
- `has_DAS`: boolean field noting presence or absence of a Data Availability Statement
- `has_CAS`: boolean field noting presence or absence of a Code Availability Statement
- `doc_title`: title of the publication
- `journal`: journal of publication
- `publication_year`: year of publication
- `arxiv_eprints_category`: category of the preprint associated with the publication
- `country`: country or countries of institutional affiliation
- `has_XML`: boolean field noting presence or absence of an associated XML file
- `xml_filename`: name of the XML file associated with the publication
- `xml_url`: URL of the XML file associated with the publication
- `creation_date`: date the metadata entry was created in the registry

## Base Registry

The registry stores generic metadata about all pipeline outputs and can be accessed at `data/metadata/registries/base_output_registry.csv`.

- `output_sha`: hexadecimal SHA-1 hash used as a unique, persistent identifier for all pipeline outputs and as a key for the subregistries
- `doc_doi`: unique DOI associated with the publication, used as a foreign key to `document_registry.csv`
- `output_type`: type of pipeline output, such as raw file, extraction, or classification
- `pipeline_version`: processing logic or workflow used to produce the pipeline output
- `software_version`: version of the software used to produce the pipeline output
- `creation_date`: creation date of the pipeline output
- `dependencies`: hexadecimal SHA-1 hash of the output preceding the current output in the pipeline

## Raw File Registry

The registry stores metadata specific to raw files of publications, such as PDF, XML, and text files. It can be accessed at `data/metadata/registries/output_subregistries/raw_file_output_registry.csv`.

- `output_sha`: hexadecimal SHA-1 hash used as a unique, persistent identifier for raw files
- `file_type`: type of raw file
- `file_path`: repository-relative path of the raw file
- `file_size`: size in bytes of the raw file

## Extraction Registry

The registry stores metadata about the extraction and normalization stages and can be accessed at `data/metadata/registries/output_subregistries/extraction_output_registry.csv`.

- `output_sha`: hexadecimal SHA-1 hash used as a unique, persistent identifier for extracted sections at the pre-cleaning or cleaned stage
- `section_type`: type of availability statement, such as data or code
- `stage`: stage of the section, before or after normalization
- `file_path`: repository-relative path of an extracted or normalized section
- `text`: full text of an extracted or normalized section

## Embedding Registry

The registry stores metadata about embedded section texts and can be accessed at `data/metadata/registries/output_subregistries/embedding_output_registry.csv`.

- `output_sha`: hexadecimal SHA-1 hash used as a unique, persistent identifier for an embedded section
- `file_path`: repository-relative path of an embedded section file
- `model`: model used for embedding a section's text

## DAS Classification Registry

The registry stores metadata about classification outputs of Data Availability Statements and can be accessed at `data/metadata/registries/output_subregistries/DAS_classification_output_registry.csv`.

- `output_sha`: hexadecimal SHA-1 hash used as a unique, persistent identifier for classification outputs of DAS sections
- `label`: classification category assigned to the Data Availability Statement
- `method`: method used for classification
- `model`: model used for classification
- `confidence`: level of confidence in the classification
- `text`: text of the classified section

## CAS Classification Registry

The registry stores metadata about classification outputs of Code Availability Statements and can be accessed at `data/metadata/registries/output_subregistries/CAS_classification_output_registry.csv`.

- `output_sha`: hexadecimal SHA-1 hash used as a unique, persistent identifier for classification outputs of CAS sections
- `MCA_label`: classification category assigned to the Code Availability Statement for the dimension "Manuscript Code Availability"
- `MCA_confidence`: level of confidence in the classification for the dimension "Manuscript Code Availability"
- `ETA_label`: classification category assigned to the Code Availability Statement for the dimension "External Tool Availability"
- `ETA_confidence`: level of confidence in the classification for the dimension "External Tool Availability"
- `method`: method used for classification
- `text`: text of the classified section
- `model`: model used for classification
