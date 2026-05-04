## v1.0.1 - 2026-03-19
### Changes

- classify_DAS_claude
    - changed default model from calude-sonet-4-6 to claude-haiku-4-5-20251001
    - introduced prompt caching mechanism for static information
    - reduced max tokens from 1000 to 500
- extract_section
    - changed start pattern first match so to NOT match "data availability statament" with lowercase "d" in "data"

### Justifications
- classify_DAS_claude
    - reduce processing speed and processing costs
- extract_section:
    - previous regex pattern was matching "data availability statement" in references for some papers

## v1.0.2 - 2026-03-20
### Changes
- extract_sections
    - changed end pattern regex for "Open Access" in order to not match "open access" with lowecase "o"
        - from "(?m)^\\s*[Oo]pen\\s+[Aa]ccess" to ""(?m)^\\s*Open\\s+[Aa]ccess"
    - changed start pattern for "Data availability" in order to not match "Data availability . . . ."
        - from "(?m)^\\s*Data\\s+[Aa]vailability\\s*(?:[Ss]tatement)?\\.?" to "(?m)^\\s*Data\\s+[Aa]vailability\\s*(?:[Ss]tatement)?(?![ \\t]*\\.[ \\t]*\\.)\\.?"
    - change start pattern regex for "Data Availability" with whitespaces in order to not match "data availability" with lowercase "d"
        - from "(?m)^\\s*[Dd]\\s*a\\s*t\\s*a\\s*[Aa]\\s*v\\s*a\\s*i\\s*l\\s*a\\s*b\\s*i\\s*l\\s*i\\s*t\\s*y\\s*(?:[Ss]\\s*t\\s*a\\s*t\\s*e\\s*m\\s*e\\s*n\\s*t)?" to "(?m)^\\s*D\\s*a\\s*t\\s*a\\s*[Aa]\\s*v\\s*a\\s*i\\s*l\\s*a\\s*b\\s*i\\s*l\\s*i\\s*t\\s*y\\s*(?:[Ss]\\s*t\\s*a\\s*t\\s*e\\s*m\\s*e\\s*n\\s*t)?"
- main
    - allowed for force_processing flag in higher-order in classify_sections
- pipeline
    - changed sha computation of failed/missing DAS to use software_version and not pipeline_version


### Justifications
- extract_sections
    - lower case "open access" was matching a string inside a DAS
    - "Data availability . . ." was matching the index with section titles at the beginning of the paper
    - The "Data availability" with whitespaces was still matching "data availability statements" in the references


## v1.0.3 - 2026-03-24
### Changes
- classification_schema
    - took off ambiguous case note in "unclear" that was identifying sections specifying data as available + embargo period as part of "unclear".

- extract_DAS_section_single_pdf:
    - changed old_sha search. Now it matches rows for extractions that are specifically about DAS extraction.
    - changed leading "\\s*" before section titles into [ \\t]*

- exrtact_CAS_section_single_pdf:
    - created section 

- compute_embeddings_single_file:
    - changed depency_sha computation to match sha computation of cleaned sections from compute_hashes(cleaned_section_path)
    to compute_hashes(cleaned_section_path, salt=f"cleaned:{doc_doi}_{version_object.software_version}")
    - changed embedding sha computation from compute_hashes(embedding_path) to compute_hashes(embedding_path, salt={doc_doi})
    

### Justifications
- classification_schema
    - That specific case was arbitrarily decided as being part of the "open_access" category. Ambiguous case note was contradictory

- extract_DAS_section_single_pdf:
    - it was necessary to change old_sha search because of the introduction of the function to extract CAS sections. Previous old_sha search did not differentiate rows for CAS/DAS. Therefore, rows for CAS section would have been deleted when forcibly reprocessing files.
    - the change in the whitespace pattern was due to a bug whereby "\\s*" was consuming ALL whitespace characters in the MASKED author's comment, therefore omitting it from extraction, the second one is a safer pattern that requires the space to be on the same line 

- compute_embeddings_single_file:
    - previous dependency_sha computation wasn't matching
    - previous output_sha computation wasn't unique (same text = same embedding = same sha)

## v1.0.4 - 2026-04-01
### Changes
-extact_CAS_section_single_pdf/config.yaml
    - added "(?m)^[ \\t]*[Ss]upplementary\\s*[Mm]aterial\\s*[Aa]nd\\s*[Dd]ata"
    - added "\\.\\n\\d+\\s+[A-Z]" 
    - changed end pattern ""(?m)^[ \\t]*[Aa]ppendix:?" to "(?m)^[ \\t]*Appendix:?"
    - changed start pattern from "(?m)^\\s*Code\\s+[Aa]vailability\\s*(?:[Ss]tatement)?(?![ \\t]*\\.[ \\t]*\\.)\\.?" to "(?m)^(?<![0-9]+\\.[0-9]+\\n)(\\s*)[Cc]ode\\s*[Aa]vailability\\s*(?:[Ss]tatement)?(?!\\s*and\\s+supplementary\\s+material)"
    - changed start pattern from "(?m)^\\s*C\\s*o\\s*d\\s*e\\s*[Aa]\\s*v\\s*a\\s*i\\s*l\\s*a\\s*b\\s*i\\s*l\\s*i\\s*t\\s*y\\s*(?:[Ss]\\s*t\\s*a\\s*t\\s*e\\s*m\\s*e\\s*n\\s*t)?" to " (?m)^(?<![0-9]+\\.[0-9]+\\n)(\\s*)C\\s*o\\s*d\\s*e\\s*[Aa]\\s*v\\s*a\\s*i\\s*l\\s*a\\s*b\\s*i\\s*l\\s*i\\s*t\\s*y\\s*(?:[Ss]\\s*t\\s*a\\s*t\\s*e\\s*m\\s*e\\s*n\\s*t)?(?!\\s*and\\s+supplementary\\s+material)"
    - changed end pattern from "(?m)^[ \\t]*[Cc]onflicts\\s+[Oo]f\\s+[Ii]nterest\\.?" to "(?m)^[ \\t]*[Cc]on(?:fl|ﬂ)icts\\s+[Oo]f\\s+[Ii]nterest\\.?"
    - changed end pattern from "(?m)^[ \\t]*[Dd]eclarations" to "(?m)^[ \\t]*[Dd]eclarations?"
    - changed end pattern from "(?m)^[ \\t]*Open\\s+[Aa]ccess" to "(?m)^\\s*Open\\s+[Aa]ccess"
    - changed end pattern from ""(?m)^[ \\t]*[Aa]cknowledegments\\.?" to "(?m)^[ \\t]*[Aa]cknowledgments\\.?"

- section_reparation_llm:
    - added function

- apply_section_reparation:
    - now calling section_reparation_llm instead of section_reparaiton
    - added flag to extract_sections

- extract_DAS_section_single_pdf:
    - changed end pattern from "(?m)^[ \\t]*[Cc]ode\\s+[Aa]\\s*vailability(?:\\s+[Ss]tatement)?\\.?" to "(?m)^[ \\t]*[Cc]ode\\s*[Aa]\\s*vailability(?:\\s*[Ss]tatement)?\\.?"
    - added end pattern "\\s*Physics\\s*Letters\\s*B"
    - added end pattern "(?m)^[ \\t]*[Aa]uthor\\s*[Ss]tatement"
    - added end pattern "[–-]?[0-9]+\\n+[–-]?[0-9]+\\n+[–-]?[0-9]+"
    - added end pattern "[Cc]o\\s*d\\s*e\\s*[Aa]\\s*v\\s*a\\s*i\\s*l\\s*i\\s*b\\s*i\\s*l\\s*i\\s*t\\s*y\\s*(?:[Ss]\\s*t\\s*a\\s*t\\s*e\\s*m\\s*e\\s*n\\s*t)?"
    - added end pattern "06490213"
    - added end pattern "0.2\\n0.4"

- data_cleaner_single_file:
    - changed function so that add_period is called only if text does not end with a period already

### Justifications
- extract_CAS_section_single_pdf/config.yaml:
    - incorrect extraction included section on supplementary materials and data
    - incorrect extraction included a footonote 
    - incorrect extraction was matching "appendix A" inside the CAS --> not a definitive fix though, it could be still be matching it if written capitalized AND it would fail if the appendix section title is misspelled with lowercase 
    - incorrect extraction due to regex matching pattern in the index, introduced negative lookbehind for index number + line break and introduced lookahead for "and supplementary material"
    - incorrect extraction due to ligature "ﬂ" not matching
    - incorrect extraction not matching "Declaration", made the final "s" optional 
    - not matching "Open Access" correctly, trying to chnage whitespace pattern 
    - "Acknowledegments" was misspelled 

section_reparation_llm:
    - simplified significantly section reparation mechanism
    - now only feeding the interrupted section and asking for the repaired version, much simpler

- extract_DAS_section_single_pdf:
    - regex was not matching "CodeAvailabilityStatement"
    - needed to add new pattern because section was extracted alongside footer with journal name
    - needed to add new pattern because section was extracted alongisde Author statement
    - needed to add new pattern because section was extracted alongside grid numbers from a graph 
    - needed to add new pattern because of OCR artifact "Code Avail i bil it y Statement"
    - needed to add specific pattern for a footer
    - needed to add new pattern because section was extracted alongside grid numbers from a graph (i.e. 0.2, 0.4, etc.)
 
- data_cleaner_single_file:
    - A lot of texts ended up with multiple periods otherwise

## v1.0.5 - 2026-05-04
### Changes
- classification_schema_CAS
    - added exemplar and note on CMS data preservation, reuse and open access policy to open_access category

- extract_CAS_section_single_pdf
    - changed the calculation of the dependency set to calculate an initial set of output shas where "section_type" in the extraction registry is "CAS". Use those shas to calculate the dependency set from the base registry
    - changed author comment masking so that masking is applied only after the CAS start header
    - changed masked author comments to preserve newlines while replacing non-newline characters with lowercase placeholders
    - changed end-pattern selection so that end matches producing empty or whitespace-only CAS sections are ignored

### Justifications
- classification_schema_CAS
    - Sections mentioning the CMS data preservation, reuse and open access policy where previously classified as "unclear". This is incorrect given the policy mandates open access data

- extract_CAS_section_single_pdf
    - fixed a bug where dependency calculation was not taking into account "section_type". Therefore, a large number of txt files whose DAS had been extracted were not processed for CAS extraction up until this point, since their sha was already present among dependencies for output_type == "extracted_section" in the base registry
    - fixed a bug where unclosed author comments before the CAS start could mask the actual CAS section, leading to empty/"nan" extractions
    - fixed a bug where the all-caps end pattern matched immediately after CAS headers followed by acronyms such as "ATLAS", leading to empty/"nan" extractions
    - full-population verification on existing successful CAS extraction rows showed no proposed extraction failures and no shortened outputs; the changed cases restored author comments or recovered missing CAS text


 
