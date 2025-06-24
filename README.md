# Berlin Open Data Catalog Metadata Quality Assessment

This project implements a metadata quality assessment framework for the Berlin Open Data Portal, based on FAIR principles (Findable, Accessible, Interoperable, Reusable).

## Project Overview

The Berlin Open Data Portal provides public datasets through a CKAN-based API. This tool evaluates the quality of the metadata provided with each dataset, generating scores across multiple dimensions and providing an overall quality rating.

This project implements the Metadata Quality Assessment (MQA) methodology developed by opendata.swiss, which is documented at [https://dashboard.opendata.swiss/de/methodik.html](https://dashboard.opendata.swiss/de/methodik.html). The Swiss MQA approach was itself adapted from the data.europa.eu methodology.

As the source code for the official Swiss implementation has not been publicly released, my implementation is derived from public documentation of their methodology and scoring system. While I aim to mirror the Swiss methodology as closely as possible, this is not an official implementation nor is it endorsed by opendata.swiss or data.europa.eu.

Important note: Due to fundamental differences in the implementation of data catalogs between Switzerland and Germany (including different meta data schemas, vocabularies, and field definitions), final results will necessarily differ from the Swiss implementation and do not provide solid ground for quality comparisons. The assessment scores should be considered approximations that may vary from the original methodology and should be used primarily as relative indicators of meta data quality rather than absolute measurements.

My implementation has been adapted to work with Berlin's DCAT-AP.de meta data schema while maintaining previous MQA scoring mechanism. This adaptation includes mapping Berlin's specific meta data fields to the FAIR dimensions and ensuring compatibility with Berlin's controlled vocabularies, licenses, and resource formats. The code is designed to be adaptable for use with other German data portals with minor adjustments, making it a valuable tool for improving metadata quality across Germany's open data landscape. By applying this methodology, data providers can systematically identify and improve the quality of their metadata.

## Features

- Direct integration with Berlin's Open Data API
- Automatic downloading and caching of metadata in Parquet format (optimized for efficient storage)
- Comprehensive metadata quality assessment using FAIR principles
- Scoring across five dimensions: Findability, Accessibility, Interoperability, Reusability, and Context
- Support for Berlin's DCAT-AP.de metadata schema
- CSV output with detailed quality scores
- Detailed analysis for troubleshooting metadata issues

## Future Plans

A web-based metadata quality dashboard for Berlin Open Data could be implemented, inspired by the Swiss opendata.swiss dashboard (https://dashboard.opendata.swiss/). The potential dashboard might feature:

- Overview of metadata quality across all Berlin Open Data organizations
- Detailed breakdown of scores by dimension with visual indicators
- Organization-specific views and comparisons
- Recommendations for improving metadata quality
- Regular updates reflecting the current state of Berlin's data catalog

The dashboard could help data providers in Berlin improve their metadata quality and make their datasets more findable, accessible, interoperable, and reusable for the public.

## How the Scripts Work

1. **Data Retrieval**: The tool (`run_metadata_assessment.py`) connects to the Berlin Open Data API (`https://datenregister.berlin.de/api/`) and fetches all metadata in batches
2. **Data Storage**: Metadata is automatically saved in the `data` directory in Parquet format with timestamps (e.g., `berlin_metadata_20250329.parquet`, including the date)
3. **Processing**: The metadata is processed using the FAIR principles scoring system (`metadata_quality_assessment.py`), which:
   - Evaluates each dataset across all five main dimensions (Findability, Accessibility, Interoperability, Reusability, Context)
   - Maps Berlin's metadata fields to the corresponding FAIR indicators
   - Calculates dimensions scores and aggregates a total score
   - Assigns a quality rating based on the total score
4. **Results Generation**: Results are saved to the `results` directory in multiple formats:
   - **CSV Files** (`mqa_scores.csv`): Contains scores for all datasets with their total points and dimension-specific scores
   - **Rating Summary** (`ratings_summary.csv`): Shows distribution of quality ratings across all evaluated datasets
   - **Detailed JSON Analysis** (`detailed_first_dataset.json`): Provides in-depth assessment of a sample dataset, showing each individual indicator's evaluation (pass/fail), points awarded, and specifics of why points were or weren't awarded. This detailed analysis is limited to sample datasets to keep file sizes manageable while still providing insights into how the scoring works

## Installation Process

### Prerequisites

```bash
pip install -r requirements.txt
```

### Running the Assessment

1. Run with a smaller sample for testing (recommended first step):

   ```bash
   python src/run_metadata_assessment.py --fetch --sample 10
   ```

2. If the small sample test turned out fine, fetch all metadata from the Berlin Open Data API and run the full assessment:

   ```bash
   python src/run_metadata_assessment.py --fetch --verbose
   ```

3. Or use existing metadata file:

   ```bash
   python src/run_metadata_assessment.py --input data/berlin_metadata.csv
   ```

4. View results in the `results` directory:
   - `mqa_scores.csv`: Overall scores for all datasets
   - `ratings_summary.csv`: Distribution of ratings
   - `detailed_first_dataset.json`: Detailed breakdown of scores for analysis

## How the Scoring Works

The assessment evaluates metadata quality across these dimensions:

1. **Findability (100 points max)**
   - Keywords/tags (30 points)
   - Categories/themes (30 points)
   - Spatial coverage (20 points)
   - Temporal coverage (20 points)

2. **Accessibility (100 points max)**
   - Access URL availability (50 points)
   - Download URL existence (20 points)
   - Download URL accessibility (30 points)

3. **Interoperability (110 points max)**
   - Format specification (20 points)
   - Media type (10 points)
   - Use of controlled vocabularies (10 points)
   - Non-proprietary format (20 points)
   - Machine-readability (20 points)
   - DCAT-AP.de conformity (30 points)

4. **Reusability (75 points max)**
   - License specification (20 points)
   - License vocabulary (10 points)
   - Access rights level (10 points)
   - Access rights vocabulary (5 points)
   - Contact information (20 points)
   - Publisher information (10 points)

5. **Context (20 points max)**
   - Usage terms (5 points)
   - Byte size (5 points)
   - Release date (5 points)
   - Update date (5 points)

### Quality Ratings

The total score (max 405 points) determines the final rating:

- **Excellent (Ausgezeichnet)**: 351-405 points
- **Good (Gut)**: 221-350 points
- **Sufficient (Ausreichend)**: 121-220 points
- **Poor (Mangelhaft)**: 0-120 points

## Overall Project Structure

- `/data`: Raw metadata files fetched from the API
- `/results`: Generated assessment results (CSV and JSON)
- `/src`: Core module code
  - `metadata_quality_assessment.py`: Core implementation of the FAIR scoring system
    - Contains the scoring logic, indicators, and dimension calculations
    - Provides reusable functions for processing datasets
  - `run_metadata_assessment.py`: User-facing script that:
    - Handles command-line arguments and user interaction
    - Fetches metadata directly from the Berlin Open Data API
    - Processes datasets using the core implementation
    - Generates and saves results

The project is organized into two main Python files, each with a specific role:

1. **`metadata_quality_assessment.py`**: Contains the core implementation of the scoring system - all evaluation logic, dimension calculations, and indicator definitions. This module is designed as a reusable library that can be imported into any Python application.

2. **`run_metadata_assessment.py`**: Provides the command-line interface, handles data fetching, file operations, and result formatting. This script uses the core implementation to process datasets and generate reports.

This separation allows the evaluation logic to be reused in different contexts (such as a web dashboard or API) while keeping the implementation details consistent.

## Collaboration

I welcome collaborations to extend this tool to other German data portals or to develop the web dashboard. If you're interested in:
- Adapting this tool for your data portal
- Contributing to the development of a metadata quality dashboard
- Discussing the technical implementation or methodology

Please feel free to open an issue or contact me directly.

## License

This project is licensed under the [MIT License](LICENSE), which permits use, copying, modification, distribution, and sublicensing, provided that the original copyright notice and permission notice are included.