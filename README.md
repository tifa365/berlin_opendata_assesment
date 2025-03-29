# Berlin Open Data Catalog Metadata Quality Assessment

This project implements a metadata quality assessment framework for the Berlin Open Data Portal, based on FAIR principles (Findable, Accessible, Interoperable, Reusable).

## Project Overview

The Berlin Open Data Portal provides public datasets through a CKAN-based API. This tool evaluates the quality of the metadata provided with each dataset, generating scores across multiple dimensions and providing an overall quality rating.

This project is based on the Metadata Quality Assessment (MQA) methodology used by opendata.swiss, which itself was adapted from the data.europa.eu approach. As the source code for the official Swiss implementation has not been publicly released, our implementation is derived from public documentation of their methodology and scoring system. While we aim to mirror the Swiss methodology as closely as possible, this is not an official implementation nor is it endorsed by opendata.swiss or data.europa.eu.

Important note: Due to fundamental differences in the implementation of data catalogs between Switzerland and Germany (including different metadata schemas, vocabularies, and field definitions), our results will necessarily differ from the Swiss implementation. The assessment scores should be considered approximations that may vary from the original methodology and should be used primarily as relative indicators of metadata quality rather than absolute measurements.

Our implementation has been adapted to work with Berlin's DCAT-AP.de metadata schema while maintaining the same scoring principles. This adaptation includes mapping Berlin's specific metadata fields to the FAIR dimensions and ensuring compatibility with Berlin's controlled vocabularies, licenses, and resource formats. By applying this methodology to Berlin's Open Data Portal, we enable data providers to check the quality of their metadata and receive improvement suggestions based on established European standards.

## Features

- Direct integration with Berlin's Open Data API
- Automatic downloading and caching of metadata in Parquet format (optimized for efficient storage)
- Comprehensive metadata quality assessment using FAIR principles
- Scoring across five dimensions: Findability, Accessibility, Interoperability, Reusability, and Context
- Support for Berlin's DCAT-AP.de metadata schema
- CSV output with detailed quality scores
- Detailed analysis for troubleshooting metadata issues

## Future Plans

A web-based metadata quality dashboard for Berlin Open Data is planned, inspired by the Swiss opendata.swiss dashboard (https://dashboard.opendata.swiss/). The planned dashboard will feature:

- Overview of metadata quality across all Berlin Open Data organizations
- Detailed breakdown of scores by dimension with visual indicators
- Organization-specific views and comparisons
- Recommendations for improving metadata quality
- Regular updates reflecting the current state of Berlin's data catalog

The dashboard will help data providers in Berlin improve their metadata quality and make their datasets more findable, accessible, interoperable, and reusable for the public.

## How It Works

1. **Data Retrieval**: The tool (`run_metadata_assessment.py`) connects to the Berlin Open Data API (`https://datenregister.berlin.de/api/`) and fetches metadata in batches
2. **Data Storage**: Metadata is automatically saved in the `data` directory in Parquet format with timestamps (e.g., `berlin_metadata_20250329.parquet`)
3. **Processing**: The metadata is processed using the FAIR principles scoring system (`metadata_quality_assessment.py`), which:
   - Evaluates each dataset across all five dimensions (Findability, Accessibility, Interoperability, Reusability, Context)
   - Maps Berlin's metadata fields to the corresponding FAIR indicators
   - Calculates dimensions scores and aggregates a total score
   - Assigns a quality rating based on the total score
4. **Results Generation**: Results are saved to the `results` directory in multiple formats:
   - **CSV Files** (`mqa_scores.csv`): Contains scores for all datasets with their total points and dimension-specific scores
   - **Rating Summary** (`ratings_summary.csv`): Shows distribution of quality ratings across all evaluated datasets
   - **Detailed JSON Analysis** (`detailed_first_dataset.json`): Provides in-depth assessment of a sample dataset, showing each individual indicator's evaluation (pass/fail), points awarded, and specifics of why points were or weren't awarded. This detailed analysis is limited to sample datasets to keep file sizes manageable while still providing insights into how the scoring works

## Getting Started

### Prerequisites

```bash
pip install -r requirements.txt
```

### Running the Assessment

1. Run with a smaller sample for testing (recommended first step):

   ```bash
   python src/run_metadata_assessment.py --fetch --sample 10
   ```

2. Fetch all metadata from the Berlin Open Data API and run the full assessment:

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

## Scoring System

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

## Project Structure

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

The project uses two main files: one for the actual scoring system (`metadata_quality_assessment.py`) and another for the command-line interface (`run_metadata_assessment.py`). This makes it easier to understand the code and allows each component to be used independently.