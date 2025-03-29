#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Metadata Quality Assessment (MQA) for Berlin Open Data Portal

This module implements the MQA scoring logic for Berlin's Open Data Portal,
adapting the framework from opendata.swiss to Berlin's DCAT-AP.de schema.
It evaluates metadata quality across the FAIR dimensions:
- Findability (100 points): keywords, categories, spatial/temporal coverage
- Accessibility (100 points): URL accessibility and resource availability
- Interoperability (110 points): format standards, machine-readability
- Reusability (75 points): licensing, contact information, publisher details
- Context (20 points): additional metadata like dates and file sizes

The implementation is specifically adapted for Berlin's Open Data Portal:
- Validates against DCAT-AP.de controlled vocabularies
- Supports Berlin-specific license IDs (dl-de-by-2.0, etc.)
- Recognizes common Berlin formats including WFS/WMS services

The module provides functions to:
- Check individual quality indicators
- Calculate scores for each dimension
- Determine an overall quality rating
"""

from typing import Dict, Any, List, Callable, Tuple, Optional
import re
import requests
from tqdm import tqdm
import pandas as pd
import json
import os

# ------------------------------------------------
# Constants and Configuration
# ------------------------------------------------

# Mapping of field names from Berlin's CKAN API to standard DCAT-AP fields
BERLIN_TO_DCAT_FIELD_MAPPING = {
    # Findability
    "tags": "dcat:keyword",
    "groups": "dcat:theme",
    "geographical_coverage": "dct:spatial",
    "temporal_coverage_from": "dct:temporal_start",
    "temporal_coverage_to": "dct:temporal_end",
    
    # Accessibility
    "url": "dcat:accessURL",
    "resources": "dcat:distribution",
    
    # Interoperability
    "resources.format": "dct:format",
    "resources.mimetype": "dcat:mediaType",
    
    # Reusability
    "license_id": "dct:license",
    "license_title": "dct:license_title",
    "license_url": "dct:license_url",
    "maintainer": "dcat:contactPoint",
    "maintainer_email": "dcat:contactPoint_email",
    "author": "dct:publisher",
    "author_email": "dct:publisher_email",
    
    # Context
    "date_released": "dct:issued",
    "date_updated": "dct:modified"
}

# Define the accepted MIME types from data.europa.eu and DCAT-AP.de
ACCEPTED_MIME_TYPES = [
    "text/csv", "application/json", "application/xml", 
    "application/geopackage+sqlite3", "application/gml+xml",
    "application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/zip", "application/pdf", "application/wfs", "application/wms"
]

# Non-proprietary formats
NON_PROPRIETARY_FORMATS = [
    "csv", "json", "xml", "gml", "geojson", "gpkg", "txt", "markdown", "md",
    "html", "htm", "zip", "rdf", "nt", "ttl", "n3", "jsonld", "trig", "wfs", "wms"
]

# Machine-readable formats
MACHINE_READABLE_FORMATS = [
    "csv", "json", "xml", "rdf", "nt", "ttl", "n3", "jsonld", "trig",
    "geojson", "gpkg", "gml", "xlsx", "xls", "ods", "wfs"
]

# Valid DCAT-AP.de license IDs
VALID_DCAT_AP_DE_LICENSES = [
    "cc-zero", "cc-by", "cc-by-sa", "cc-by/4.0", "cc-nc",
    "dl-de-zero-2.0", "dl-de-by-2.0", "odc-odbl", "CC BY 3.0 DE",
    # Even though "other-closed" is not an open license, it's a valid identifier
    "other-closed"
]

# Values that indicate hidden nulls
HIDDEN_NULLS = (
    "", "null", "[]", "{}", "nan", "none", "ohne angabe", 
    "keine angabe", "nichts", "N/A", "n/a"
)

# Define maximum points per dimension
MAX_POINTS_PER_DIMENSION = {
    "Findability": 100,
    "Accessibility": 100,
    "Interoperability": 110,
    "Reusability": 75,
    "Context": 20
}

# ------------------------------------------------
# Helper Functions
# ------------------------------------------------

def clamp_score(value: int, max_points: int) -> int:
    """Clamp a score between 0 and max_points."""
    return max(0, min(value, max_points))

def check_presence(metadata: Dict[str, Any], field_name: str) -> bool:
    """
    Returns True if 'field_name' is present in 'metadata' and not empty.
    Handles various forms of "empty" values.
    """
    if field_name not in metadata:
        return False
    
    value = metadata[field_name]
    
    # Handle None
    if value is None:
        return False
        
    # Handle empty strings and hidden nulls
    if isinstance(value, str):
        if value.lower() in HIDDEN_NULLS or value.strip() == "":
            return False
        return True
        
    # Handle lists
    if isinstance(value, list):
        if not value or all(not item for item in value):
            return False
        return True
        
    # Handle dictionaries
    if isinstance(value, dict):
        if not value:
            return False
        return True
        
    # Default case for other types
    return True

def is_url_accessible(url: str) -> bool:
    """Check if a URL is accessible (returns 2xx or 3xx status code)."""
    try:
        response = requests.head(url, timeout=5)
        return response.status_code < 400
    except:
        return False

def check_format_in_register(format_value: str) -> bool:
    """
    Check if the format is in a recognized format register
    based on DCAT-AP.de controlled vocabularies.
    """
    if not format_value:
        return False
        
    format_lower = format_value.lower()
    
    # Check against MIME types
    for mime in ACCEPTED_MIME_TYPES:
        if format_lower in mime.lower():
            return True
    
    # Check common formats used in Berlin's data portal
    common_formats = [
        "csv", "json", "xml", "wfs", "wms", "pdf", "zip", "xls", "xlsx", 
        "html", "geojson", "gml", "kml", "shp", "gpkg", "gis"
    ]
    
    # Check if the format is one of the common formats
    if format_lower in common_formats:
        return True
        
    # Check if format contains one of the common formats (e.g., "EXCEL", "CSV-Datei")
    for fmt in common_formats:
        if fmt in format_lower:
            return True
    
    return False

def extract_resources_formats(metadata: Dict[str, Any]) -> List[str]:
    """Extract all format values from resources."""
    formats = []
    
    if "resources" in metadata and isinstance(metadata["resources"], list):
        for resource in metadata["resources"]:
            if isinstance(resource, dict) and "format" in resource:
                format_value = resource.get("format", "")
                if format_value and format_value.strip() and format_value.lower() not in HIDDEN_NULLS:
                    formats.append(format_value.lower())
                    
    return formats

def extract_resources_mimetypes(metadata: Dict[str, Any]) -> List[str]:
    """Extract all mimetype values from resources."""
    mimetypes = []
    
    if "resources" in metadata and isinstance(metadata["resources"], list):
        for resource in metadata["resources"]:
            if isinstance(resource, dict) and "mimetype" in resource:
                mimetype = resource.get("mimetype", "")
                if mimetype and mimetype.strip() and mimetype.lower() not in HIDDEN_NULLS:
                    mimetypes.append(mimetype.lower())
                    
    return mimetypes

def extract_resources_urls(metadata: Dict[str, Any]) -> List[str]:
    """Extract all URL values from resources."""
    urls = []
    
    if "resources" in metadata and isinstance(metadata["resources"], list):
        for resource in metadata["resources"]:
            if isinstance(resource, dict) and "url" in resource:
                url = resource.get("url", "")
                if url and url.strip() and url.lower() not in HIDDEN_NULLS:
                    urls.append(url.lower())
                    
    return urls

def get_best_distribution_score(metadata: Dict[str, Any], 
                               indicator_functions: List[Tuple[str, Callable, int]]) -> int:
    """
    Calculate the maximum score for a single distribution (resource).
    Returns the highest score among all resources for the given indicators.
    """
    if "resources" not in metadata or not metadata["resources"]:
        return 0
        
    max_score = 0
    
    for resource in metadata["resources"]:
        resource_score = 0
        
        # Create a resource-specific metadata dictionary
        resource_metadata = {**metadata, "resource": resource}
        
        for name, check_func, points in indicator_functions:
            if check_func(resource_metadata):
                resource_score += points
                
        max_score = max(max_score, resource_score)
        
    return max_score

# ------------------------------------------------
# 1. Define MQA Indicators for each dimension
# ------------------------------------------------

# ----- A. Findability (Auffindbarkeit) -----
INDICATORS_FINDABILITY = [
    {
        "name": "Keywords (Schlagwörter)",
        "field": "dcat:keyword",
        "max_points": 30,
        "check_func": lambda meta: check_presence(meta, "tags") and isinstance(meta["tags"], list) and len(meta["tags"]) > 0,
    },
    {
        "name": "Categories (Kategorien)",
        "field": "dcat:theme",
        "max_points": 30,
        "check_func": lambda meta: check_presence(meta, "groups") and isinstance(meta["groups"], list) and len(meta["groups"]) > 0,
    },
    {
        "name": "Spatial Coverage (Räumliche Abdeckung)",
        "field": "dct:spatial",
        "max_points": 20,
        "check_func": lambda meta: check_presence(meta, "geographical_coverage"),
    },
    {
        "name": "Temporal Coverage (Zeitliche Abdeckung)",
        "field": "dct:temporal",
        "max_points": 20,
        "check_func": lambda meta: (check_presence(meta, "temporal_coverage_from") or 
                                 check_presence(meta, "temporal_coverage_to")),
    },
]

# ----- B. Accessibility (Zugänglichkeit) -----
INDICATORS_ACCESSIBILITY = [
    {
        "name": "Access URL Accessibility (Zugänglichkeit der AccessURL)",
        "field": "dcat:accessURL_is_reachable",
        "max_points": 50,
        "check_func": lambda meta: check_presence(meta, "url") and is_url_accessible(meta["url"]),
    },
    {
        "name": "Download URL Presence (Download URL vorhanden)",
        "field": "dcat:downloadURL",
        "max_points": 20,
        "check_func": lambda meta: check_presence(meta, "resources") and len(extract_resources_urls(meta)) > 0,
    },
    {
        "name": "Download URL Accessibility (Zugänglichkeit der Download URL)",
        "field": "dcat:downloadURL_is_reachable",
        "max_points": 30,
        "check_func": lambda meta: check_presence(meta, "resources") and 
                                any(is_url_accessible(url) for url in extract_resources_urls(meta)),
    },
]

# ----- C. Interoperability (Interoperabilität) -----
# Note: Only the distribution with the highest score counts for interoperability
INDICATORS_INTEROPERABILITY = [
    {
        "name": "Format",
        "field": "dct:format",
        "max_points": 20,
        "check_func": lambda meta: check_presence(meta, "resources") and len(extract_resources_formats(meta)) > 0,
    },
    {
        "name": "Media Type (Medientyp)",
        "field": "dcat:mediaType",
        "max_points": 10,
        "check_func": lambda meta: check_presence(meta, "resources") and len(extract_resources_mimetypes(meta)) > 0,
    },
    {
        "name": "Format/Media Type from Vocabulary (Format/Medientyp aus Vokabular)",
        "field": "format_media_vocab_check",
        "max_points": 10,
        "check_func": lambda meta: check_presence(meta, "resources") and 
                                any(check_format_in_register(fmt) for fmt in extract_resources_formats(meta)) or
                                any(mime in ACCEPTED_MIME_TYPES for mime in extract_resources_mimetypes(meta)),
    },
    {
        "name": "Non-proprietary (Nicht-proprietär)",
        "field": "non_proprietary_format_check",
        "max_points": 20,
        "check_func": lambda meta: check_presence(meta, "resources") and 
                                any(fmt.lower() in NON_PROPRIETARY_FORMATS 
                                    for fmt in extract_resources_formats(meta)),
    },
    {
        "name": "Machine-readability (Maschinenlesbarkeit)",
        "field": "machine_readable_format_check",
        "max_points": 20,
        "check_func": lambda meta: check_presence(meta, "resources") and 
                                any(fmt.lower() in MACHINE_READABLE_FORMATS 
                                    for fmt in extract_resources_formats(meta)),
    },
    {
        "name": "DCAT-AP.de Conformity (DCAT-AP.de Konformität)",
        "field": "dcat_ap_de_conformance",
        "max_points": 30,
        # Currently automatically granted, as in the Swiss model
        "check_func": lambda meta: True,
    },
]

# ----- D. Reusability (Wiederverwendbarkeit) -----
INDICATORS_REUSABILITY = [
    {
        "name": "License (Lizenz)",
        "field": "dct:license",
        "max_points": 20,
        "check_func": lambda meta: check_presence(meta, "license_id"),
    },
    {
        "name": "License Vocabulary (Lizenzvokabular)",
        "field": "license_vocab_check",
        "max_points": 10,
        # Check against DCAT-AP.de vocabulary
        "check_func": lambda meta: check_presence(meta, "license_id") and 
                             meta.get("license_id", "").lower() in [lic.lower() for lic in VALID_DCAT_AP_DE_LICENSES],
    },
    {
        "name": "Access Rights Level (Zugänglichkeitsgrad)",
        "field": "dct:accessRights",
        "max_points": 10,
        # Automatically granted as per the Swiss model
        "check_func": lambda meta: True,
    },
    {
        "name": "Access Rights from Vocabulary (Zugänglichkeitsgrad aus Vokabular)",
        "field": "access_rights_vocab_check",
        "max_points": 5,
        # Automatically granted as per the Swiss model
        "check_func": lambda meta: True,
    },
    {
        "name": "Contact Point (Kontakt)",
        "field": "dcat:contactPoint",
        "max_points": 20,
        "check_func": lambda meta: check_presence(meta, "maintainer") or 
                                check_presence(meta, "maintainer_email"),
    },
    {
        "name": "Publisher (Publizierender)",
        "field": "dct:publisher",
        "max_points": 10,
        "check_func": lambda meta: check_presence(meta, "author") or 
                                check_presence(meta, "organization.title"),
    },
]

# ----- E. Context (Kontext) -----
INDICATORS_CONTEXT = [
    {
        "name": "Usage Terms (Nutzungsbedingungen)",
        "field": "dct:rights",
        "max_points": 5,
        # Berlin doesn't have a direct equivalent for dct:rights, so check license info
        "check_func": lambda meta: check_presence(meta, "license_title"),
    },
    {
        "name": "Byte Size (Grösse in Bytes)",
        "field": "dcat:byteSize",
        "max_points": 5,
        "check_func": lambda meta: check_presence(meta, "resources") and 
                                any(check_presence(resource, "size") 
                                    for resource in meta["resources"] if isinstance(resource, dict)),
    },
    {
        "name": "Release Date (Veröffentlichungsdatum)",
        "field": "dct:issued",
        "max_points": 5,
        "check_func": lambda meta: check_presence(meta, "date_released"),
    },
    {
        "name": "Modification Date (Aktualisierungsdatum)",
        "field": "dct:modified",
        "max_points": 5,
        "check_func": lambda meta: check_presence(meta, "date_updated"),
    },
]

# Group indicators by dimension
MQA_DIMENSIONS = {
    "Findability": INDICATORS_FINDABILITY,
    "Accessibility": INDICATORS_ACCESSIBILITY,
    "Interoperability": INDICATORS_INTEROPERABILITY,
    "Reusability": INDICATORS_REUSABILITY,
    "Context": INDICATORS_CONTEXT
}

# ------------------------------------------------
# 2. Main Scoring Function
# ------------------------------------------------

def calculate_mqa_score(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate Metadata Quality Assessment (MQA) score for a dataset.
    
    Args:
        metadata: Dictionary containing dataset metadata fields
    
    Returns:
        Dictionary with dimension scores, total score, final rating,
        and detailed information about which indicators passed/failed
    """
    dimension_scores = {}
    total_score = 0
    detailed_results = {}
    
    # Calculate score for each dimension
    for dimension_name, indicators in MQA_DIMENSIONS.items():
        dim_score = 0
        max_dim_score = MAX_POINTS_PER_DIMENSION[dimension_name]
        dimension_details = []
        
        # Special case for Interoperability which uses distribution-specific logic
        if dimension_name == "Interoperability":
            # Convert indicators to format needed by get_best_distribution_score
            indicator_tuples = [(ind["name"], ind["check_func"], ind["max_points"]) 
                              for ind in indicators]
            dim_score = get_best_distribution_score(metadata, indicator_tuples)
            
            # Record details for each indicator (simplified for distributions)
            for ind in indicators:
                passed = ind["check_func"](metadata)
                points = ind["max_points"] if passed else 0
                dimension_details.append({
                    "indicator": ind["name"],
                    "field": ind["field"],
                    "max_points": ind["max_points"],
                    "points": points,
                    "passed": passed
                })
        else:
            # Standard dimension scoring
            for ind in indicators:
                passed = ind["check_func"](metadata)
                points = ind["max_points"] if passed else 0
                dim_score += points
                
                # Record details for this indicator
                dimension_details.append({
                    "indicator": ind["name"],
                    "field": ind["field"],
                    "max_points": ind["max_points"],
                    "points": points,
                    "passed": passed
                })
        
        # Ensure dimension score does not exceed official maximum
        dim_score = clamp_score(dim_score, max_dim_score)
        dimension_scores[dimension_name] = dim_score
        total_score += dim_score
        detailed_results[dimension_name] = dimension_details
    
    # Get the final rating category
    rating = get_final_rating(total_score)
    
    return {
        "dimension_scores": dimension_scores,
        "total_score": total_score,
        "rating": rating,
        "detailed_results": detailed_results
    }

def get_final_rating(score: int) -> str:
    """
    Returns the rating category based on the total score.
    
    Args:
        score: Total MQA score
        
    Returns:
        Rating category: Excellent, Good, Sufficient, or Poor
    """
    if 351 <= score <= 405:
        return "Ausgezeichnet" # Excellent
    elif 221 <= score <= 350:
        return "Gut" # Good
    elif 121 <= score <= 220:
        return "Ausreichend" # Sufficient
    else:
        return "Mangelhaft" # Poor
        
# ------------------------------------------------
# 3. Batch Processing Functions
# ------------------------------------------------

def process_datasets(datasets: List[Dict[str, Any]], 
                    output_file: Optional[str] = None,
                    show_progress: bool = True) -> pd.DataFrame:
    """
    Process multiple datasets, calculating MQA score for each one.
    
    Args:
        datasets: List of dataset metadata dictionaries
        output_file: Optional path to save results as CSV
        show_progress: Whether to show a progress bar
        
    Returns:
        DataFrame with MQA results for all datasets
    """
    results = []
    
    # Use tqdm for progress tracking if requested
    iterator = tqdm(datasets, desc="Processing datasets") if show_progress else datasets
    
    for dataset in iterator:
        # Skip datasets that don't have crucial metadata
        if not dataset.get("title") or not dataset.get("id"):
            continue
            
        try:
            # Calculate MQA score
            mqa_result = calculate_mqa_score(dataset)
            
            # Create a row with dataset info and scores
            result_row = {
                "id": dataset.get("id", ""),
                "title": dataset.get("title", ""),
                "organization": dataset.get("organization", {}).get("title", "") 
                               if isinstance(dataset.get("organization"), dict) else "",
                "total_score": mqa_result["total_score"],
                "rating": mqa_result["rating"],
            }
            
            # Add dimension scores
            for dimension, score in mqa_result["dimension_scores"].items():
                result_row[f"{dimension}_score"] = score
                
            results.append(result_row)
        except Exception as e:
            print(f"Error processing dataset {dataset.get('id', 'unknown')}: {e}")
    
    # Create DataFrame from results
    result_df = pd.DataFrame(results)
    
    # Save to CSV if requested
    if output_file:
        result_df.to_csv(output_file, index=False)
        
    return result_df

def load_datasets_from_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Load datasets from a JSON or CSV file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        List of dataset metadata dictionaries
    """
    if file_path.endswith('.json'):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
        return df.to_dict('records')
    else:
        raise ValueError(f"Unsupported file format: {file_path}")

# ------------------------------------------------
# 4. Example Usage
# ------------------------------------------------

def main():
    """
    Example usage of the MQA scoring system.
    """
    # Check if data file exists
    data_path = "notebooks/01_dataset_metadata.csv"
    if not os.path.exists(data_path):
        print(f"Error: Data file not found at {data_path}")
        return
        
    print(f"Loading datasets from {data_path}...")
    datasets = []
    
    # Read CSV file
    df = pd.read_csv(data_path)
    
    # Process string columns that might contain JSON
    for col in ['resources', 'tags', 'groups', 'extras']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: 
                                   json.loads(x) if isinstance(x, str) else x)
    
    datasets = df.to_dict('records')
    print(f"Loaded {len(datasets)} datasets.")
    
    # Calculate MQA scores
    output_path = "_results/mqa_scores.csv"
    results = process_datasets(datasets, output_file=output_path)
    
    # Print summary
    print("\nMetadata Quality Assessment Results:")
    print(f"Average score: {results['total_score'].mean():.2f}")
    print("\nRating distribution:")
    print(results['rating'].value_counts())
    
    print(f"\nResults saved to {output_path}")

if __name__ == "__main__":
    main()