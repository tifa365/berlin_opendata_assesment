#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Berlin Open Data Metadata Quality Assessment

This script:
1. Fetches metadata from the Berlin Open Data API or loads from local files
2. Calculates quality scores based on FAIR principles (Findable, Accessible, Interoperable, Reusable)
3. Generates detailed reports and summary statistics
4. Saves all results to the results directory
"""

import os
import sys
import json
import argparse
import pandas as pd
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional
from tqdm import tqdm

# Add parent directory to path to import local modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.metadata_quality_assessment import process_datasets, calculate_mqa_score
except ImportError:
    from metadata_quality_assessment import process_datasets, calculate_mqa_score

# Constants
BERLIN_API_URL = "https://datenregister.berlin.de/api/3/action/current_package_list_with_resources"
DEFAULT_DATA_DIR = "data"
DEFAULT_RESULTS_DIR = "results"

def fetch_metadata(
        limit: int = 500, 
        sleep: int = 2, 
        output_file: Optional[str] = None
    ) -> pd.DataFrame:
    """
    Fetch metadata from Berlin Open Data API
    
    Args:
        limit: Number of records to fetch per request
        sleep: Delay between requests in seconds
        output_file: Optional path to save the raw metadata
        
    Returns:
        DataFrame containing metadata for all datasets
    """
    import time
    
    print(f"Fetching metadata from Berlin Open Data API: {BERLIN_API_URL}")
    
    offset = 0
    frames = []
    
    while True:
        url = f"{BERLIN_API_URL}?limit={limit}&offset={offset}"
        print(f"Fetching records {offset} to {offset+limit}...")
        
        try:
            res = requests.get(url)
            data = res.json()
            
            if "result" not in data or not data["result"]:
                break
                
            # Convert to DataFrame
            df = pd.json_normalize(data["result"])
            frames.append(df)
            
            # Increment offset for next batch
            offset += limit
            time.sleep(sleep)
            
        except Exception as e:
            print(f"Error fetching data: {e}")
            break
    
    if not frames:
        raise ValueError("No data retrieved from API")
        
    # Combine all batches
    data = pd.concat(frames)
    print(f"Retrieved {len(data)} datasets from API")
    
    # Save raw data if requested
    if output_file:
        # Create directory if needed
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save in both CSV and Parquet formats
        data.to_csv(output_file, index=False)
        if output_file.endswith('.csv'):
            parquet_file = output_file.replace('.csv', '.parquet')
            data.to_parquet(parquet_file, index=False)
            print(f"Raw metadata saved to {output_file} and {parquet_file}")
        else:
            print(f"Raw metadata saved to {output_file}")
    
    return data

def load_data(file_path: str) -> pd.DataFrame:
    """
    Load dataset metadata from file.
    
    Args:
        file_path: Path to metadata file (CSV or Parquet)
        
    Returns:
        DataFrame with dataset metadata
    """
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.parquet'):
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path}")

def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare and clean metadata for quality assessment.
    
    Args:
        df: DataFrame with raw metadata
        
    Returns:
        DataFrame with cleaned metadata
    """
    import ast
    
    # Convert string representations of JSON to Python objects
    for col in ['resources', 'tags', 'groups', 'extras']:
        if col in df.columns:
            # Try both json.loads and ast.literal_eval for robustness
            df[col] = df[col].apply(
                lambda x: _safe_parse_json(x) if isinstance(x, str) else x
            )
    
    # Ensure all expected columns exist
    for col in ['organization']:
        if col not in df.columns:
            df[col] = None
            
    return df

def _safe_parse_json(json_string: str) -> Any:
    """
    Safely parse a JSON string using multiple methods.
    
    Args:
        json_string: String containing JSON or Python literal
        
    Returns:
        Parsed Python object
    """
    if not isinstance(json_string, str) or not json_string.strip():
        return json_string
        
    # Try json.loads first
    try:
        return json.loads(json_string)
    except:
        # Fall back to ast.literal_eval
        try:
            import ast
            return ast.literal_eval(json_string)
        except:
            # Return original if all parsing fails
            return json_string

def main():
    """Run the metadata quality assessment process."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Berlin Open Data Metadata Quality Assessment'
    )
    
    # Data source group (mutually exclusive)
    data_source = parser.add_mutually_exclusive_group()
    data_source.add_argument(
        '--fetch', '-f', action='store_true',
        help='Fetch metadata from Berlin Open Data API'
    )
    data_source.add_argument(
        '--input', '-i', 
        help='Path to input metadata file (CSV or Parquet)'
    )
    
    # Other options
    parser.add_argument(
        '--data-dir', '-d', default=DEFAULT_DATA_DIR,
        help=f'Directory to save/load raw data (default: {DEFAULT_DATA_DIR})'
    )
    parser.add_argument(
        '--results-dir', '-r', default=DEFAULT_RESULTS_DIR,
        help=f'Directory to save results (default: {DEFAULT_RESULTS_DIR})'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Show detailed progress for each dataset'
    )
    parser.add_argument(
        '--sample', '-s', type=int, default=0,
        help='Process only a sample of datasets (0 for all)'
    )
    
    args = parser.parse_args()
    
    # Create necessary directories
    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.results_dir, exist_ok=True)
    
    # Determine data source and load data
    if args.fetch:
        # Fetch from API
        timestamp = datetime.now().strftime("%Y%m%d")
        raw_data_path = os.path.join(args.data_dir, f"berlin_metadata_{timestamp}.csv")
        df = fetch_metadata(output_file=raw_data_path)
        
    elif args.input:
        # Load from specified file
        print(f"Loading data from {args.input}...")
        df = load_data(args.input)
        
    else:
        # Try to find existing data files
        data_files = [f for f in os.listdir(args.data_dir) 
                     if f.endswith('.csv') or f.endswith('.parquet')]
        
        if data_files:
            # Use the latest file by name or modification time
            latest_file = sorted(data_files)[-1]
            input_path = os.path.join(args.data_dir, latest_file)
            print(f"No input specified, using most recent file: {input_path}")
            df = load_data(input_path)
        else:
            # Fetch new data if no existing files
            print("No existing data files found, fetching from API...")
            timestamp = datetime.now().strftime("%Y%m%d")
            raw_data_path = os.path.join(args.data_dir, f"berlin_metadata_{timestamp}.csv")
            df = fetch_metadata(output_file=raw_data_path)
    
    # Display basic info
    print(f"Loaded {len(df)} datasets.")
    
    # Prepare data for processing
    df = prepare_data(df)
    print("Data preparation complete.")
    
    # Optionally limit to a sample
    if args.sample > 0 and args.sample < len(df):
        print(f"Processing a sample of {args.sample} datasets")
        df = df.sample(args.sample)
    
    # Convert DataFrame to list of dictionaries for processing
    datasets = df.to_dict('records')
    
    if args.verbose:
        # Process with detailed logging
        results = []
        total_datasets = len(datasets)
        
        # Counters for statistics
        successful = 0
        errors = 0
        ratings_count = {"Ausgezeichnet": 0, "Gut": 0, "Ausreichend": 0, "Mangelhaft": 0}
        
        # Process each dataset with detailed logging
        for i, dataset in enumerate(tqdm(datasets, desc="Processing datasets")):
            # Print progress periodically or for the first few
            if i < 10 or i % 100 == 0 or i == total_datasets - 1:
                print(f"Processing dataset {i+1}/{total_datasets}: {dataset.get('title', 'Unknown')}")
            
            try:
                # Log details for the first few datasets
                if i < 5:
                    print(f"  Checking dimensions for: {dataset.get('id', 'unknown')}")
                    print(f"  Resources: {len(dataset.get('resources', []))} found")
                    print(f"  Tags: {len(dataset.get('tags', []))} found")
                
                # Calculate the MQA score
                result = calculate_mqa_score(dataset)
                successful += 1
                
                # Update ratings counter
                if result['rating'] in ratings_count:
                    ratings_count[result['rating']] += 1
                
                # Create a results dictionary
                result_summary = {
                    'id': dataset.get('id', 'unknown'),
                    'title': dataset.get('title', 'Unknown'),
                    'organization': dataset.get('organization', {}).get('title', 'Unknown') 
                                  if isinstance(dataset.get('organization'), dict) else 'Unknown',
                    'total_score': result['total_score'],
                    'rating': result['rating'],
                }
                
                # Add dimension scores
                for dimension, score in result['dimension_scores'].items():
                    result_summary[f"{dimension}_score"] = score
                    
                results.append(result_summary)
                
                # Print detailed results for the first few datasets
                if i < 3:
                    print(f"  Total Score: {result['total_score']}")
                    print(f"  Rating: {result['rating']}")
                    print("  Dimension Scores:")
                    for dim, score in result['dimension_scores'].items():
                        print(f"    {dim}: {score}")
                    
                    # Print sample indicator results
                    print("  Sample Indicators:")
                    for dimension, indicators in result['detailed_results'].items():
                        for ind in indicators[:2]:  # Print first 2 indicators per dimension
                            print(f"    {dimension} - {ind['indicator']}: {ind['points']}/{ind['max_points']} (Passed: {ind['passed']})")
                    print()
                    
                # Print progress summary periodically
                if (i + 1) % 500 == 0:
                    print(f"\nProgress Update ({i+1}/{total_datasets}):")
                    print(f"  Successful: {successful}")
                    print(f"  Errors: {errors}")
                    print(f"  Ratings so far: {ratings_count}")
                    print()
                    
            except Exception as e:
                errors += 1
                print(f"  Error processing dataset {dataset.get('id', 'unknown')}: {e}")
                
                # Print details about the problematic dataset
                print(f"  Problem dataset info:")
                for key in ['id', 'title', 'resources', 'tags']:
                    print(f"    {key}: {dataset.get(key, 'Not found')}")
        
        # Convert results to DataFrame
        results_df = pd.DataFrame(results)
        
    else:
        # Use standard processing function
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(args.results_dir, f"mqa_scores_{timestamp}.csv")
        print(f"Calculating metadata quality scores for {len(datasets)} datasets...")
        
        # Process datasets
        results_df = process_datasets(datasets, output_file=output_path)
    
    # Save results with timestamp and generic name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Define output paths
    if args.sample > 0 and args.sample < 20:
        # Sample run
        output_path = os.path.join(args.results_dir, f"mqa_sample_scores_{timestamp}.csv")
        generic_output_path = os.path.join(args.results_dir, "mqa_sample_scores.csv")
    else:
        # Full run
        output_path = os.path.join(args.results_dir, f"mqa_scores_{timestamp}.csv")
        generic_output_path = os.path.join(args.results_dir, "mqa_scores.csv")
    
    # Save to both timestamped and generic files
    results_df.to_csv(output_path, index=False)
    results_df.to_csv(generic_output_path, index=False)
    
    # Also save ratings summary
    ratings_summary = pd.DataFrame(results_df['rating'].value_counts())
    ratings_summary.index.name = 'rating'
    ratings_summary.to_csv(os.path.join(args.results_dir, "ratings_summary.csv"))
    
    # Save detailed results for the first dataset as JSON
    if len(datasets) > 0:
        first_result = calculate_mqa_score(datasets[0])
        with open(os.path.join(args.results_dir, "detailed_first_dataset.json"), "w") as f:
            json.dump(first_result, f, indent=2)
    
    # Print summary
    print("\nMetadata Quality Assessment Results:")
    print(f"Average score: {results_df['total_score'].mean():.2f}")
    print("\nRating distribution:")
    print(results_df['rating'].value_counts())
    
    print(f"\nResults saved to {args.results_dir}")

if __name__ == "__main__":
    main()