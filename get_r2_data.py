"""
Download and process data from R2 storage.

Usage:
  python get_r2data.py                          # Download and normalize data
  python get_r2data.py --filter                 # Download, normalize, and filter data (score==1 by default)
  python get_r2data.py --filter                 # If normalized data exists, only filter it
  python get_r2data.py --filter --score 0       # Filter for score==0
  python get_r2data.py --filter --score both    # Include all scores (no score filtering)
  python get_r2data.py --filter --uid 123      # Filter for specific UID
  python get_r2data.py --filter --model "gpt4" # Filter for specific model
  python get_r2data.py --filter --uid 123 --model "gpt4" --score 0 # Combine multiple filters

The script:
1. Downloads JSON files from R2 storage
2. Normalizes nested fields to JSON strings for consistent schema
3. Optionally filters data by score (0, 1, or both), UID, and/or model name
"""

import affine as af
import os
import asyncio
import json
from dotenv import load_dotenv
from botocore.config import Config
from aiobotocore.session import get_session
from pathlib import Path
from asyncio import Semaphore
from tqdm.asyncio import tqdm as async_tqdm
from tqdm import tqdm
import time
import argparse

# Load environment variables from .env file
load_dotenv()

# Limit concurrent downloads to avoid overwhelming the server
MAX_CONCURRENT_DOWNLOADS = 1000

# Guaranteed keys that must exist in a valid result
REQUIRED_KEYS = ['version', 'miner', 'challenge', 'response', 'evaluation']

def normalize_result(item):
    """
    Normalize a result item to have consistent schema by converting 
    variable-structure fields to JSON strings
    """
    # Create a deep copy to avoid modifying original
    normalized = json.loads(json.dumps(item))
    
    # Convert challenge.extra to JSON string if it exists
    if 'challenge' in normalized and 'extra' in normalized['challenge']:
        normalized['challenge']['extra_json'] = json.dumps(normalized['challenge']['extra'])
        del normalized['challenge']['extra']
    
    # Convert evaluation.extra to JSON string if it exists and is not empty
    if ('evaluation' in normalized and 'extra' in normalized['evaluation'] and 
        normalized['evaluation']['extra'] and normalized['evaluation']['extra'] != {}):
        normalized['evaluation']['extra_json'] = json.dumps(normalized['evaluation']['extra'])
        del normalized['evaluation']['extra']
    elif 'evaluation' in normalized and 'extra' in normalized['evaluation']:
        # Remove empty extra field without creating extra_json
        del normalized['evaluation']['extra']
    
    # Convert miner.chute to JSON string if it exists
    if 'miner' in normalized and 'chute' in normalized['miner'] and normalized['miner']['chute'] is not None:
        normalized['miner']['chute_json'] = json.dumps(normalized['miner']['chute'])
        del normalized['miner']['chute']
    
    return normalized

def is_valid_result(json_data):
    """Check if the JSON data contains a valid result with required keys"""
    if isinstance(json_data, list):
        # Check if at least one item in the list has all required keys
        for item in json_data:
            if isinstance(item, dict) and all(key in item for key in REQUIRED_KEYS):
                return True
        return False
    elif isinstance(json_data, dict):
        # Check if the single object has all required keys
        return all(key in json_data for key in REQUIRED_KEYS)
    return False

async def process_file(client, key, semaphore):
    """Process a single file with semaphore for concurrency control"""
    async with semaphore:
        try:
            response = await client.get_object(Bucket=os.getenv("R2_BUCKET_ID"), Key=key)
            body = response['Body']
            content = await body.read()
            
            # Check if content is empty or just contains []
            content_str = content.decode('utf-8').strip()
            if content_str == '[]' or content_str == '':
                return None, f"Empty file: {key}"
            
            # Parse JSON content
            try:
                json_data = json.loads(content_str)
                
                # Check if the result contains required keys
                if not is_valid_result(json_data):
                    return None, f"Invalid result (missing keys): {key}"
                
                # Collect normalized items
                normalized_items = []
                if isinstance(json_data, list):
                    for item in json_data:
                        if isinstance(item, dict) and all(key in item for key in REQUIRED_KEYS):
                            # Validate score - must be exactly 0 or 1
                            score = item.get('evaluation', {}).get('score')
                            # Convert score to int if it's a valid numeric value
                            if isinstance(score, (int, float)) and score in [0, 1]:
                                score = int(score)
                                item['evaluation']['score'] = score
                            else:
                                continue  # Skip items with invalid score values
                            normalized_items.append(normalize_result(item))
                else:
                    # If it's a single object
                    # Validate score - must be exactly 0 or 1
                    score = json_data.get('evaluation', {}).get('score')
                    # Convert score to int if it's a valid numeric value
                    if isinstance(score, (int, float)) and score in [0, 1]:
                        score = int(score)
                        json_data['evaluation']['score'] = score
                    else:
                        return None, f"Invalid score value ({score}) in {key}"
                    normalized_items.append(normalize_result(json_data))
                
                return normalized_items, None
                
            except json.JSONDecodeError as e:
                return None, f"JSON decode error in {key}: {e}"
                
        except Exception as e:
            return None, f"Error processing {key}: {e}"

async def write_batch(output_file, batch, file_lock):
    """Write a batch of items to file"""
    async with file_lock:
        with open(output_file, 'a', encoding='utf-8') as f:
            for item in batch:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')

async def main():
    start_time = time.time()
    
    # Create output JSONL file
    output_file = Path("combined_data.jsonl")
    
    # Check if file exists and prompt user
    if output_file.exists():
        response = input(f"\n{output_file} already exists. Do you want to overwrite it? (y/N): ")
        if response.lower() != 'y':
            print("Aborting download. Use --filter to filter existing data.")
            return
        else:
            output_file.unlink()
            print(f"Overwriting {output_file}...")
    
    get_client_ctx = lambda: get_session().create_client(
        "s3",
        endpoint_url=f"https://{os.getenv('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        aws_access_key_id=os.getenv("R2_WRITE_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_WRITE_SECRET_ACCESS_KEY"),
        region_name="auto",
        config=Config(max_pool_connections=256)
    )
    
    # Create semaphore to limit concurrent downloads
    semaphore = Semaphore(MAX_CONCURRENT_DOWNLOADS)
    
    # Create a single lock for file writing
    file_lock = asyncio.Lock()
    
    print("Fetching file list from R2...")
    async with get_client_ctx() as client:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=os.getenv("R2_BUCKET_ID"), Prefix="affine/results/")
        
        # Collect all JSON files first
        json_files = []
        async for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('.json'):
                        json_files.append(key)
        
        print(f"Found {len(json_files)} JSON files to process")
        
        # Process files in batches for better performance
        batch_size = 100
        write_batch_size = 1000
        write_buffer = []
        processed_count = 0
        error_count = 0
        errors = []
        
        # Process files with progress bar
        for i in async_tqdm(range(0, len(json_files), batch_size), desc="Processing batches"):
            batch_files = json_files[i:i + batch_size]
            
            # Create tasks for this batch
            tasks = [process_file(client, key, semaphore) for key in batch_files]
            
            # Process batch
            results = await asyncio.gather(*tasks)
            
            # Collect results
            for items, error in results:
                if items:
                    write_buffer.extend(items)
                    processed_count += 1
                else:
                    error_count += 1
                    if error:
                        errors.append(error)
            
            # Write buffer when it gets large enough
            if len(write_buffer) >= write_batch_size:
                await write_batch(output_file, write_buffer, file_lock)
                write_buffer = []
        
        # Write remaining items
        if write_buffer:
            await write_batch(output_file, write_buffer, file_lock)
        
        # Print summary
        elapsed_time = time.time() - start_time
        print(f"\n✓ Processing complete in {elapsed_time:.1f} seconds!")
        print(f"✓ Successfully processed: {processed_count} files")
        print(f"✗ Errors/skipped: {error_count} files")
        
        if errors and error_count <= 10:
            print("\nFirst few errors:")
            for error in errors[:5]:
                print(f"  - {error}")
        
        print(f"\nOutput saved to: {output_file}")
        print("\nThe normalized data can now be uploaded to Hugging Face without schema issues.")
        print("Variable-structure fields have been converted to JSON strings:")
        print("  - challenge.extra → challenge.extra_json")
        print("  - evaluation.extra → evaluation.extra_json")
        print("  - miner.chute → miner.chute_json")

def filter_data(input_file, output_file, filter_uid=None, filter_model=None, filter_score='1'):
    """
    Filter the normalized data and extract specific fields. 
    Optionally filter by specific UID, model name, or score.
    """
    # Print filter criteria
    filters = []
    if filter_score != 'both':
        filters.append(f"score == {filter_score}")
    if filter_uid is not None:
        filters.append(f"uid == {filter_uid}")
    if filter_model is not None:
        filters.append(f"model == '{filter_model}'")
    
    if filters:
        print(f"Filtering with criteria: {' AND '.join(filters)}")
    else:
        print("Extracting all data (no score filtering)")
    
    # First, count the number of lines for progress bar
    with open(input_file, "r", encoding="utf-8") as fin:
        total_lines = sum(1 for _ in fin)
    
    filtered_count = 0
    with open(input_file, "r", encoding="utf-8") as fin, open(output_file, "w", encoding="utf-8") as fout:
        for line in tqdm(fin, total=total_lines, desc="Filtering"):
            try:
                row = json.loads(line)
                evaluation = row.get("evaluation", {})
                challenge = row.get("challenge", {})
                response = row.get("response", {})
                # Extract required fields
                env = challenge.get("env")
                prompt = challenge.get("prompt")
                resp = response.get("response")
                miner = row.get("miner", {})
                model = miner.get("model")
                uid = miner.get("uid")
                
                # Get score and extra_json if available
                score = evaluation.get("score")
                evaluation_extra_json = evaluation.get("extra_json")
                
                # Validate score - must be exactly 0 or 1
                # Convert score to int if it's a valid numeric value
                if isinstance(score, (int, float)) and score in [0, 1]:
                    score = int(score)
                else:
                    continue  # Skip records with invalid score values
                
                # Apply filters
                score_match = (filter_score == 'both' or 
                             (filter_score == '0' and score == 0) or 
                             (filter_score == '1' and score == 1))
                
                # Filter for required fields and matching criteria
                if (
                    env is not None and
                    prompt is not None and
                    resp is not None and
                    model is not None and
                    score_match and
                    (filter_uid is None or uid == filter_uid) and
                    (filter_model is None or model == filter_model)
                ):
                    filtered = {
                        "uid": uid,
                        "env": env,
                        "prompt": prompt,
                        "response": resp,
                        "model": model,
                        "score": score,
                        "evaluation_extra_json": evaluation_extra_json
                    }
                    fout.write(json.dumps(filtered, ensure_ascii=False) + "\n")
                    filtered_count += 1
            except Exception as e:
                # Skip malformed lines
                continue
    
    print(f"\nNumber of rows in filtered dataset: {filtered_count}")
    print(f"Filtered data saved to: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and process R2 data")
    parser.add_argument("--filter", action="store_true", help="Filter data and save to filtered_data.jsonl")
    parser.add_argument("--uid", type=int, help="Filter by specific UID number")
    parser.add_argument("--model", type=str, help="Filter by specific model name")
    parser.add_argument("--score", type=str, choices=['0', '1', 'both'], default='1', 
                        help="Filter by score: 0, 1, or both (default: 1)")
    args = parser.parse_args()
    
    output_file = Path("combined_data.jsonl")
    
    # If filter flag is set and normalized data already exists, skip download
    if args.filter and output_file.exists():
        print(f"Found existing {output_file}, skipping download...")
        print("Running filtering only...")
        filter_data(str(output_file), "filtered_data.jsonl", 
                   filter_uid=args.uid, filter_model=args.model, filter_score=args.score)
    else:
        # Run main download/processing
        asyncio.run(main())
        
        # If filter flag is set, run the filtering after download
        if args.filter:
            print("\nRunning filtering...")
            filter_data(str(output_file), "filtered_data.jsonl", 
                       filter_uid=args.uid, filter_model=args.model, filter_score=args.score)