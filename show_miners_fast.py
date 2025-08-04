#!/usr/bin/env python3
"""
Display miner statistics with limited data processing for faster results
"""
import asyncio
import os
import sys
import argparse
from collections import defaultdict
from tabulate import tabulate
from affine import miners, dataset, ENVS, get_subtensor, NETUID, get_client_ctx, get_conf
import json
from pathlib import Path


async def get_miner_stats_fast(target_uid=None):
    """Fetch miner statistics with limited historical data for speed"""
    print("Fetching miner data...")
    
    # Get current miners
    miner_map = await miners()
    if not miner_map:
        print("No miners found")
        return
    
    # If specific UID requested, check if it exists
    if target_uid is not None:
        if target_uid not in miner_map:
            print(f"UID {target_uid} not found in active miners")
            print(f"Available UIDs: {sorted(miner_map.keys())}")
            return
        print(f"Fetching detailed data for UID {target_uid}...")
    
    # Initialize score tracking
    ALPHA = 0.9  # Same as validator
    scores = {uid: defaultdict(float) for uid in miner_map.keys()}
    prev = {}
    
    print("Fetching recent results (limited to last 1000 blocks for speed)...")
    
    # Get current block to limit our search
    sub = await get_subtensor()
    current_block = await sub.get_current_block()
    min_block = current_block - 1000  # Only last 1000 blocks
    
    # Directly fetch recent files from R2
    bucket = get_conf("R2_BUCKET_ID")
    prefix = "affine/results/"
    
    processed_count = 0
    file_count = 0
    
    async with get_client_ctx() as client:
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        async for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = Path(key).name
                
                if not name.endswith(".json"):
                    continue
                    
                # Extract block number
                base = name[:-5]  # strip ".json"
                parts = base.split("-", 1)
                if len(parts) != 2 or not parts[0].isdigit():
                    continue
                    
                block = int(parts[0])
                if block < min_block:
                    continue
                
                file_count += 1
                if file_count > 20:  # Limit to 20 most recent files
                    break
                
                print(f"Processing file {file_count}/20: {name}")
                
                # Fetch and process this file
                try:
                    resp = await client.get_object(Bucket=bucket, Key=key)
                    raw = await resp["Body"].read()
                    data = json.loads(raw)
                    
                    for item in data:
                        try:
                            result = {
                                "miner": item["miner"],
                                "challenge": item["challenge"],
                                "response": item["response"],
                                "evaluation": item["evaluation"]
                            }
                            
                            uid = result["miner"]["uid"]
                            if uid not in miner_map:
                                continue
                            
                            model = result["miner"].get("model", "")
                            if not model or model.split('/')[1].lower()[:6] != 'affine':
                                continue
                            
                            env = result["challenge"]["env"]
                            score = result["evaluation"]["score"]
                            success = result["response"]["success"]
                            
                            # Update score
                            if success:
                                scores[uid][env] = score * (1 - ALPHA) + scores[uid][env] * ALPHA
                                processed_count += 1
                                
                                
                        except Exception:
                            continue
                            
                except Exception as e:
                    print(f"Error processing {key}: {e}")
                    continue
            
            if file_count > 20:
                break
    
    print(f"\nProcessed {processed_count} results from {file_count} files")
    
    # Filter for specific UID if requested
    if target_uid is not None:
        if target_uid not in miner_map:
            print(f"UID {target_uid} not found")
            return
        miner_map = {target_uid: miner_map[target_uid]}
    
    # Calculate ranks for each environment (use all miners for ranking, not just filtered ones)
    all_miners = await miners()  # Get all miners again for proper ranking
    ranks = {}
    for env in ENVS:
        # Get all miners with scores for this environment for proper ranking
        env_miners = [(uid, scores[uid][env]) for uid in all_miners.keys() if scores[uid][env] > 0]
        env_miners.sort(key=lambda x: x[1], reverse=True)
        
        # Assign ranks
        for rank, (uid, _) in enumerate(env_miners, 1):
            if env not in ranks:
                ranks[env] = {}
            ranks[env][uid] = rank
    
    # Build table data - reorder to ABD, SAT, DED
    env_order = ["ABD", "SAT", "DED"]
    headers = ["UID", "Model"] + [f"{e} Score" for e in env_order] + [f"{e} Rank" for e in env_order]
    rows = []
    
    for uid, miner in sorted(miner_map.items()):
        if not miner.model:
            continue
        
        # Check if we have any scores for this miner
        has_scores = any(scores[uid][env] > 0 for env in env_order)
        if not has_scores:
            continue
            
        row = [
            uid,
            miner.model[:50]  # Longer model name display
        ]
        
        # Add scores in ABD, SAT, DED order
        for env in env_order:
            score = scores[uid][env]
            row.append(f"{score:.4f}" if score > 0 else "-")
        
        # Add ranks in ABD, SAT, DED order
        for env in env_order:
            rank = ranks.get(env, {}).get(uid, "-")
            row.append(str(rank) if rank != "-" else "-")
            
        rows.append(row)
    
    # Sort by average rank (lower is better)
    def avg_rank(row):
        rank_values = []
        for i in range(len(env_order)):
            rank_str = row[2 + len(env_order) + i]  # Start after UID, Model, and scores
            if rank_str != "-":
                rank_values.append(int(rank_str))
        return sum(rank_values) / len(rank_values) if rank_values else float('inf')
    
    rows.sort(key=avg_rank)
    
    # Display table
    print("\n" + "="*100)
    print("MINER STATISTICS (Recent Data)")
    print("="*100)
    print(tabulate(rows, headers=headers, tablefmt="grid"))
    print(f"\nTotal active miners with scores: {len(rows)}")
    print(f"Environments: {', '.join(env_order)}")
    print(f"Data source: Last 1000 blocks (~{file_count} files)")

async def main():
    parser = argparse.ArgumentParser(description="Show miner statistics")
    parser.add_argument("--uid", type=int, help="Show detailed info for specific UID")
    args = parser.parse_args()
    
    try:
        await get_miner_stats_fast(target_uid=args.uid)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Ensure required environment variables are set
    required_vars = ["SUBTENSOR_ENDPOINT", "R2_ACCOUNT_ID", "R2_WRITE_ACCESS_KEY_ID", 
                     "R2_WRITE_SECRET_ACCESS_KEY", "R2_BUCKET_ID"]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        print("Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print("\nPlease set these in your .env file or environment")
        exit(1)
    
    asyncio.run(main())
