#!/usr/bin/env python3

import asyncio
import json
from tqdm.asyncio import tqdm
from affine.envs.sat import SAT

async def generate_sat_problems(num_problems=10000):
    """Generate SAT problems and save to JSONL file."""
    sat_env = SAT(n=10, k=3, m=42)  # 3-SAT with 10 variables, ~42 clauses
    
    problems = []
    
    print(f"Generating {num_problems} SAT problems...")
    
    for i in tqdm(range(num_problems), desc="Generating problems"):
        challenge = await sat_env.generate()
        
        problem_data = {
            "id": i + 1,
            "prompt": challenge.prompt,
            "solution": challenge.extra["sol"],
            "clauses": challenge.extra["cls"],
            "n_variables": sat_env.n,
            "k_sat": sat_env.k,
            "n_clauses": sat_env.m
        }
        
        problems.append(problem_data)
    
    # Save to JSONL file
    output_file = "sat_problems_10k.jsonl"
    with open(output_file, 'w') as f:
        for problem in problems:
            f.write(json.dumps(problem, ensure_ascii=False) + '\n')
    
    print(f"Successfully saved {num_problems} SAT problems to {output_file}")

if __name__ == "__main__":
    asyncio.run(generate_sat_problems(10000))
