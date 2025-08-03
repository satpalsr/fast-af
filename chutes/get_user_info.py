#!/usr/bin/env python3
"""
Script to get user info from chutes API using the authentication system
"""
import os
import sys
import time
import json
import asyncio
import aiohttp
from substrateinterface import Keypair
from pathlib import Path

# Configuration
USERNAME = ""
HOTKEY_SS58 = ""
HOTKEY_SEED = ""
API_BASE_URL = "https://api.chutes.ai"

# Add chutes to Python path
sys.path.insert(0, str(Path(__file__).parent / "chutes"))

from chutes.util.auth import get_signing_message
from chutes.constants import HOTKEY_HEADER, NONCE_HEADER, SIGNATURE_HEADER

async def get_user_info():
    """Try to get user info using various endpoints"""
    
    # Create keypair
    keypair = Keypair.create_from_seed(seed_hex=HOTKEY_SEED)
    
    # First, let's try to list chutes - this should work and might reveal user_id in response
    print(f"Attempting to get user info for username: {USERNAME}")
    
    async with aiohttp.ClientSession(base_url=API_BASE_URL) as session:
        # Try listing chutes (this uses purpose-based auth)
        nonce = str(int(time.time()))
        headers = {
            HOTKEY_HEADER: HOTKEY_SS58,
            NONCE_HEADER: nonce,
        }
        
        # Sign with purpose
        sig_str = get_signing_message(HOTKEY_SS58, nonce, payload_str=None, purpose="chutes")
        signature = keypair.sign(sig_str.encode()).hex()
        headers[SIGNATURE_HEADER] = signature
        
        print("\n1. Trying to list chutes to find user_id...")
        async with session.get("/chutes/", headers=headers, params={"limit": "1"}) as resp:
            if resp.status == 200:
                data = await resp.json()
                print(f"Success! Response headers: {dict(resp.headers)}")
                
                # Check if user_id is in response headers
                for header, value in resp.headers.items():
                    if 'user' in header.lower() and 'id' in header.lower():
                        print(f"Found user ID in header {header}: {value}")
                
                # Pretty print the response to look for user_id
                print("\nResponse data:")
                print(json.dumps(data, indent=2))
            else:
                print(f"Failed: {resp.status} - {await resp.text()}")
        
        # Try a different endpoint - images
        print("\n2. Trying to list images...")
        nonce = str(int(time.time()))
        headers[NONCE_HEADER] = nonce
        sig_str = get_signing_message(HOTKEY_SS58, nonce, payload_str=None, purpose="images")
        headers[SIGNATURE_HEADER] = keypair.sign(sig_str.encode()).hex()
        
        async with session.get("/images/", headers=headers, params={"limit": "1"}) as resp:
            if resp.status == 200:
                data = await resp.json()
                print("Success!")
                print(json.dumps(data, indent=2))
            else:
                print(f"Failed: {resp.status}")
        
        # Try API keys endpoint
        print("\n3. Trying to list API keys...")
        nonce = str(int(time.time()))
        headers[NONCE_HEADER] = nonce
        sig_str = get_signing_message(HOTKEY_SS58, nonce, payload_str=None, purpose="api_keys")
        headers[SIGNATURE_HEADER] = keypair.sign(sig_str.encode()).hex()
        
        async with session.get("/api_keys/", headers=headers, params={"limit": "1"}) as resp:
            if resp.status == 200:
                data = await resp.json()
                print("Success!")
                print(json.dumps(data, indent=2))
            else:
                print(f"Failed: {resp.status}")

if __name__ == "__main__":
    asyncio.run(get_user_info())
