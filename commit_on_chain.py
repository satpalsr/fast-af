# This script allows you to commit a model deployment to the Affine subnet on Bittensor.
# 
# Usage example:
# python commit_to_chain.py --repo-name "Alphatao/Affine-0000000" --revision "2a61cda260894e6f809f7e6164135f6d4ac6b175" --chute-id "f68a823e-b848-55c2-a2a0-02b8229c43fe" --coldkey vroom --hotkey af1
#
# Required arguments:
#   --repo-name:   The HuggingFace repo name of your model (e.g., "Alphatao/Affine-0000000")
#   --revision:    The git commit hash or revision string of your model
#   --chute-id:    The unique Chutes deployment ID for your model
#   --coldkey:     Your Bittensor coldkey name (must be available locally)
#   --hotkey:      Your Bittensor hotkey name (must be available locally)
#
# Make sure your wallet is unlocked and you have the necessary permissions to submit to the Affine subnet.


import json
import asyncio
import os
from typing import Optional
import click
import bittensor as bt
from substrateinterface.exceptions import SubstrateRequestException
from bittensor.core.errors import MetadataError

# Constants from affine
NETUID = 120  # Affine subnet

async def get_subtensor(network: str = None):
    """Get subtensor instance."""
    network = network or os.getenv("BT_SUBTENSOR_NETWORK", "finney")
    subtensor = bt.async_subtensor(network=network)
    await subtensor.initialize()
    return subtensor

async def commit_to_chain(
    wallet: bt.wallet,
    repo_name: str,
    revision: str,
    chute_id: Optional[str] = None,
    network: str = None
):
    """Submit the model commitment to chain, retrying on quota errors."""
    if not chute_id:
        raise ValueError("chute_id must be provided.")
    print(f"Preparing on-chain commitment for {repo_name} @ {revision}")
    
    sub = await get_subtensor(network)
    payload = json.dumps({
        "model": repo_name,
        "revision": revision,
        "chute_id": chute_id
    })
    
    print(f"Commitment payload: {payload}")
    
    while True:
        try:
            print("Submitting on-chain commitment...")
            await sub.set_reveal_commitment(
                wallet=wallet,
                netuid=NETUID,
                data=payload,
                blocks_until_reveal=1
            )
            print("✅ On-chain commitment submitted successfully!")
            break
        except MetadataError as e:
            if "SpaceLimitExceeded" in str(e):
                print("SpaceLimitExceeded – waiting one block before retrying...")
                await sub.wait_for_block()
            else:
                raise
        except SubstrateRequestException as e:
            print(f"❌ Substrate error: {e}")
            raise
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            raise

@click.command()
@click.option('--repo-name', required=True, help='HuggingFace repo name (e.g., username/model-name)')
@click.option('--revision', required=True, help='Git revision/commit SHA of the model')
@click.option('--chute-id', default=None, help='Chute ID (required)')
@click.option('--coldkey', default='default', help='Name of the cold wallet to use')
@click.option('--hotkey', default='default', help='Name of the hot wallet to use')
@click.option('--network', default=None, help='Bittensor network (finney/test/local)')
def main(repo_name: str, revision: str, chute_id: str, coldkey: str, hotkey: str, network: str):
    """Commit a model to Bittensor chain (step 2 of af push)."""

    if not chute_id:
        raise ValueError("chute_id must be provided via --chute-id.")

    # Load wallet
    print(f"Loading wallet: coldkey={coldkey}, hotkey={hotkey}")
    wallet = bt.wallet(name=coldkey, hotkey=hotkey)
    
    # Verify wallet is registered
    if not wallet.coldkey or not wallet.hotkey:
        print("❌ Wallet not found. Make sure your coldkey and hotkey exist.")
        return
    
    print(f"Wallet loaded: {wallet.hotkey.ss58_address}")
    
    # Run the async commit
    try:
        asyncio.run(commit_to_chain(
            wallet=wallet,
            repo_name=repo_name,
            revision=revision,
            chute_id=chute_id,
            network=network
        ))
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
    except Exception as e:
        print(f"❌ Failed to commit: {e}")
        raise

if __name__ == "__main__":
    main()
