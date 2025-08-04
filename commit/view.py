import asyncio
import json
import bittensor as bt

uid = 143

async def view_commit(uid):
    sub = bt.async_subtensor(network="finney")
    await sub.initialize()
    meta = await sub.metagraph(netuid=120)
    hotkey = meta.hotkeys[uid]
    commits = await sub.get_all_revealed_commitments(120)
    if hotkey in commits:
        for block, data in commits[hotkey]:
            print(f"Block {block}: {data}")
            try:
                print(json.loads(data))
            except: pass
    else:
        print("No commit found")

asyncio.run(view_commit(uid))
