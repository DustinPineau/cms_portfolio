import asyncio
import httpx

async def fetch(client, npi):
    r = await client.get(f'https://npiregistry.cms.hhs.gov/api/?number={npi}&version=2.1')
    return r.status_code

async def main():
    npis = [f'100300{str(i).zfill(4)}' for i in range(1, 501)]
    async with httpx.AsyncClient() as client:
        tasks = [fetch(client, npi) for npi in npis]
        results = await asyncio.gather(*tasks)
    for code in set(results):
        print(f'{code}: {results.count(code)} responses')

asyncio.run(main())

