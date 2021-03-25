import asyncio

import aiohttp

from recorders import TuneinStationRecorder


async def main():
    async with aiohttp.ClientSession() as session:
        await TuneinStationRecorder('s297990', session).record()

if __name__ == '__main__':
    asyncio.run(main())
