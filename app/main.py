import asyncio
import logging

import aiohttp

from recorders import TuneinStationRecorder

logging.basicConfig(level=logging.INFO)


async def main():
    async with aiohttp.ClientSession() as session:
        await TuneinStationRecorder('s297990', session).record()

if __name__ == '__main__':
    asyncio.run(main())
