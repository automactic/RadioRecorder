import asyncio

import aiohttp

from  recorder import Recorder


async def main():
    async with aiohttp.ClientSession() as session:
        headers = {'id': 's297990', 'formats': 'hls'}
        async with session.get('http://opml.radiotime.com/Tune.ashx', params=headers) as response:
            url = await response.text()
            await Recorder(url, session).record()

if __name__ == '__main__':
    asyncio.run(main())
