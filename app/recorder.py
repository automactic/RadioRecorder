import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

import aiofiles
import aiohttp
import dateutil.tz
import dateutil.parser
from dateutil.parser import parse
from logging import getLogger

logger = getLogger(__name__)


@dataclass
class Chunk:
    timestamp: datetime
    url: Optional[str] = None


@dataclass
class StreamContent:
    available_duration: int
    chunks: List[Chunk]


class Recorder:
    def __init__(self, url, session: aiohttp.ClientSession):
        self.url = url
        self.session = session
        self.queue = asyncio.Queue()

    async def get_stream_url(self) -> str:
        async with self.session.get(self.url) as response:
            content = await response.text()
            return content.split()[-1]

    async def record(self):
        asyncio.create_task(self.chunk_processor())

        stream_url = await self.get_stream_url()
        buffer = []
        while True:
            # parse all the chunks
            async with self.session.get(stream_url) as response:
                content = await response.text()
                chunk = None
                for line in content.split():
                    if line.startswith('#EXT-X-PROGRAM-DATE-TIME'):
                        chunk = Chunk(timestamp=parse(line.replace('#EXT-X-PROGRAM-DATE-TIME:', '')))
                    if line.startswith('http'):
                        chunk.url = line
                        if not buffer or (buffer and chunk.timestamp > buffer[-1].timestamp):
                            buffer.append(chunk)

            # add all but last buffered chunks to queue
            for chuck in buffer[:-1]:
                self.queue.put_nowait(chuck)
            buffer = [buffer[-1]]

            # wait for next loop
            await asyncio.sleep(180)

    @staticmethod
    async def parse_stream(raw_content: str) -> StreamContent:
        """Parse content of a M3U stream response

        :param raw_content: the raw stream content
        :return: the parsed stream content
        """

        available_duration = 60
        chunks = []
        current_timestamp = None
        for line in raw_content.split():
            if line.startswith('#EXT-X-COM-TUNEIN-AVAIL-DUR:'):
                try:
                    available_duration = int(line.replace('#EXT-X-COM-TUNEIN-AVAIL-DUR:', ''))
                except ValueError:
                    pass
            elif line.startswith('#EXT-X-PROGRAM-DATE-TIME:'):
                current_timestamp = dateutil.parser.parse(line.replace('#EXT-X-PROGRAM-DATE-TIME:', ''))
            elif line.startswith('http'):
                chunk = Chunk(timestamp=current_timestamp, url=line)
                chunks.append(chunk)
                current_timestamp = None
        return StreamContent(available_duration, chunks)

    async def chunk_processor(self):
        """Process chunks in the stream."""

        while True:
            # get the next chunk
            chunk = await self.queue.get()

            # figure out the filename
            timestamp = chunk.timestamp.replace(minute=0, second=0, microsecond=0).astimezone(dateutil.tz.tzlocal())
            filename = f'{timestamp.strftime("%Y-%m-%d-%H-%M")}.ts'

            # append to file
            print(f'processing: {chunk.timestamp}')
            async with aiofiles.open(filename, 'ab') as file:
                async with self.session.get(chunk.url) as response:
                    content = await response.read()
                    await file.write(content)

            # let the queue know the task is done
            self.queue.task_done()
