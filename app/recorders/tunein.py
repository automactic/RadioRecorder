import asyncio
import difflib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiofiles
import aiohttp
import dateutil.parser
import dateutil.tz


@dataclass
class Segment:
    timestamp: datetime
    url: Optional[str] = None


class TuneinStationRecorder:
    def __init__(self, station_id: str, session: aiohttp.ClientSession):
        self.station_id = station_id
        self.session = session

        self._stream_url = None
        self.queue = asyncio.Queue()

    async def record(self):
        stream_url = await self._get_stream_url()
        asyncio.create_task(self.grab())

        previous_contents = []
        current_segment = None
        sleep_time = 120
        ad_segment_in_progress = False

        while True:
            # retrieve contents of the stream url
            async with self.session.get(stream_url) as response:
                contents = (await response.text()).splitlines()

            # update sleep_time based on available duration
            if content_duration := self._get_content_duration(contents):
                sleep_time = content_duration * 2 // 3
                print('sleep_time', sleep_time)

            # find new lines
            diff = difflib.ndiff(previous_contents, contents)
            new_lines = [line for line in diff if line.startswith('+')]

            # process new lines
            for new_line in new_lines:
                # strip out ads
                if 'X-TUNEIN-AD-EVENT="START"' in new_line:
                    ad_segment_in_progress = True
                if 'X-TUNEIN-AD-EVENT="END"' in new_line:
                    ad_segment_in_progress = False
                if ad_segment_in_progress:
                    continue

                # parse segment and put into queue
                line = new_line.split(' ')[-1]
                if line.startswith('#EXT-X-PROGRAM-DATE-TIME:'):
                    timestamp = dateutil.parser.parse(line.replace('#EXT-X-PROGRAM-DATE-TIME:', ''))
                    current_segment = Segment(timestamp=timestamp)
                if line.startswith('http') and current_segment:
                    current_segment.url = line
                    self.queue.put_nowait(current_segment)
                    current_segment = None

            previous_contents = contents
            await asyncio.sleep(sleep_time)

    async def grab(self):
        """Grab segment data and save to file."""

        while True:
            # get the next segment
            segment = await self.queue.get()

            # filename to save data
            timezone = dateutil.tz.gettz('America/New_York')
            timestamp = segment.timestamp.replace(minute=0, second=0, microsecond=0).astimezone(timezone)
            filename = f'{timestamp.strftime("%Y-%m-%d-%H-%M")}.ts'

            # append to file
            print(f'grabbing: {segment}')
            working_dir = Path('/data/MSNBC/')
            working_dir.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(working_dir.joinpath(filename), 'ab') as file:
                async with self.session.get(segment.url) as response:
                    content = await response.read()
                    await file.write(content)

            # let the queue know the task is done
            self.queue.task_done()

    async def _get_stream_url(self) -> Optional[str]:
        """Get the first stream url in the master playlist of the station."""

        params = {'id': self.station_id, 'formats': 'hls'}
        async with self.session.get('https://opml.radiotime.com/Tune.ashx', params=params) as response:
            master_playlist = await response.text()
        async with self.session.get(master_playlist) as response:
            content = await response.text()
            for line in content.splitlines():
                if line.startswith('#EXT'):
                    continue
                return line
            return None

    @staticmethod
    def _get_content_duration(lines: [str]) -> Optional[float]:
        for line in lines:
            if line.startswith('#EXT-X-COM-TUNEIN-AVAIL-DUR:'):
                try:
                    return float(line.replace('#EXT-X-COM-TUNEIN-AVAIL-DUR:', ''))
                except ValueError:
                    return None
        return None
