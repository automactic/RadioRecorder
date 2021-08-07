import asyncio
import difflib
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiofiles
import aiohttp
import dateutil.parser
import dateutil.tz
import mutagen.easyid3

logger = logging.getLogger(__name__)


@dataclass
class Segment:
    timestamp: datetime
    url: Optional[str] = None


class TuneinStationRecorder:
    DEFAULT_SLEEP_TIME = 120

    def __init__(self, station_id: str, session: aiohttp.ClientSession):
        self.station_id = station_id
        self.session = session

        self._station_name = 'MSNBC'
        self._stream_url = None
        self._segment_queue = asyncio.Queue()
        self._conversion_queue = asyncio.Queue()

    async def record(self):
        self._stream_url = await self._get_stream_url()
        if not self._stream_url:
            logger.error(f'Unable to retrieve streaming url for station {self.station_id}')
            return

        asyncio.create_task(self._grab())
        asyncio.create_task(self._convert())
        await self._stream()

    async def _stream(self):
        """Periodically pull data from stream url, process data into segments, then put segments into queue."""

        previous_contents = []
        current_segment = None
        ad_segment_in_progress = False

        while True:
            # retrieve contents of the stream url
            async with self.session.get(self._stream_url) as response:
                contents = (await response.text()).splitlines()

            # update sleep_time based on available duration
            sleep_time = self._get_sleep_time(contents)

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
                    self._segment_queue.put_nowait(current_segment)
                    current_segment = None

            # finished processing response content, prepare for the next loop
            previous_contents = contents
            await asyncio.sleep(sleep_time)

    async def _grab(self):
        """Grab data for each segment and save to file."""

        previous_path: Optional[Path] = None

        while True:
            # get the next segment form queue
            segment = await self._segment_queue.get()

            # filename to save data
            timezone = dateutil.tz.gettz('America/New_York')
            timestamp = segment.timestamp.replace(minute=0).astimezone(timezone)
            filename = f'{timestamp.strftime("%Y-%m-%d-%H-%M")}.ts'

            # append to file
            logger.debug(f'Grabbing segment: {segment.timestamp}')
            working_dir = Path('/data/MSNBC/')
            working_dir.mkdir(parents=True, exist_ok=True)
            path = working_dir.joinpath(filename)
            async with aiofiles.open(path, 'ab') as file:
                async with self.session.get(segment.url) as response:
                    content = await response.read()
                    await file.write(content)

            # let the queue know the task is done
            self._segment_queue.task_done()

            # if a new file is being created, put the previous file path into conversion queue
            if previous_path and path != previous_path:
                self._conversion_queue.put_nowait(previous_path)

            # prepare for the next loop
            previous_path = path

    async def _convert(self):
        """Convert files to m4a."""

        while True:
            # get the next path form queue
            input_path: Path = await self._conversion_queue.get()

            # get albumn name
            timestamp = datetime.strptime(input_path.with_suffix('').name, "%Y-%m-%d-%H-%M")
            albumn_name = timestamp.strftime('%Y%m%d')

            # get output path
            output_path_parts = list(input_path.with_suffix('.mp3').parts)
            output_path_parts.insert(-1, albumn_name)
            Path(*output_path_parts[:-1]).mkdir(exist_ok=True)
            output_path = Path(*output_path_parts)

            # convert file to mp3
            logger.info(f'Converting file: {input_path}')
            process = await asyncio.create_subprocess_exec(
                'ffmpeg', '-i', input_path, output_path,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await process.communicate()

            # add ID3 tags
            audio = mutagen.easyid3.EasyID3(output_path)
            audio['album'] = timestamp.strftime('%Y%m%d')
            audio['title'] = timestamp.strftime('%H:%M')
            audio['tracknumber'] = str(timestamp.hour)
            audio['artist'] = self._station_name
            audio.save()

            # deleted original file
            input_path.unlink()

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

    def _get_sleep_time(self, lines: [str]) -> int:
        """Get sleep time based on available duration in response content of the stream URL.

        :param lines: response content of the stream URL, split into lines
        :return: sleep time, which determines how long to wait between starting the next iteration
        """

        available_duration = None
        for line in lines:
            if line.startswith('#EXT-X-COM-TUNEIN-AVAIL-DUR:'):
                try:
                    available_duration = float(line.replace('#EXT-X-COM-TUNEIN-AVAIL-DUR:', ''))
                except ValueError:
                    pass
                break
        return available_duration * 4 // 5 if available_duration else self.DEFAULT_SLEEP_TIME
