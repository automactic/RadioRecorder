FROM python:3.8-slim

RUN apt-get update -y && apt-get install -y ffmpeg

WORKDIR /usr/src/
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app ./app
CMD [ "python", "./app/main.py" ]