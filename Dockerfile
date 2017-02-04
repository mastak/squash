FROM python:3.6-slim

COPY . /usr/src/squash

RUN pip install -e /usr/src/squash

EXPOSE 6379

ENTRYPOINT ["python", "/usr/src/squash/squash/server.py"]
