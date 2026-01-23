# syntax=docker/dockerfile:1

# Note that 3.9 is required for greenlet to work (as of July 21/2022), 3.10 fails to compile
FROM python:3.9.13-slim-bullseye

# Get the required libraries to compile psycopg
RUN apt-get update
RUN apt-get install libpq-dev build-essential dumb-init -y

WORKDIR /srv/cnodc

VOLUME /cnodc-config
VOLUME /cnodc-data

ENV CNODC_CONFIG_SEARCH_PATHS=/srv/cnodc/app;/cnodc-config
ENV PYTHONPATH=/srv/cnodc/app
ENV MODULE_NAME=app
ENV VARIABLE_NAME=app
ENV GUNICORN_CONF=/srv/cnodc/gunicorn_conf.py

RUN pip install --upgrade pip

COPY docker/requirements-docker.txt requirements-docker.txt

RUN pip install -r requirements-docker.txt

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY docker/start.sh start.sh
RUN chmod +x start.sh

COPY docker/gunicorn_conf.py gunicorn_conf.py

COPY src app/src
COPY vocab app/vocab
COPY app.py app/app.py
COPY process.py app/process.py
COPY docker/.cnodc.processor.defaults.toml app/.cnodc.processor.defaults.toml
COPY docker/.cnodc.web.defaults.toml app/.cnodc.web.defaults.toml
COPY docker/.cnodc.defaults.toml app/.cnodc.defaults.toml

EXPOSE 80

WORKDIR /srv/cnodc/app

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/srv/cnodc/start.sh"]

CMD ["web"]
