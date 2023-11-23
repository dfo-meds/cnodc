#! /bin/sh
set -e

PYTHONBUFFERED=TRUE
export PYTHONUNBUFFERED

cd /srv/cnodc/app || exit

# Run the daemon
if [ "$1" = "processor" ] ; then

  echo "not supported"
  exit 1

# Upgrade or install
elif [ "$1" = "upgrade" ] ; then

  echo "not supported"
  exit 1

else

  # Check for the default name and remove it
  if [ "$1" = "web" ] ; then
    shift 1
  fi

  # Set the Prometheus directory
  export PROMETHEUS_MULTIPROC_DIR=/srv/cnodc/_prometheus

  # Handle prometheus directory
  if [ -e "/srv/cnodc/_prometheus" ] ; then
    rm -r /srv/cnodc/_prometheus/*
  else
    mkdir /srv/cnodc/_prometheus
  fi

  # Start Gunicorn or Flask
  if [ -z "$USE_FLASK" ]; then
    exec gunicorn --chdir /srv/cnodc -c "$GUNICORN_CONF" "app:app" "$@"
  else
    python -m flask run --host="0.0.0.0" --port=80
  fi

fi
