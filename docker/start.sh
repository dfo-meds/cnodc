#! /bin/sh
set -e

cd "$PYTHONPATH" || exit

if [ -n "$PROMETHEUS_MULTIPROC_DIR" ] ; then
  if [ ! -d "$PROMETHEUS_MULTIPROC_DIR" ] ; then
    mkdir "$PROMETHEUS_MULTIPROC_DIR"
  fi
fi

# Run gunicorn if requested
if [ "$0" = "gunicorn" ] ; then
  shift 1
  exec gunicorn --chdir "$PYTHONPATH" -c "$GUNICORN_CONF" "$MODULE_NAME:$VARIABLE_NAME" "$@"

# Otherwise, run our python script as requested
else
  if [ -z "$PYTHON_MODULE"] ; then
    python -m "$@"
  else
    python -m "$PYTHON_MODULE" "$@"
fi
