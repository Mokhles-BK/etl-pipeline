#!/usr/bin/env bash
set -e
VENV=/home/mokhles/airflow-venv
REPO=/mnt/c/Users/mokhl/etl-pipeline
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
echo "=== reinstall etl-pipeline editable ==="
"$VENV/bin/pip" install -e "$REPO" -q 2>&1 | tail -3
echo "=== reinstall Airflow stack with correct pins ==="
"$VENV/bin/pip" install -r "$REPO/requirements-airflow.txt" --constraint "$CONSTRAINTS" 2>&1 | tail -12
echo "=== verify ==="
"$VENV/bin/airflow" version 2>&1 | head -2
"$VENV/bin/python" -c "import etl.cli, etl.pipeline, etl.sources.usgs_earthquakes; print('etl OK')"
echo "INSTALL_DONE"