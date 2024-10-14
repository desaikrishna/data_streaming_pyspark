#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
    $(command python) pip install --upgrade pip
    $(command -v pip) install --user -r requirements.txt
fi

airflow db init

if [ ! -f "/opt/airflow/airflow.db" ]; then
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password asd
fi

$(command -v airflow) db upgrade

exec airflow webserver