FROM quay.io/astronomer/astro-runtime:11.7.0

RUN python -m venv soda_venv && source soda_venv/bin/activate && \
    pip install soda-core-bigquery==3.3.18 && \
    pip install soda-core-scientific==3.3.18 && deactivate

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install dbt-bigquery==1.8.2 && deactivate