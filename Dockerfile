FROM python:3.11-slim

# set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# create dagster home directory
RUN mkdir -p $DAGSTER_HOME

# set working directory
WORKDIR /opt/dagster/app

# copy dependency files first for better caching
COPY pyproject.toml README.md ./

# install Python dependencies
RUN pip install --no-cache-dir -e .

# copy dagster configuration
COPY dagster.yaml $DAGSTER_HOME/dagster.yaml

# copy application code
COPY orchestrator/ orchestrator/
COPY dbt_src/ dbt_src/

# create data directories
RUN mkdir -p /opt/dagster/app/data/bronze \
    /opt/dagster/app/data/silver

# expose Dagster webserver port
EXPOSE 3001

# default command (overridden in docker-compose)
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3001"]
