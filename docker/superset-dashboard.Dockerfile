FROM apache/superset
# Switching to root to install the required packages
USER root

RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    libpq-dev \
    build-essential \
    libssl-dev \
    libffi-dev \
    pkg-config \
    libmariadb-dev \
    default-libmysqlclient-dev \
    curl \
    unzip \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install uv
RUN uv pip install --system duckdb-engine==0.13.0 duckdb==1.1.3 


USER superset

# Install Python database drivers
RUN pip install --no-cache-dir \
    duckdb \
    mysql-connector-python \
    pymysql \
    pyodbc \
    mysqlclient \
    psycopg2-binary \
    sqlalchemy-dremio \
    sqlalchemy-redshift


ENV SUPERSET_SECRET_KEY=ThisIsMySupersetSecretKey
RUN superset fab create-admin \
              --username admin \
              --firstname superset \
              --lastname admin \
              --email example@email.com \
              --password admin
RUN superset db upgrade
RUN superset init
RUN superset load_examples
