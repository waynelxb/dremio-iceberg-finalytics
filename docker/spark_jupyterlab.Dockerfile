# Use Python 3.11 base image with Debian Bullseye
ARG python_version=3.11
FROM python:${python_version}-bullseye

# Set the default Shell to bash and some environment variables # 
# /usr/lib/jvm/java-11-openjdk-amd64 is where openjdk will be installed by default. This folder will be created when installing openjdk
# /opt/spark is where spark will be installed. This folder will will be created when install spark
ENV SHELL=/bin/bash \
    SHARED_WORKSPACE=/opt/workspace \
    SPARK_VERSION=3.5.2 \
    HADOOP_VERSION=3 \
    PYTHON_VERSION=${python_version} \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
    

# Create shared workspace and set as working directory
RUN mkdir -p ${SHARED_WORKSPACE}
WORKDIR ${SHARED_WORKSPACE}
VOLUME ${SHARED_WORKSPACE}


# Install essential tools and dependencies
RUN apt-get update -y --fix-missing && \
    apt-get install -y --no-install-recommends \
        sudo \
        curl \
        vim \
        unzip \
        rsync \
        nano \
        openjdk-11-jdk \
        build-essential \
        software-properties-common \
        ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Install Spark
# archival link https://archive.apache.org/dist/spark/
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz


# Install Poetry
ENV POETRY_HOME=/root/.local
ENV PATH=$POETRY_HOME/bin:$PATH
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    # The following also works, but /root/.bashrc is only sourced by bash shell.
    # echo 'export PATH=$PATH:$POETRY_HOME/bin' >> /root/.bashrc
    # Add POETRY_HOME to PATH in /etc/profile, where python:3.11-slim saves the default PATH.
    # Add it to /etc/profile, the setting can be used for sh (the default JupyterLab container interactive shell) and bash.     
    echo 'export PATH=$PATH:$POETRY_HOME/bin' >> /etc/profile && \
# Create poetry env within project 
    poetry config virtualenvs.in-project true


# Upgrade pip
RUN pip3 install --upgrade pip 

# Install Python Libraries
RUN pip install --no-cache-dir \
    jupyterlab \
    scipy \
    findspark \
    getdaft[all] \
    sqlframe[all] \
    ipywidgets \
    ibis-framework[all] \
    notebook \
    pandas \
    numpy \
    matplotlib \
    seaborn \
    # scikit-learn \
    # tensorflow \
    # torch \
    # xgboost \
    # lightgbm \
    # dask \
    # statsmodels \
    # plotly \
    # openpyxl \
    pyarrow \
    sqlalchemy \
    psycopg2-binary \
    requests \
    beautifulsoup4 \
    lxml \
    duckdb \
    polars \
    pyspark==3.5.2 \
    dremio-simple-query \
    boto3 \
    s3fs \
    minio \
    poetry-kernel \
    datafusion \
    pyiceberg[gcsfs,adlfs,s3fs,sql-sqlite,sql-postgres,glue,hive]


# Set umask globally by adding it to the entrypoint
RUN echo "umask 000" >> /etc/profile

# Start Spark Master, Worker, and JupyterLab
CMD bash -c "source /etc/profile && \
    $SPARK_HOME/sbin/start-master.sh && \
    $SPARK_HOME/sbin/start-worker.sh spark://$(hostname):7077 && \
    jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''"