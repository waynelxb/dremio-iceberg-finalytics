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
    # pyspark 
    pyspark==${SPARK_VERSION} \
    findspark \
    # jupyter
    jupyterlab \
    jupyter_scheduler \
    notebook \
    ipywidgets \
    # Since poetry 2.0.0, the shell is a poetry-plugin
    poetry-plugin-shell \
    poetry-kernel \
    # database
    sqlalchemy \
    psycopg2-binary \
    # data processing   
    pandas \
    numpy \
    datafusion \
    getdaft[all] \
    sqlframe[all] \
    ibis-framework[all] \
    scipy \
    matplotlib \
    seaborn \
    pyarrow \
    requests \
    beautifulsoup4 \
    lxml \
    duckdb \
    polars \
    # dremio, minio, iceberg    
    dremio-simple-query \
    boto3 \
    s3fs \
    minio \
    pyiceberg[gcsfs,adlfs,s3fs,sql-sqlite,sql-postgres,glue,hive]
    # scikit-learn \
    # tensorflow \
    # torch \
    # xgboost \
    # lightgbm \
    # dask \
    # statsmodels \
    # plotly \
    # openpyxl \

    
# Enable the Jupyter Scheduler
RUN jupyter server extension list && \
    jupyter server extension enable --user --py jupyter_scheduler    

# Set umask globally by adding it to the entrypoint
RUN echo "umask 000" >> /etc/profile


# The CMD instruction in a Dockerfile only runs when the container is started. 
# It defines the default command that will be executed when the container starts, unless it is overridden by entrypoint specified in docker-compose file at runtime.
# CMD below ensures that Spark services, the History Server, and JupyterLab are launched in sequence.
CMD bash -c "source /etc/profile && \
    $SPARK_HOME/sbin/start-master.sh && \
    # $(hostname) is container name, spark-jupyterlab. If container name is not specified, it will be the service name.
    # So if you intend for Spark components to communicate using the service name in your docker-compose, like for dependency, be careful when using these names
    # And to make it simple, you can make container name the same as corresponding service name, or you should use the name explicitly instead of $(hostname). 
    $SPARK_HOME/sbin/start-worker.sh spark://$(hostname):7077 && \
    # keep the mkdir -p /tmp/spark-events command in the container's startup sequence to ensure the Spark History Server functions properly.
    mkdir -p /tmp/spark-events && \ 
    $SPARK_HOME/sbin/start-history-server.sh && \     
    jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''"
