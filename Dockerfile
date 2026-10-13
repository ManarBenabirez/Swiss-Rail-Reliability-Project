# One image for the whole project: Python + Java + Spark (PySpark).
# Works on Intel/AMD laptops and on Apple Silicon (arm64).
FROM python:3.11-slim-bookworm

# Spark runs on the Java Virtual Machine, so we need a Java runtime.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps unzip \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The project code is mounted as a volume by docker-compose (see docker-compose.yml),
# so editing a file on your laptop is immediately visible inside the container.
CMD ["bash", "run_pipeline.sh"]
