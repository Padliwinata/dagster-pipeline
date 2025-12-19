FROM python:3.9-slim

# Environment
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Create Dagster home
RUN mkdir -p /opt/dagster/dagster_home

# Set workdir
WORKDIR /opt/dagster/app

# Install system deps (optional but recommended)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first (better cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ONLY what runtime needs
COPY my_dagster_project ./my_dagster_project
COPY workspace.yaml .
COPY dagster.yaml .

# Expose Dagster webserver
EXPOSE 8085

# Run Dagster webserver
CMD ["dagster-webserver", "--host", "0.0.0.0", "--port", "8085"]
