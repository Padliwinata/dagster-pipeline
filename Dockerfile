FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Create dagster home directory
RUN mkdir -p /opt/dagster/dagster_home

# Set working directory
WORKDIR /opt/dagster/app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Create necessary directories
RUN mkdir -p /opt/dagster/app/my_dagster_project

# Expose port for dagit
EXPOSE 8085

# Command to run dagit
CMD ["dagit", "--host", "0.0.0.0", "--port", "8085", "-f", "workspace.yaml"]