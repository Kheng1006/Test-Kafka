FROM apache/airflow:2.10.0

# Install Python packages
RUN pip install --no-cache-dir minio google-api-python-client google-auth-oauthlib python-dotenv google-auth-httplib2 google-auth

USER root

# Update package lists and install wget
RUN apt-get update && apt-get install -y wget

# Download and install Google Chrome
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb

# Clean up to reduce image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
