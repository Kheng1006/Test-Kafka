FROM postgres:13.16

# Install PL/Python3 extension, pip, and venv
RUN apt-get update && apt-get install -y postgresql-plpython3-13 python3-pip python3-venv

# Create a virtual environment
RUN python3 -m venv /opt/venv

# Activate the virtual environment and install the packages
RUN /opt/venv/bin/pip install deltalake typing-extensions

# Set the environment variables to use the virtual environment by default
ENV PATH="/opt/venv/bin:$PATH"
