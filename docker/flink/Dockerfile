FROM flink:1.18.1

# Install Python 3, pip, and required system libraries
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Create a symbolic link so 'python' points to 'python3'
RUN ln -s /usr/bin/python3 /usr/bin/python

# Upgrade pip and install PyFlink + other dependencies required for our scripts
RUN pip3 install --no-cache-dir apache-flink==1.18.1 redis confluent-kafka
