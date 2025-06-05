FROM openjdk:17-slim

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    unzip \
    procps \
 && curl -s https://rclone.org/install.sh | bash \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark

WORKDIR /app

COPY shuffle_1.py /app/

CMD ["python3", "shuffle_1.py"]
