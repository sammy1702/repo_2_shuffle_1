FROM openjdk:11-slim

RUN apt-get update && apt-get install -y python3 python3-pip procps && rm -rf /var/lib/apt/lists/*

#RUN pip3 install --no-cache-dir -r requirements.txt
RUN pip3 install pyspark

COPY shuffle_1.py /app/shuffle_1.py
COPY . /app

WORKDIR /app

CMD ["python3", "shuffle_1.py"]
