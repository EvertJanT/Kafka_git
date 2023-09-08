#KAFKA opstart

# ga naar command propmt en in de kafka directoty start zookeeper via 

#.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# open een andere command prompt en in de kafka directoy start de kafka server via:

#.\bin\windows\kafka-server-start.bat .\config\server.properties

from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
with open('c:\pythonej\kafka-input-file.txt') as f:
     lines = f.readlines()

for line in lines:
    producer.send('your-topic', json.dumps(line).encode('utf-8'))
