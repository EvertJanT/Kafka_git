from kafka import KafkaConsumer
import ast
import json
from bs4 import BeautifulSoup

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
consumer.subscribe(['your-topic6'])

for event in consumer:
    message = event.value
    json_str = message.decode('utf-8')
    # Parse the JSON string into a Python dictionary
    data = json.loads(json_str)
    # Access the "text" key within the dictionary
    text = data['text']
    #remove HTML tags
    soup = BeautifulSoup(text, 'html.parser')
    text_without_tags = soup.get_text()
    # Print the text without HTML tags
    print(text_without_tags)
    print('sentiment')
    print(data['sentiment'])
    print('gebruiker')
    print('@'+ str(data['user'])+ ' ' + data['timestamp'])
    print('')


