import threading
from confluent_kafka import Producer
from mastodon import Mastodon, StreamListener
import signal
import json
from textblob import TextBlob

#define the hastag to monitor

HT = 'nieuws'

# Create a list of Mastodon instances with different access tokens and server URLs
mastodon_instances = [
    {
        'access_token': 'YT',
        'api_base_url': 'https://mastodon.social'
    },
    {
        'access_token': 'YT',
        'api_base_url': 'https://mastodon.nl'
    },
    # Add more instances as needed
]

class MastodonStream:
    def __init__(self, access_token, api_base_url, kafka_config, kafka_topic):
        self.mastodon_api = Mastodon(
            access_token=access_token,
            api_base_url=api_base_url
        )
        self.kafka_producer = Producer(kafka_config)
        self.kafka_topic = kafka_topic
        self.stream_thread = None
        self.stop_stream_flag = threading.Event()

    class MyStreamListener(StreamListener):
        def __init__(self, kafka_producer, kafka_topic):
            super().__init__()
            self.kafka_producer = kafka_producer
            self.kafka_topic = kafka_topic

        def on_update(self, status):
            status_text = status.get('content', '')
            user_name = status.get('account', {}).get('username', '')
            timestamp = str(status.get('created_at', ''))

            #if ('#<span>' + HT) in status_text.lower():
            if HT in status_text.lower():    
                cleaned_content = status_text
                blob_object = TextBlob(str(cleaned_content))
                sentiment = blob_object.sentiment.polarity
                subjectivity = blob_object.sentiment.subjectivity

                toot_data = {
                    'text': status_text,
                    'user': user_name,
                    'timestamp': timestamp,
                    'sentiment': sentiment
                }

                json_data = json.dumps(toot_data)
                self.kafka_producer.produce(self.kafka_topic, key=None, value=json_data.encode('utf-8'))

    def start_stream(self):
        my_stream_listener = self.MyStreamListener(self.kafka_producer, self.kafka_topic)
        self.stream_thread = threading.Thread(target=self.mastodon_api.stream_public, args=(my_stream_listener,))
        self.stream_thread.start()

    def stop_stream(self):
        self.stop_stream_flag.set()

    def wait_for_stream_to_finish(self):
        self.stream_thread.join()

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Change to your Kafka broker(s)
    'client.id': 'your-topic6'
}

topic = 'your-topic6'

# Create an instance of MastodonStream for each Mastodon instance
mastodon_streams = []
for instance in mastodon_instances:
    mastodon_stream = MastodonStream(
        access_token=instance['access_token'],
        api_base_url=instance['api_base_url'],
        kafka_config=kafka_config,
        kafka_topic=topic
    )
    mastodon_streams.append(mastodon_stream)



# Set up the signal handler for Ctrl+C (SIGINT) for all streams
for mastodon_stream in mastodon_streams:
    signal.signal(signal.SIGINT, mastodon_stream.stop_stream)

# Start streaming user events in separate threads for all streams
for mastodon_stream in mastodon_streams:
    mastodon_stream.start_stream()

# Listen for a key press to stop the stream
print("Druk op enter om de stream te stoppen.")
input()  # This will block until Enter is pressed

# Wait for any outstanding messages to be delivered and delivery reports received for all streams
for mastodon_stream in mastodon_streams:
    mastodon_stream.kafka_producer.flush()

# Stop all streams when Enter is pressed
for mastodon_stream in mastodon_streams:
    mastodon_stream.stop_stream()
    mastodon_stream.wait_for_stream_to_finish()

print("Stream stopped.")





