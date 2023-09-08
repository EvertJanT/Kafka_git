from kafka import KafkaConsumer
import json

class KafkaEventConsumer:
    def __init__(self, topic, bootstrap_servers):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
        )

    def consume_events(self):
        for message in self.consumer:
            event = message.value
            barcode = message.key
            if event is not None and isinstance(event, dict):
                yield barcode, event

class EventProcessor:
    def __init__(self, attributes_to_monitor=None):
        self.previous_values = {}
        self.attributes_to_monitor = attributes_to_monitor or []

    def process_event(self, barcode, event):
        for attribute in self.attributes_to_monitor:
            if attribute in event:
                self.process_attribute_change(barcode, attribute, event)

    def process_attribute_change(self, barcode, attribute, event):
        current_value = event[attribute]
        current_naam = event.get('naam', '')
        current_stad = event.get('woonplaats', '')

        if barcode in self.previous_values:
            previous_value = self.previous_values[barcode].get(attribute)
            if current_value != previous_value and previous_value != None :
                print(f"{attribute} voor barcode {barcode} is veranderd van: {previous_value} naar: {current_value}")
                print(f'Dit betreft een zending voor {current_naam} in {current_stad}')
            self.previous_values[barcode][attribute] = current_value
        else:
            self.previous_values[barcode] = {attribute: current_value}

# Voorbeeld gebruik

if __name__ == "__main__":
    kafka_topic = 'EJT-events6a'
    kafka_bootstrap_servers = ['localhost:9092']

    event_attributes_to_monitor = ['bezorgvenster', 'bezorgdag', 'woonplaats', 'leeftijd', 'naam']  # specificeer de attributen om te monitoren
    event_processor = EventProcessor(event_attributes_to_monitor)

    consumer = KafkaEventConsumer(kafka_topic, kafka_bootstrap_servers)

    # Process events obtained from Kafka
    for barcode, event in consumer.consume_events():
        event_processor.process_event(barcode, event)

