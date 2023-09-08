#KAFKA opstart

# ga naar command propmt en in de kafka directoty start zookeeper via 

#.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# open een andere command prompt en in de kafka directoy start de kafka server via:

#.\bin\windows\kafka-server-start.bat .\config\server.properties

import json
from confluent_kafka import Producer, KafkaError

class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers, client_id):
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': client_id
        }
        self.producer = Producer(self.producer_config)

    def produce_message(self, topic, key, value):
        try:
            self.producer.produce(topic, key=key, value=value, callback=self.delivery_report)
            self.producer.flush()
        except KafkaError as e:
            print(f'Fout bij het produceren van het bericht: {e}')

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print(f'Levering van het bericht mislukt: {err}')
        else:
            print(f'Bericht afgeleverd op {msg.topic()} [{msg.partition()}] op offset {msg.offset()}')

class JSONFileHandler:
    @staticmethod
    def opslaan_naar_json_bestand(data, bestandsnaam):
        try:
            with open(bestandsnaam, "w") as json_bestand:
                json.dump(data, json_bestand, indent=4)
            print(f"Gegevens opgeslagen in {bestandsnaam}")
        except Exception as e:
            print(f'Er is een fout opgetreden bij het opslaan van gegevens in {bestandsnaam}: {e}')

    @staticmethod
    def laden_uit_json_bestand(bestandsnaam):
        data = []
        try:
            with open(bestandsnaam, "r") as json_bestand:
                data = json.load(json_bestand)
            print("Gegevens geladen.")
        except FileNotFoundError:
            print(f'Bestand "{bestandsnaam}" niet gevonden.')
        except json.JSONDecodeError as e:
            print(f'JSON-decoderingsfout: {e}')
        except Exception as e:
            print(f'Er is een fout opgetreden bij het laden van gegevens uit {bestandsnaam}: {e}')
        return data

# Voorbeeld van gebruik:

# Kafka-producent
kafka_bootstrap_servers = 'localhost:9092'  # Vervang door het adres van uw Kafka-makelaar(s)
kafka_client_id = 'python-producent'
kafka_topic = 'EJT-events6a'
producer_wrapper = KafkaProducerWrapper(kafka_bootstrap_servers, kafka_client_id)

# Handler voor JSON-bestanden
uitvoer_bestandsnaam = "records.json"
invoer_bestandsnaam = "records.json"
json_gegevens = [
    {'naam': "Alice", 'leeftijd': 30, 'stad': "Amsterdam", 'barcode': '3S12345', 'bezorgdag': "woensdag",
     'bezorgvenster': "1500-1700"},
    {'naam': "Bob", 'leeftijd': 25, 'stad': "Deventer", 'barcode': '3S54321', 'bezorgdag': "donderdag",
     'bezorgvenster': "13:00-15:00"},
    {'naam': "Jaap", 'leeftijd': 35, 'stad': "Nijmegen", 'barcode': '3S9999', 'bezorgdag': "zaterdag",
     'bezorgvenster': "13:00-15:00"}
]

# Gegevens opslaan naar JSON-bestand
JSONFileHandler.opslaan_naar_json_bestand(json_gegevens, uitvoer_bestandsnaam)

# Gegevens laden uit JSON-bestand
geladen_gegevens = JSONFileHandler.laden_uit_json_bestand(invoer_bestandsnaam)

# Berichten produceren naar Kafka
for waarde in geladen_gegevens:
    bericht_sleutel = waarde['barcode']
    bericht_waarde = {
        'naam': waarde['naam'],
        'leeftijd': waarde['leeftijd'],
        'woonplaats': waarde['stad'],
        'bezorgdag': waarde['bezorgdag'],
        'bezorgvenster': waarde['bezorgvenster']
    }
    json_bericht = json.dumps(bericht_waarde).encode('utf-8')
    producer_wrapper.produce_message(kafka_topic, bericht_sleutel, json_bericht)


