from confluent_kafka import Producer
import json
import time
from datetime import datetime
import random

def delivery_report(err, msg):
    """Delivery report callback"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} partition [{msg.partition()}]')

def produce_messages():
    # Producer configuration
    conf = {
        'bootstrap.servers': 'localhost:9092'
    }

    # Create Producer instance
    producer = Producer(conf)

    # Sample data types
    data_types = ['dataset1', 'dataset2', 'dataset2', 'dataset3', 'dataset4']
    
    try:
        while True:
            # Create message data
            message = {
                'data_type': random.choice(data_types),
                'timestamp': datetime.now().isoformat(),
            }
            
            # Convert message to JSON and encode as UTF-8
            message_json = json.dumps(message)
            
            # Produce message
            producer.produce(
                'test-topic_batch',
                value=message_json.encode('utf-8'),
                callback=delivery_report
            )
            
            # Flush to ensure delivery
            producer.flush()
            
            print(f'Produced message: {message}')
            time.sleep(30)  # Wait 1 second between messages
                
    except KeyboardInterrupt:
        print("Stopped by user")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        producer.flush(30)  # Flush with 30 second timeout
        print("Producer closed")

if __name__ == "__main__":
    produce_messages()