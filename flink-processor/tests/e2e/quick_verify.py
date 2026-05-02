#!/usr/bin/env python3
"""Quick pipeline verification - simplified and fast."""

import os
import sys
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
import json
import logging

logging.basicConfig(level=logging.WARNING)

def check_kafka_topics():
    """Check if pipeline outputs exist in Kafka topics."""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '163.47.8.3:9094')
    username = os.getenv('KAFKA_USERNAME', 'user1')
    password = os.getenv('KAFKA_PASSWORD', 'c4eFajFH2t')
    
    topics_to_check = {
        'waste.bin.processed': 'Pipeline 1 output',
        'waste.zone.statistics': 'Pipeline 2 output',
        'waste.vehicle.deviation': 'Pipeline 3 output',
    }
    
    print("\n=== KAFKA VERIFICATION ===")
    
    for topic, description in topics_to_check.items():
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                security_protocol='SASL_PLAINTEXT',
                sasl_mechanism='SCRAM-SHA-256',
                sasl_plain_username=username,
                sasl_plain_password=password,
                auto_offset_reset='latest',
                max_poll_records=5,
                consumer_timeout_ms=3000,
                api_version=(2, 5, 0),
            )
            
            messages = []
            for msg in consumer:
                messages.append(msg)
            
            consumer.close()
            
            if messages:
                print(f"✓ {topic}: {description} - Found {len(messages)} messages")
                for i, msg in enumerate(messages[:2], 1):
                    try:
                        data = json.loads(msg.value.decode('utf-8'))
                        print(f"  Message {i}: {json.dumps(data, indent=2)[:200]}...")
                    except:
                        print(f"  Message {i}: {msg.value.decode('utf-8')[:100]}...")
            else:
                print(f"⚠ {topic}: {description} - No messages found (processing may be delayed)")
        
        except Exception as e:
            print(f"✗ {topic}: {description} - Error: {str(e)[:100]}")

if __name__ == '__main__':
    check_kafka_topics()
    print("\n=== END VERIFICATION ===\n")
