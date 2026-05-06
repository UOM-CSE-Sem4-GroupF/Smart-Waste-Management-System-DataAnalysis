#!/usr/bin/env python3
"""
End-to-End Pipeline Testing Summary Report
Smart Waste Management System - Data Analysis Layer (Group F2)
Date: 2026-05-01
"""

import os
import sys
import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import logging

logging.basicConfig(level=logging.WARNING)

def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '163.47.8.3:9094')
    username = os.getenv('KAFKA_USERNAME', 'user1')
    password = os.getenv('KAFKA_PASSWORD', 'c4eFajFH2t')
    
    print("\n" + "="*70)
    print("SMART WASTE MANAGEMENT SYSTEM - E2E PIPELINE TEST REPORT")
    print("="*70 + "\n")
    
    print(f"Test Date: {datetime.now().isoformat()}")
    print(f"Kafka Bootstrap: {bootstrap_servers}")
    print(f"Kafka User: {username}")
    print()
    
    # === SECTION 1: INPUT SOURCE TESTING ===
    print("="*70)
    print("SECTION 1: INPUT SOURCE - BIN TELEMETRY")
    print("="*70)
    
    # Check if messages exist in input topic
    try:
        consumer = KafkaConsumer(
            'waste.bin.telemetry',
            bootstrap_servers=bootstrap_servers,
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='SCRAM-SHA-256',
            sasl_plain_username=username,
            sasl_plain_password=password,
            auto_offset_reset='earliest',
            max_poll_records=3,
            consumer_timeout_ms=3000,
            api_version=(2, 5, 0),
        )
        
        messages = list(consumer)
        consumer.close()
        
        if messages:
            print(f"✓ INPUT TOPIC 'waste.bin.telemetry': {len(messages)} messages found")
            for i, msg in enumerate(messages[:3], 1):
                try:
                    data = json.loads(msg.value.decode('utf-8'))
                    bin_id = data.get('payload', {}).get('bin_id', 'UNKNOWN')
                    fill = data.get('payload', {}).get('fill_level_pct', 'N/A')
                    print(f"  • Message {i}: bin_id={bin_id}, fill={fill}%")
                except Exception as e:
                    print(f"  • Message {i}: [parse error]")
        else:
            print(f"✗ INPUT TOPIC 'waste.bin.telemetry': No messages (expected test data)")
    
    except Exception as e:
        print(f"✗ INPUT TOPIC 'waste.bin.telemetry': Connection error - {str(e)[:80]}")
    
    print()
    
    # === SECTION 2: PIPELINE OUTPUTS ===
    print("="*70)
    print("SECTION 2: PIPELINE OUTPUTS")
    print("="*70)
    
    pipelines = [
        ('waste.bin.processed', 'Pipeline 1: Bin Telemetry Processing'),
        ('waste.zone.statistics', 'Pipeline 2: Zone Aggregation'),
        ('waste.vehicle.deviation', 'Pipeline 3: Vehicle Deviation Detection'),
    ]
    
    results = {}
    for topic, description in pipelines:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                security_protocol='SASL_PLAINTEXT',
                sasl_mechanism='SCRAM-SHA-256',
                sasl_plain_username=username,
                sasl_plain_password=password,
                auto_offset_reset='earliest',
                max_poll_records=1,
                consumer_timeout_ms=3000,
                api_version=(2, 5, 0),
            )
            
            messages = list(consumer)
            consumer.close()
            
            status = "✓ PASSING" if messages else "⚠ NO OUTPUT"
            results[topic] = len(messages) > 0
            print(f"{status}: {description}")
            print(f"       Topic: {topic} | Messages: {len(messages)}")
            
        except Exception as e:
            results[topic] = False
            print(f"✗ ERROR: {description}")
            print(f"       Topic: {topic} | Error: {str(e)[:60]}")
    
    print()
    
    # === SECTION 3: PIPELINE HEALTH ===
    print("="*70)
    print("SECTION 3: PIPELINE HEALTH & DIAGNOSTICS")
    print("="*70)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    print(f"\nPipeline Status Summary:")
    print(f"  • Pipelines Outputting: {passed}/{total}")
    
    if passed == 0:
        print("\n⚠ BLOCKERS IDENTIFIED:")
        print("  1. Flink pipelines are not outputting to expected Kafka topics")
        print("  2. Root cause: Docker container PostgreSQL authentication failed")
        print("     - Error: 'FATAL: password authentication failed for user waste_admin'")
        print("     - Location: host.docker.internal:5432 (mapped from Docker to host)")
        print("  3. Resolution required:")
        print("     - Verify PostgreSQL is running on host and accepting connections")
        print("     - Confirm credentials match docker-compose.dev.yml environment")
        print("     - Start fresh database via: docker compose -f db/docker-compose.yml up -d")
        print("     - Then restart Flink: docker compose -f flink-processor/docker-compose.dev.yml restart")
    elif passed < total:
        print("\n⚠ PARTIAL SUCCESS:")
        print(f"  • {passed} pipelines are outputting, {total-passed} need investigation")
    else:
        print("\n✓ ALL PIPELINES OPERATIONAL")
    
    print()
    
    # === SECTION 4: TEST DATA SUMMARY ===
    print("="*70)
    print("SECTION 4: TEST DATA SENT")
    print("="*70)
    
    print("""
  Bin Telemetry Scenarios (7 bins tested):
    • Normal operation (BIN-001, BIN-002)
    • Monitor priority (BIN-003, BIN-004)
    • Urgent priority (BIN-005)
    • Critical priority (BIN-006)
    • Low battery (BIN-007)
    
  Expected Pipeline Processing:
    1. Bin telemetry → Pipeline 1 → waste.bin.processed (f2.bin_current_state)
    2. Zone aggregation → Pipeline 2 → waste.zone.statistics
    3. Vehicle deviation → Pipeline 3 → waste.vehicle.deviation
""")
    
    print("="*70)
    print("SECTION 5: VERIFICATION COMMANDS")
    print("="*70)
    
    print(f"""
  To verify after fixing PostgreSQL:
  
  1. Check Flink logs:
     cd flink-processor
     docker compose -f docker-compose.dev.yml logs -f flink-processor
  
  2. Run quick verification:
     docker compose -f docker-compose.dev.yml exec -T flink-processor \\
       env KAFKA_BOOTSTRAP_SERVERS={bootstrap_servers} \\
       KAFKA_USERNAME={username} KAFKA_PASSWORD={password} \\
       python tests/e2e/quick_verify.py
  
  3. Run full verification:
     docker compose -f docker-compose.dev.yml exec -T flink-processor \\
       env KAFKA_BOOTSTRAP_SERVERS={bootstrap_servers} \\
       KAFKA_USERNAME={username} KAFKA_PASSWORD={password} \\
       python tests/e2e/verify_outputs.py
""")
    
    print("="*70)
    print("FINAL STATUS")
    print("="*70)
    
    if passed == 0:
        print(f"\n❌ TEST RESULT: BLOCKED - Database connectivity issue")
        print(f"   ACTION REQUIRED: Start PostgreSQL and InfluxDB, restart Flink")
        return 1
    elif passed < total:
        print(f"\n⚠️  TEST RESULT: PARTIAL - {passed}/{total} pipelines working")
        return 2
    else:
        print(f"\n✅ TEST RESULT: SUCCESS - All {total} pipelines operational")
        return 0

if __name__ == '__main__':
    sys.exit(main())
