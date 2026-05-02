#!/usr/bin/env python3
"""
Patched verify_outputs that works around DNS resolution issues
by manually resolving controller.internal to the bootstrap server IP.
"""

import socket
import os

# Monkey-patch socket.getaddrinfo to resolve controller.internal manually
_original_getaddrinfo = socket.getaddrinfo

def patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    """Override getaddrinfo to manually resolve controller.internal."""
    if host == 'controller.internal':
        # Map to the bootstrap server IP (from docker-compose.dev.yml or env)
        bootstrap_host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '163.47.8.3:9094').split(':')[0]
        print(f"[DNS-PATCH] Resolving controller.internal -> {bootstrap_host}")
        host = bootstrap_host
    return _original_getaddrinfo(host, port, family, type, proto, flags)

# Apply the patch
socket.getaddrinfo = patched_getaddrinfo

# Now import and run the original verify_outputs
import sys
sys.path.insert(0, os.path.dirname(__file__))

# Import all the verify functions
from verify_outputs import (
    verify_kafka_topic,
    verify_postgres,
    verify_influx,
    main as original_main
)

if __name__ == '__main__':
    print("Running verify_outputs with DNS patching for controller.internal...")
    original_main()
