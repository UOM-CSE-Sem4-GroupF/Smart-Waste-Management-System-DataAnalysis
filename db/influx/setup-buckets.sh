#!/bin/sh
set -e

INFLUX_HOST="${INFLUXDB_HOST:-http://influxdb:8086}"
INFLUX_ORG="${INFLUXDB_ORG:-waste-org}"
INFLUX_TOKEN="${INFLUXDB_TOKEN:?INFLUXDB_TOKEN is required}"

wait_for_influx() {
  echo "Waiting for InfluxDB at ${INFLUX_HOST}..."
  i=0
  until influx ping --host "${INFLUX_HOST}" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -gt 60 ]; then
      echo "InfluxDB did not become ready in time."
      exit 1
    fi
    sleep 2
  done
}

bucket_exists() {
  name="$1"
  influx bucket list \
    --host "${INFLUX_HOST}" \
    --org "${INFLUX_ORG}" \
    --token "${INFLUX_TOKEN}" \
    --name "${name}" 2>/dev/null | grep -q "${name}"
}

ensure_bucket() {
  name="$1"
  retention="$2"

  if bucket_exists "${name}"; then
    echo "Bucket ${name} already exists."
    return 0
  fi

  if [ "${retention}" = "forever" ]; then
    influx bucket create \
      --host "${INFLUX_HOST}" \
      --org "${INFLUX_ORG}" \
      --token "${INFLUX_TOKEN}" \
      --name "${name}" >/dev/null
  else
    influx bucket create \
      --host "${INFLUX_HOST}" \
      --org "${INFLUX_ORG}" \
      --token "${INFLUX_TOKEN}" \
      --name "${name}" \
      --retention "${retention}" >/dev/null
  fi

  echo "Created bucket ${name} (retention: ${retention})."
}

wait_for_influx

# 1 year retention
ensure_bucket "bin_readings_raw" "8760h"
ensure_bucket "vehicle_positions" "8760h"

# 90 day retention
ensure_bucket "bin_readings_processed" "2160h"

# 2 year retention
ensure_bucket "zone_statistics" "17520h"

# Forever retention
ensure_bucket "waste_generation_trends" "forever"

echo "InfluxDB bucket setup completed."
