# API Integration Guide: Security & Events Tab
## Smart Waste Management System - Anomaly Detection

**Version**: 1.0  
**Date**: May 14, 2026  
**Audience**: Frontend Development Team  
**Base URL**: `http://localhost:8000/api/v1`

---

## Table of Contents
1. [Overview](#overview)
2. [Security Incident Endpoints](#security-incident-endpoints)
3. [Event Management Endpoints](#event-management-endpoints)
4. [Anomaly Correlation Endpoints](#anomaly-correlation-endpoints)
5. [Response Models](#response-models)
6. [Error Handling](#error-handling)
7. [Rate Limiting & Pagination](#rate-limiting--pagination)
8. [WebSocket Streaming](#websocket-streaming-optional)
9. [Sample Responses](#sample-responses)
10. [Authentication](#authentication)

---

## Overview

The Smart Waste Management System now includes comprehensive anomaly detection with a focus on:

1. **Security Anomalies** - Detecting hacking attempts, spoofed data, coordinated attacks
2. **Event-Based Anomalies** - Capturing unpredictable waste patterns due to special events

### Key Features for Frontend
- Real-time security incident dashboard
- Event logging UI for ops team
- Anomaly correlation visualization
- Auto-detected event suggestions
- Impact analysis for logged events

---

## Security Incident Endpoints

### 1. Get Recent Security Incidents

**Endpoint**: `GET /anomalies/security/recent`

**Purpose**: Retrieve recent security incidents with filtering capabilities

**Query Parameters**:
```
- hours (int, default: 24): Look back N hours [1-720]
- limit (int, default: 100): Max results [1-1000]
- event_type (string, optional): Filter by incident type
- severity (string, optional): Filter by severity (LOW, MEDIUM, HIGH, CRITICAL)
```

**Example Request**:
```bash
curl -X GET "http://localhost:8000/api/v1/anomalies/security/recent?hours=24&limit=50&severity=CRITICAL" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response**:
```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "event_type": "COORDINATED_ATTACK",
      "severity": "CRITICAL",
      "attack_vector": "3+ critical failures across zones 1, 3, 5",
      "affected_bins": ["BIN-001", "BIN-042", "BIN-087"],
      "affected_zones": [1, 3, 5],
      "source_ips": ["192.168.1.100"],
      "bayesian_probability": 0.92,
      "resolution_status": "open",
      "timestamp": "2026-05-14T10:30:00Z",
      "created_at": "2026-05-14T10:30:01Z"
    }
  ],
  "timestamp": "2026-05-14T15:00:00Z",
  "total_count": 1
}
```

---

### 2. Get Security Dashboard Summary

**Endpoint**: `GET /anomalies/security/dashboard-summary`

**Purpose**: High-level security KPIs for dashboard widget

**Query Parameters**: None

**Example Request**:
```bash
curl -X GET "http://localhost:8000/api/v1/anomalies/security/dashboard-summary" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response**:
```json
{
  "success": true,
  "data": {
    "total_incidents_24h": 5,
    "critical_incidents_24h": 1,
    "open_incidents": 2,
    "resolved_incidents_24h": 3,
    "attack_types_distribution": {
      "COORDINATED_ATTACK": 1,
      "ENTROPY_ANOMALY": 2,
      "NETWORK_ANOMALY": 2
    },
    "avg_resolution_time_hours": 2.5,
    "false_alarm_rate_pct": 8.0,
    "last_incident_timestamp": "2026-05-14T14:30:00Z"
  },
  "timestamp": "2026-05-14T15:00:00Z"
}
```

---

### 3. Get Security Incident Details

**Endpoint**: `GET /anomalies/security/{incident_id}`

**Purpose**: Detailed information about a specific security incident

**Path Parameters**:
```
- incident_id (int): Security incident ID
```

**Example Request**:
```bash
curl -X GET "http://localhost:8000/api/v1/anomalies/security/1" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response**:
```json
{
  "success": true,
  "data": {
    "id": 1,
    "event_type": "COORDINATED_ATTACK",
    "severity": "CRITICAL",
    "attack_vector": "DDoS attack - 3 bins reporting critical failures within 5 minutes",
    "affected_bins": ["BIN-001", "BIN-042", "BIN-087"],
    "affected_zones": [1, 3, 5],
    "source_ips": ["192.168.1.100", "10.0.0.50"],
    "suspicious_tokens": ["token_hash_a1b2c3"],
    "evidence_json": {
      "critical_events_count": 3,
      "time_window_minutes": 5,
      "impossible_physics_detected": [
        "Fill decreased without collection on BIN-001",
        "GPS jump >100km in 5min on vehicle LORRY-02"
      ],
      "entropy_anomalies": ["BIN-042 fill readings show entropy 0.08 (spoofed constant)"]
    },
    "bayesian_probability": 0.92,
    "resolution_status": "investigating",
    "timestamp": "2026-05-14T10:30:00Z",
    "created_at": "2026-05-14T10:30:01Z"
  },
  "timestamp": "2026-05-14T15:00:00Z"
}
```

---

### 4. Get Network Activity Anomalies

**Endpoint**: `GET /anomalies/network-activity`

**Purpose**: Retrieve suspicious network patterns

**Query Parameters**:
```
- min_hours (int, default: 1): Look back N hours [1-24]
```

**Example Request**:
```bash
curl -X GET "http://localhost:8000/api/v1/anomalies/network-activity?min_hours=2" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response**:
```json
{
  "success": true,
  "data": [
    {
      "api_token_hash": "a1b2c3d4e5f6g7h8",
      "unique_ips_count": 5,
      "ips": ["192.168.1.1", "10.0.0.50", "203.0.113.5", "198.51.100.20", "192.0.2.100"],
      "request_count": 2500,
      "time_window_minutes": 60,
      "anomaly_flags": ["MULTIPLE_IPS", "HIGH_BURST"],
      "severity": "HIGH"
    }
  ],
  "timestamp": "2026-05-14T15:00:00Z",
  "total_count": 1
}
```

---

## Event Management Endpoints

### 1. Get Logged Special Events

**Endpoint**: `GET /events/list`

**Purpose**: Retrieve manually logged special events

**Query Parameters**:
```
- zone_id (int, optional): Filter by zone
- days (int, default: 30): Look back N days [1-365]
```

**Example Request**:
```bash
curl -X GET "http://localhost:8000/api/v1/events/list?zone_id=2&days=30" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response**:
```json
{
  "events": [
    {
      "id": 1,
      "event_name": "City Festival 2026",
      "zone_ids": [1, 2, 3],
      "start_time": "2026-05-09T09:00:00Z",
      "end_time": "2026-05-09T22:00:00Z",
      "event_type": "FESTIVAL",
      "description": "Annual city festival with heavy foot traffic",
      "created_by": "ops_admin",
      "created_at": "2026-05-08T14:00:00Z"
    },
    {
      "id": 2,
      "event_name": "Street Cleaning - Main Avenue",
      "zone_ids": [5],
      "start_time": "2026-05-10T07:00:00Z",
      "end_time": "2026-05-10T12:00:00Z",
      "event_type": "STREET_CLEANING",
      "description": "Scheduled street cleaning with additional waste pickup",
      "created_by": "ops_admin",
      "created_at": "2026-05-09T10:00:00Z"
    }
  ],
  "total_count": 2,
  "zone_id": 2,
  "days": 30
}
```

---

### 2. Log a New Special Event

**Endpoint**: `POST /events/log`

**Purpose**: Ops team marks a new special event

**Request Body**:
```json
{
  "event_name": "Concert - City Arena",
  "zone_ids": [1, 2, 4],
  "start_time": "2026-05-20T18:00:00Z",
  "end_time": "2026-05-20T23:00:00Z",
  "event_type": "CONCERT",
  "description": "Large concert event - expecting high waste generation"
}
```

**Example Request**:
```bash
curl -X POST "http://localhost:8000/api/v1/events/log" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "Concert - City Arena",
    "zone_ids": [1, 2, 4],
    "start_time": "2026-05-20T18:00:00Z",
    "end_time": "2026-05-20T23:00:00Z",
    "event_type": "CONCERT",
    "description": "Large concert event"
  }'
```

**Response**:
```json
{
  "success": true,
  "data": {
    "event": {
      "id": 3,
      "event_name": "Concert - City Arena",
      "zone_ids": [1, 2, 4],
      "start_time": "2026-05-20T18:00:00Z",
      "end_time": "2026-05-20T23:00:00Z",
      "event_type": "CONCERT",
      "created_by": "ops_admin",
      "created_at": "2026-05-14T15:30:00Z"
    },
    "correlations": {
      "anomalies_during_event_timeframe": 0,
      "suggestion": "No anomalies detected during this time - system ready for event"
    }
  },
  "timestamp": "2026-05-14T15:30:01Z"
}
```

**Allowed Event Types**:
- `FESTIVAL` - City festivals, fairs
- `CONCERT` - Music events, concerts
- `PROTEST` - Public protests, demonstrations
- `ROAD_CLOSURE` - Road closures affecting collection
- `NATURAL_DISASTER` - Floods, storms, earthquakes
- `STREET_CLEANING` - Street cleaning operations
- `SPORTS_EVENT` - Sports events, marathons
- `MARKET_DAY` - Market days, street markets
- `CONSTRUCTION` - Construction activity nearby
- `OTHER` - Other events

---

### 3. Get Auto-Detected Events

**Endpoint**: `GET /events/auto-detected`

**Purpose**: Retrieve system-automatically detected event-driven anomalies

**Query Parameters**:
```
- hours (int, default: 24): Look back N hours [1-720]
```

**Example Request**:
```bash
curl -X GET "http://localhost:8000/api/v1/events/auto-detected?hours=24" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response**:
```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "zone_id": 2,
      "detection_type": "CHAOTIC_DEMAND",
      "start_time": "2026-05-14T14:00:00Z",
      "end_time": "2026-05-14T16:00:00Z",
      "affected_bins_count": 12,
      "waste_volume_actual_kg": 2450.5,
      "waste_volume_forecasted_kg": 1200.0,
      "deviation_std_count": 3.2,
      "confidence_score": 0.85,
      "estimated_event_type": "FESTIVAL",
      "estimated_event_probability": {
        "FESTIVAL": 0.40,
        "ACCIDENT": 0.30,
        "CLEANING": 0.20,
        "OTHER": 0.10
      },
      "processed": false,
      "linked_special_event_id": null,
      "created_at": "2026-05-14T16:05:00Z"
    }
  ],
  "timestamp": "2026-05-14T15:00:00Z",
  "total_count": 1
}
```

---

### 4. Get Event Impact Analysis

**Endpoint**: `GET /events/{event_id}/impact`

**Purpose**: Detailed impact analysis of a logged event

**Path Parameters**:
```
- event_id (int): Event ID
```

**Example Request**:
```bash
curl -X GET "http://localhost:8000/api/v1/events/1/impact" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response**:
```json
{
  "success": true,
  "data": {
    "event_id": 1,
    "zones_analyzed": [1, 2, 3],
    "anomalies_detected_count": 12,
    "anomalies_high_severity_count": 3,
    "anomalies_critical_count": 0,
    "baseline_waste_kg": 1200.0,
    "actual_waste_kg": 1750.5,
    "waste_increase_pct": 45.87,
    "scheduled_collections_count": 30,
    "executed_collections_count": 28,
    "failed_collections_count": 2,
    "collection_execution_rate": 93.33,
    "bins_with_chaotic_fill": 8,
    "rapid_surge_incidents": 2,
    "ml_prediction_accuracy": 78.5,
    "analysis_notes": "Festival caused 45% waste increase, but collections handled well"
  },
  "timestamp": "2026-05-14T15:00:00Z"
}
```

---

## Anomaly Correlation Endpoints

### Get Correlated Anomalies with Context

**Endpoint**: `GET /anomalies/correlated`

**Purpose**: Retrieve anomaly with complete context (security incidents, events)

**Query Parameters**:
```
- anomaly_id (int, optional): Specific anomaly ID
- zone_id (int, optional): Zone filter
```

**Example Request**:
```bash
curl -X GET "http://localhost:8000/api/v1/anomalies/correlated?anomaly_id=100&zone_id=2" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

**Response**:
```json
{
  "success": true,
  "data": {
    "primary_anomaly": {
      "id": 100,
      "anomaly_type": "data_quality",
      "severity": "HIGH",
      "bin_id": "BIN-042",
      "zone_id": 2,
      "timestamp": "2026-05-14T14:30:00Z",
      "fill_level_anomaly": "entropy 0.08 (constant spoofed values)"
    },
    "related_security_incidents": [],
    "related_events": [
      {
        "id": 1,
        "event_name": "City Festival 2026",
        "event_type": "FESTIVAL",
        "overlap_hours": 1.5,
        "zones_overlap": [2]
      }
    ],
    "auto_detected_events": [
      {
        "id": 5,
        "detection_type": "CHAOTIC_DEMAND",
        "zone_id": 2,
        "confidence_score": 0.85
      }
    ],
    "recommended_actions": [
      "Anomaly correlates with City Festival (09:00-22:00) in zone 2",
      "No security incident detected",
      "Auto-detection flagged chaotic demand pattern (confidence: 0.85)",
      "High entropy anomaly on BIN-042 suggests sensor malfunction or spoofing",
      "Recommend: Check BIN-042 calibration after festival ends"
    ]
  },
  "timestamp": "2026-05-14T15:00:00Z"
}
```

---

## Response Models

### Common Response Wrapper
```json
{
  "success": boolean,
  "data": any,
  "error": string (optional),
  "timestamp": ISO8601 datetime,
  "total_count": integer (optional),
  "page": integer (optional),
  "page_size": integer (optional)
}
```

### Severity Levels
- `LOW` - Informational, minor issue
- `MEDIUM` - Should be reviewed, potential issue
- `HIGH` - Significant issue, action recommended
- `CRITICAL` - Urgent, immediate action required

### Resolution Status (Security Incidents)
- `open` - Newly detected, under investigation
- `investigating` - In progress
- `mitigated` - Temporary fix applied
- `confirmed` - Confirmed as real attack/incident
- `false_alarm` - Determined to be false positive

### Detection Types (Auto-Detected Events)
- `CHAOTIC_DEMAND` - Unusual waste volume pattern
- `RAPID_SURGE` - 5+ bins fill rapidly in short time
- `FORECAST_DEVIATION` - Actual deviates >3σ from forecast

---

## Error Handling

### HTTP Status Codes
- `200` - Successful request
- `400` - Bad request (invalid parameters)
- `401` - Unauthorized (invalid/missing token)
- `403` - Forbidden (insufficient permissions)
- `404` - Not found (resource doesn't exist)
- `429` - Rate limited (too many requests)
- `500` - Server error

### Error Response Example
```json
{
  "success": false,
  "data": null,
  "error": "Invalid severity filter value. Allowed: LOW, MEDIUM, HIGH, CRITICAL",
  "timestamp": "2026-05-14T15:00:00Z"
}
```

---

## Rate Limiting & Pagination

### Rate Limits
- **Global**: 1000 requests/hour per API token
- **Per-endpoint**: 100 requests/minute

### Pagination
Use `limit` and `offset` parameters where applicable:
```
GET /anomalies/security/recent?limit=50&offset=0
```

Headers included in response:
```
X-Total-Count: 150
X-Returned-Count: 50
X-Has-More: true
```

---

## WebSocket Streaming (Optional)

For real-time anomaly alerts in the dashboard:

**Connection**: `ws://localhost:8000/ws/anomalies/stream`

**Query Parameters**:
```
?types=security,events  // Subscribe to event types
?zone_ids=1,2,3         // Filter by zones
?severity=HIGH,CRITICAL // Filter by severity
```

**Example**:
```javascript
const ws = new WebSocket('ws://localhost:8000/ws/anomalies/stream?types=security,events&severity=CRITICAL');

ws.onmessage = (event) => {
  const anomaly = JSON.parse(event.data);
  console.log('New anomaly:', anomaly);
  // Update dashboard in real-time
};
```

**Message Format**:
```json
{
  "type": "security_incident | event_detected",
  "id": integer,
  "severity": "CRITICAL",
  "data": {...},
  "timestamp": "2026-05-14T15:00:00Z"
}
```

---

## Sample Responses

See `sample_responses/` directory:
- `security_dashboard.json` - Security dashboard view
- `events_dashboard.json` - Events dashboard view
- `combined_tab_view.json` - Combined "Security & Events" tab

---

## Authentication

All endpoints require authentication via Bearer token:

```
Authorization: Bearer YOUR_JWT_TOKEN
```

**Token obtained from**: `/auth/login` (via Keycloak or main auth service)

---

## Frontend Integration Checklist

- [ ] Implement Security Dashboard Tab
  - [ ] Display recent incidents list
  - [ ] Show dashboard summary KPIs
  - [ ] Link to incident details modal
  - [ ] Visualize affected zones/bins on map

- [ ] Implement Event Management UI
  - [ ] Event logging form
  - [ ] List of logged events
  - [ ] Auto-detected events suggestions
  - [ ] Event impact drill-down

- [ ] Implement Anomaly Correlation View
  - [ ] Show primary anomaly details
  - [ ] List related security incidents
  - [ ] Link to related events
  - [ ] Display recommended actions

- [ ] Connect to Backend APIs
  - [ ] Implement all GET endpoints
  - [ ] Implement POST for event logging
  - [ ] Add error handling
  - [ ] Implement pagination/filtering

- [ ] Optional: Real-time Updates
  - [ ] Connect WebSocket stream
  - [ ] Display live alerts
  - [ ] Update dashboard in real-time

---

**Questions?** Contact the backend team or check the `/api/v1/docs` endpoint for auto-generated Swagger documentation.
