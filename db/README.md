# Database Setup (Member 2)

## Role

Provide database for all services

---

## Frameworks

- PostgreSQL
- Docker

---

## Quick Start (Exact Details)

Run all commands from this folder:

`Smart-Waste-Management-System-DataAnalysis/db`

### Start Database

```powershell
docker compose up -d
docker compose ps
```

Expected service/container:

- Service name: `postgres`
- Container name: `db-postgres-1`
- Image: `postgres:16-alpine`

### Database Configuration

These are defined in `docker-compose.yml`:

- Host: `localhost`
- Port: `5432`
- Database: `waste_management`
- Username: `waste_admin`
- Password: `waste_admin_password`

Connection string:

```text
postgresql://waste_admin:waste_admin_password@localhost:5432/waste_management
```

### Connect and Verify

Open psql in the running container:

```powershell
docker exec -it db-postgres-1 psql -U waste_admin -d waste_management
```

Inside psql, verify schema:

```sql
SELECT current_database(), current_user;
SELECT count(*) FROM information_schema.tables WHERE table_schema='public';
\dt
```

Expected table count from `init.sql`: `15`

### Important Behavior

- `init.sql` runs only on first initialization of the Postgres volume.
- If schema changes are made in `init.sql`, reinitialize:

```powershell
docker compose down -v
docker compose up -d
```

### Stop Database

```powershell
docker compose down
```

---

## Responsibilities

1. Create schema
2. Insert sample data
3. Provide DB connection

---

## Tables

- waste_categories
- bins
- vehicles
- bin_current_state
- route_plans

---

## Input

None

---

## Output

### SQL Script

init.sql

---

### Running Database

Accessible via docker-compose

---

## Deliverables

- docker-compose.yml
- init.sql
