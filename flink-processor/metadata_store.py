import logging
from typing import Any, Dict, Optional
from threading import Lock

import psycopg2
from psycopg2 import pool, sql

from config import Settings

logger = logging.getLogger(__name__)


class MetadataStoreError(Exception):
    """Raised when metadata store operations fail."""
    pass


class MetadataStore:
    """
    PostgreSQL metadata store for bin enrichment.

    Schema mapping (db/init.sql):
    - bins.id, bins.zone_id, bins.lat, bins.lng, bins.volume_litres, bins.waste_category_id, bins.active
    - city_zones.id, city_zones.active
    - waste_categories.id, waste_categories.avg_kg_per_litre

    Uses connection pooling and caching to efficiently fetch:
    - Bin metadata (zone_id, location, volume, waste category)
    - Waste category properties (avg_kg_per_litre)
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._pool: Optional[pool.SimpleConnectionPool] = None
        self._pool_lock = Lock()
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = Lock()
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize PostgreSQL connection pool."""
        try:
            with self._pool_lock:
                if self._pool is not None:
                    return

                self._pool = pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=5,
                    host=self.settings.postgres_host,
                    port=self.settings.postgres_port,
                    database=self.settings.postgres_db,
                    user=self.settings.postgres_user,
                    password=self.settings.postgres_password,
                )
                logger.info(
                    "✓ PostgreSQL connection pool initialized for %s@%s:%d",
                    self.settings.postgres_user,
                    self.settings.postgres_host,
                    self.settings.postgres_port,
                )
        except psycopg2.Error as e:
            logger.error(f"✗ Failed to initialize connection pool: {e}")
            raise MetadataStoreError(f"Connection pool initialization failed: {e}") from e

    def get_bin_metadata(self, bin_id: str) -> Optional[Dict[str, Any]]:
        """
        Get enriched bin metadata from PostgreSQL.
        
        Returns dict with fields:
        {
            "bin_id": "BIN-001",
            "zone_id": 1,
            "latitude": 6.927079,
            "longitude": 79.861244,
            "volume_litres": 240.00,
            "waste_category_id": 1,
            "avg_kg_per_litre": 0.900
        }
        
        Args:
            bin_id: The bin ID to look up
            
        Returns:
            Dict with metadata if found, None if not found or error
        """
        if not bin_id or not isinstance(bin_id, str):
            logger.warning(f"Invalid bin_id: {bin_id}")
            return None

        # Check cache first
        with self._cache_lock:
            if bin_id in self._cache:
                logger.debug(f"Cache hit for bin_id {bin_id}")
                return self._cache[bin_id]

        try:
            conn = self._pool.getconn() if self._pool else None
            if conn is None:
                logger.error("No connection available from pool")
                return None

            try:
                cur = conn.cursor()

                # Keep SQL fully aligned with db/init.sql table relationships.
                query = sql.SQL("""
                    SELECT
                        b.id AS bin_id,
                        b.zone_id,
                        b.lat AS latitude,
                        b.lng AS longitude,
                        b.volume_litres,
                        b.waste_category_id,
                        wc.avg_kg_per_litre
                    FROM bins b
                                        INNER JOIN city_zones cz ON b.zone_id = cz.id
                    LEFT JOIN waste_categories wc ON b.waste_category_id = wc.id
                                        WHERE b.id = %s
                                            AND b.active = TRUE
                                            AND cz.active = TRUE
                """)

                cur.execute(query, (bin_id,))
                row = cur.fetchone()
                cur.close()

                if row is None:
                    logger.warning(f"Bin not found or inactive: {bin_id}")
                    return None

                metadata = {
                    "bin_id": row[0],
                    "zone_id": row[1],
                    "latitude": float(row[2]) if row[2] else None,
                    "longitude": float(row[3]) if row[3] else None,
                    "volume_litres": float(row[4]) if row[4] else None,
                    "waste_category_id": row[5],
                    "avg_kg_per_litre": float(row[6]) if row[6] else None,
                }

                # Cache the result
                with self._cache_lock:
                    self._cache[bin_id] = metadata

                logger.debug(f"✓ Fetched metadata for bin_id {bin_id}")
                return metadata

            finally:
                if self._pool:
                    self._pool.putconn(conn)

        except psycopg2.Error as e:
            logger.error(f"✗ Database error fetching bin {bin_id}: {e}")
            return None

    def close(self) -> None:
        """Close all connections in the pool."""
        with self._pool_lock:
            if self._pool:
                self._pool.closeall()
                self._pool = None
                logger.info("✓ Connection pool closed")

    def __del__(self) -> None:
        """Cleanup on deletion."""
        self.close()
