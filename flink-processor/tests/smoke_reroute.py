import logging
import sys
import types
from datetime import datetime, timedelta, timezone

# stub psycopg2 so route_store imports don't fail in this smoke test
sys.modules.setdefault("psycopg2", types.ModuleType("psycopg2"))
sys.modules.setdefault("psycopg2.pool", types.ModuleType("psycopg2.pool"))
sys.modules.setdefault("psycopg2.extras", types.ModuleType("psycopg2.extras"))

from processors.vehicle_deviation import VehicleDeviationProcessor
from reroute import compute_reroute


class DummyRouteStore:
    def load_route_plan(self, vehicle_id, job_id=None):
        class RP:
            route_plan_id = "rp-1"
            waypoints = [
                {"latitude": -33.865143, "longitude": 151.209900},
                {"latitude": -33.870000, "longitude": 151.210000},
            ]
        return RP()


def make_event(vehicle_id, lat, lon, ts):
    return {
        "vehicle_id": vehicle_id,
        "timestamp": ts.isoformat().replace("+00:00", "Z"),
        "latitude": lat,
        "longitude": lon,
    }


def main():
    logging.basicConfig(level=logging.INFO)
    route_store = DummyRouteStore()
    processor = VehicleDeviationProcessor(route_store)

    # place vehicle far from waypoints to trigger deviation
    t0 = datetime.now(timezone.utc)
    e1 = make_event("veh-1", -34.0, 150.0, t0)
    out1 = processor.process(e1)
    print("first process output:", out1)

    # second event after ALERT_DURATION_SECONDS + 10
    t1 = t0 + timedelta(seconds=processor.ALERT_DURATION_SECONDS + 10)
    e2 = make_event("veh-1", -34.0, 150.0, t1)
    out2 = processor.process(e2)
    print("second process output (alert):", out2)

    if out2 is not None:
        rp = route_store.load_route_plan("veh-1", out2.get("job_id"))
        reroute = compute_reroute(e2, rp)
        print("computed reroute waypoints:", reroute)


if __name__ == "__main__":
    main()
