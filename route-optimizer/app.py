from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from models import SolveRequest, SolveResponse
from solver import InvalidRequestError, NoFeasibleSolutionError, SolverTimeoutError, solve

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("route-optimizer")

app = FastAPI(
    title="Route Optimizer",
    description=(
        "Capacitated VRP with Time Windows (CVRPTW) optimization service "
        "for waste collection scheduling. Called exclusively by the scheduler service."
    ),
    version="1.0.0",
)


@app.get("/health", tags=["ops"])
def health() -> dict:
    """Health check — no auth required."""
    return {"status": "ok", "service": "route-optimizer", "version": "1.0.0"}


@app.get("/ready", tags=["ops"])
def ready() -> dict:
    """Readiness check — stateless service is ready as soon as it starts."""
    return {"status": "ready", "service": "route-optimizer", "version": "1.0.0"}


@app.post(
    "/internal/route-optimizer/solve",
    response_model=SolveResponse,
    tags=["routing"],
    summary="Solve CVRPTW for a collection job",
    responses={
        400: {"description": "Invalid request (e.g. unsupported waste category)"},
        422: {"description": "No feasible solution (e.g. weight exceeds all vehicles)"},
        504: {"description": "Solver timeout — no solution within time limit"},
    },
)
def solve_route(request: SolveRequest):
    """
    Solve the Capacitated VRP with Time Windows for the given collection job.

    Returns an optimised ordered route for the assigned vehicle.
    Falls back to nearest-neighbour when OR-Tools cannot find a solution
    within the configured time limit.
    """
    logger.info(
        "solve request job_id=%s job_type=%s clusters=%d bins=%d vehicles=%d",
        request.job_id,
        request.job_type,
        len(request.clusters),
        len(request.bins),
        len(request.available_vehicles),
    )

    try:
        response = solve(request)
    except InvalidRequestError as exc:
        logger.warning("invalid request job_id=%s: %s", request.job_id, exc)
        return JSONResponse(
            status_code=400,
            content={"error": "INVALID_REQUEST", "detail": str(exc)},
        )
    except NoFeasibleSolutionError as exc:
        logger.warning("no feasible solution job_id=%s: %s", request.job_id, exc)
        return JSONResponse(
            status_code=422,
            content={"error": "NO_FEASIBLE_SOLUTION", "detail": str(exc)},
        )
    except SolverTimeoutError as exc:
        logger.warning("solver timeout job_id=%s: %s", request.job_id, exc)
        return JSONResponse(
            status_code=504,
            content={"error": "SOLVER_TIMEOUT", "detail": str(exc)},
        )

    logger.info(
        "solved job_id=%s method=%s vehicle=%s waypoints=%d distance_km=%.2f minutes=%d",
        response.job_id,
        response.method,
        response.vehicle_id,
        len(response.waypoints),
        response.total_distance_km,
        response.estimated_minutes,
    )
    return response