import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function routePlansRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/route-plans
   * Supports filtering by date, vehicle_id, and status.
   * Used by F3 orchestrator to load the active plan for a vehicle.
   */
  fastify.get<{ Querystring: { date?: string; vehicle_id?: string; status?: string } }>(
    "/route-plans",
    async (request, reply) => {
      const { date, vehicle_id, status } = request.query;
      const plans = await prisma.routePlan.findMany({
        where: {
          ...(date ? { valid_for_date: new Date(date) } : {}),
          ...(vehicle_id ? { vehicle_id } : {}),
          ...(status ? { status } : {}),
        },
        include: {
          vehicle: {
            select: { id: true, registration: true, vehicle_type: true, max_cargo_kg: true, status: true },
          },
        },
        orderBy: { created_at: "desc" },
      });
      return reply.send({ data: plans });
    }
  );

  /**
   * GET /api/v1/route-plans/:id
   */
  fastify.get<{ Params: { id: string } }>("/route-plans/:id", async (request, reply) => {
    const { id } = request.params;
    const plan = await prisma.routePlan.findUnique({
      where: { id },
      include: {
        vehicle: {
          include: { waste_categories: { include: { category: true } } },
        },
      },
    });
    if (!plan) return reply.code(404).send({ error: "Not Found" });
    return reply.send({ data: plan });
  });

  /**
   * POST /api/v1/route-plans
   * Written by the route optimizer after each solve.
   */
  fastify.post<{ Body: any }>("/route-plans", async (request, reply) => {
    try {
      const plan = await prisma.routePlan.create({ data: request.body as any });
      return reply.code(201).send({ data: plan });
    } catch (err: any) {
      if (err?.code === "P2003") return reply.code(400).send({ error: "Bad Request", message: "Vehicle or zone does not exist." });
      throw err;
    }
  });

  /**
   * PATCH /api/v1/route-plans/:id
   * Used to update status (planned → active → completed / superseded / cancelled).
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/route-plans/:id", async (request, reply) => {
    const { id } = request.params;
    try {
      const plan = await prisma.routePlan.update({
        where: { id },
        data: request.body as any,
      });
      return reply.send({ data: plan });
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  /**
   * DELETE /api/v1/route-plans/:id
   */
  fastify.delete<{ Params: { id: string } }>("/route-plans/:id", async (request, reply) => {
    const { id } = request.params;
    try {
      await prisma.routePlan.delete({ where: { id } });
      return reply.code(204).send();
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });
}
