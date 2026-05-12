import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function routePlansRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/route-plans
   */
  fastify.get<{ Querystring: { date?: string; vehicle_id?: string } }>(
    "/route-plans",
    async (request, reply) => {
      const { date, vehicle_id } = request.query;
      const plans = await prisma.routePlan.findMany({
        where: {
          ...(date ? { valid_for_date: new Date(date) } : {}),
          ...(vehicle_id ? { vehicle_id } : {}),
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
    const plan = await prisma.routePlan.findUnique({ where: { id } });
    if (!plan) return reply.code(404).send({ error: "Not Found" });
    return reply.send({ data: plan });
  });

  /**
   * POST /api/v1/route-plans
   */
  fastify.post<{ Body: any }>("/route-plans", async (request, reply) => {
    const plan = await prisma.routePlan.create({
      data: request.body,
    });
    return reply.code(211).send({ data: plan });
  });

  /**
   * PATCH /api/v1/route-plans/:id
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/route-plans/:id", async (request, reply) => {
    const { id } = request.params;
    const plan = await prisma.routePlan.update({
      where: { id },
      data: request.body,
    });
    return reply.send({ data: plan });
  });

  /**
   * DELETE /api/v1/route-plans/:id
   */
  fastify.delete<{ Params: { id: string } }>("/route-plans/:id", async (request, reply) => {
    const { id } = request.params;
    await prisma.routePlan.delete({ where: { id } });
    return reply.code(204).send();
  });
}
