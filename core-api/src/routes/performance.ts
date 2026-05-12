import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function performanceRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/model-performance
   */
  fastify.get<{ Querystring: { model_name?: string } }>("/model-performance", async (request, reply) => {
    const { model_name } = request.query;
    const performance = await prisma.modelPerformance.findMany({
      where: model_name ? { model_name } : {},
      orderBy: { created_at: "desc" },
    });
    return reply.send({ data: performance });
  });

  /**
   * GET /api/v1/model-performance/:id
   */
  fastify.get<{ Params: { id: string } }>("/model-performance/:id", async (request, reply) => {
    const id = BigInt(request.params.id);
    const perf = await prisma.modelPerformance.findUnique({ where: { id } });
    if (!perf) return reply.code(404).send({ error: "Not Found" });
    return reply.send({ data: perf });
  });

  /**
   * POST /api/v1/model-performance
   */
  fastify.post<{ Body: any }>("/model-performance", async (request, reply) => {
    const perf = await prisma.modelPerformance.create({
      data: request.body,
    });
    return reply.code(211).send({ data: perf });
  });

  /**
   * PATCH /api/v1/model-performance/:id
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/model-performance/:id", async (request, reply) => {
    const id = BigInt(request.params.id);
    const perf = await prisma.modelPerformance.update({
      where: { id },
      data: request.body,
    });
    return reply.send({ data: perf });
  });
}
