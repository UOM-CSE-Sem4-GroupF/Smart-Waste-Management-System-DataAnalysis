import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function performanceRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/model-performance
   * Supports filtering by model_name and promoted_to_prod.
   */
  fastify.get<{ Querystring: { model_name?: string; promoted?: string } }>(
    "/model-performance",
    async (request, reply) => {
      const { model_name, promoted } = request.query;
      const performance = await prisma.modelPerformance.findMany({
        where: {
          ...(model_name ? { model_name } : {}),
          ...(promoted === "true" ? { promoted_to_prod: true } : {}),
        },
        orderBy: { created_at: "desc" },
      });
      return reply.send({ data: performance });
    }
  );

  /**
   * GET /api/v1/model-performance/current/:model_name
   * Returns the currently promoted-to-production record for a model.
   * Used by the ML service on startup to identify the active model version.
   */
  fastify.get<{ Params: { model_name: string } }>(
    "/model-performance/current/:model_name",
    async (request, reply) => {
      const { model_name } = request.params;
      const perf = await prisma.modelPerformance.findFirst({
        where: { model_name, promoted_to_prod: true },
        orderBy: { promoted_at: "desc" },
      });
      if (!perf) {
        return reply.code(404).send({
          error: "Not Found",
          message: `No production model found for '${model_name}'.`,
        });
      }
      return reply.send({ data: perf });
    }
  );

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
   * Written by Airflow after each training run.
   */
  fastify.post<{ Body: any }>("/model-performance", async (request, reply) => {
    try {
      const perf = await prisma.modelPerformance.create({ data: request.body });
      return reply.code(201).send({ data: perf });
    } catch (err: any) {
      if (err?.code === "P2002") return reply.code(409).send({ error: "Conflict", message: "A record for this model_name + model_version already exists." });
      throw err;
    }
  });

  /**
   * PATCH /api/v1/model-performance/:id
   * Used by Airflow to promote a model to production or record replacement.
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/model-performance/:id", async (request, reply) => {
    const id = BigInt(request.params.id);
    try {
      const perf = await prisma.modelPerformance.update({
        where: { id },
        data: request.body,
      });
      return reply.send({ data: perf });
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  /**
   * DELETE /api/v1/model-performance/:id
   */
  fastify.delete<{ Params: { id: string } }>("/model-performance/:id", async (request, reply) => {
    const id = BigInt(request.params.id);
    try {
      await prisma.modelPerformance.delete({ where: { id } });
      return reply.code(204).send();
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });
}
