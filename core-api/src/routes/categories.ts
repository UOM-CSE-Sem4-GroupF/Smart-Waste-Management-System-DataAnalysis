import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function categoriesRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/waste-categories
   */
  fastify.get("/waste-categories", async (_request, reply) => {
    const categories = await prisma.wasteCategory.findMany({
      orderBy: { id: "asc" },
    });
    return reply.send({ data: categories });
  });

  /**
   * GET /api/v1/waste-categories/:id
   */
  fastify.get<{ Params: { id: string } }>("/waste-categories/:id", async (request, reply) => {
    const id = parseInt(request.params.id, 10);
    const category = await prisma.wasteCategory.findUnique({ where: { id } });
    if (!category) return reply.code(404).send({ error: "Not Found" });
    return reply.send({ data: category });
  });

  /**
   * POST /api/v1/waste-categories
   */
  fastify.post<{ Body: any }>("/waste-categories", async (request, reply) => {
    try {
      const category = await prisma.wasteCategory.create({ data: request.body as any });
      return reply.code(201).send({ data: category });
    } catch (err: any) {
      if (err?.code === "P2002") return reply.code(409).send({ error: "Conflict", message: "A category with that name already exists." });
      throw err;
    }
  });

  /**
   * PATCH /api/v1/waste-categories/:id
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/waste-categories/:id", async (request, reply) => {
    const id = parseInt(request.params.id, 10);
    try {
      const category = await prisma.wasteCategory.update({
        where: { id },
        data: { ...(request.body as any), updated_at: new Date() },
      });
      return reply.send({ data: category });
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  /**
   * DELETE /api/v1/waste-categories/:id
   */
  fastify.delete<{ Params: { id: string } }>("/waste-categories/:id", async (request, reply) => {
    const id = parseInt(request.params.id, 10);
    try {
      await prisma.wasteCategory.delete({ where: { id } });
      return reply.code(204).send();
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });
}
