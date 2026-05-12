import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function vehiclesRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/vehicles
   */
  fastify.get("/vehicles", async (_request, reply) => {
    const vehicles = await prisma.vehicle.findMany({
      include: { waste_categories: { include: { category: true } } },
      orderBy: { id: "asc" },
    });
    return reply.send({ data: vehicles });
  });

  /**
   * GET /api/v1/vehicles/:id
   */
  fastify.get<{ Params: { id: string } }>("/vehicles/:id", async (request, reply) => {
    const { id } = request.params;
    const vehicle = await prisma.vehicle.findUnique({
      where: { id },
      include: { waste_categories: { include: { category: true } } },
    });
    if (!vehicle) return reply.code(404).send({ error: "Not Found" });
    return reply.send({ data: vehicle });
  });

  /**
   * POST /api/v1/vehicles
   */
  fastify.post<{ Body: any }>("/vehicles", async (request, reply) => {
    const vehicle = await prisma.vehicle.create({
      data: request.body,
    });
    return reply.code(211).send({ data: vehicle });
  });

  /**
   * PATCH /api/v1/vehicles/:id
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/vehicles/:id", async (request, reply) => {
    const { id } = request.params;
    const vehicle = await prisma.vehicle.update({
      where: { id },
      data: request.body,
    });
    return reply.send({ data: vehicle });
  });

  /**
   * DELETE /api/v1/vehicles/:id
   */
  fastify.delete<{ Params: { id: string } }>("/vehicles/:id", async (request, reply) => {
    const { id } = request.params;
    await prisma.vehicle.delete({ where: { id } });
    return reply.code(204).send();
  });

  // --- VehicleWasteCategory CRUD ---

  /**
   * POST /api/v1/vehicles/:id/categories
   */
  fastify.post<{ Params: { id: string }; Body: { category_id: number } }>(
    "/vehicles/:id/categories",
    async (request, reply) => {
      const { id } = request.params;
      const { category_id } = request.body;
      const mapping = await prisma.vehicleWasteCategory.create({
        data: { vehicle_id: id, category_id },
      });
      return reply.code(211).send({ data: mapping });
    }
  );

  /**
   * DELETE /api/v1/vehicles/:id/categories/:category_id
   */
  fastify.delete<{ Params: { id: string; category_id: string } }>(
    "/vehicles/:id/categories/:category_id",
    async (request, reply) => {
      const { id, category_id } = request.params;
      await prisma.vehicleWasteCategory.delete({
        where: {
          vehicle_id_category_id: {
            vehicle_id: id,
            category_id: parseInt(category_id, 10),
          },
        },
      });
      return reply.code(204).send();
    }
  );
}
