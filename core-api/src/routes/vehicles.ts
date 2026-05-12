import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function vehiclesRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/vehicles
   * Supports filtering by status and active.
   * Default: only active vehicles (active=true).
   * Pass active=all to include decommissioned ones.
   */
  fastify.get<{ Querystring: { status?: string; active?: string } }>(
    "/vehicles",
    async (request, reply) => {
      const { status, active } = request.query;
      const showAll = active === "all";
      const vehicles = await prisma.vehicle.findMany({
        where: {
          ...(showAll ? {} : { active: true }),
          ...(status ? { status } : {}),
        },
        include: { waste_categories: { include: { category: true } } },
        orderBy: { id: "asc" },
      });
      return reply.send({ data: vehicles });
    }
  );

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
    try {
      const vehicle = await prisma.vehicle.create({ data: request.body });
      return reply.code(201).send({ data: vehicle });
    } catch (err: any) {
      if (err?.code === "P2002") return reply.code(409).send({ error: "Conflict", message: "Vehicle ID or registration already exists." });
      throw err;
    }
  });

  /**
   * PATCH /api/v1/vehicles/:id
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/vehicles/:id", async (request, reply) => {
    const { id } = request.params;
    try {
      const vehicle = await prisma.vehicle.update({
        where: { id },
        data: { ...request.body, updated_at: new Date() },
      });
      return reply.send({ data: vehicle });
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  /**
   * DELETE /api/v1/vehicles/:id
   */
  fastify.delete<{ Params: { id: string } }>("/vehicles/:id", async (request, reply) => {
    const { id } = request.params;
    try {
      await prisma.vehicle.delete({ where: { id } });
      return reply.code(204).send();
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  // --- VehicleWasteCategory ---

  /**
   * GET /api/v1/vehicles/:id/categories
   * Returns the waste categories this vehicle supports.
   */
  fastify.get<{ Params: { id: string } }>(
    "/vehicles/:id/categories",
    async (request, reply) => {
      const { id } = request.params;
      const vehicle = await prisma.vehicle.findUnique({ where: { id } });
      if (!vehicle) return reply.code(404).send({ error: "Not Found" });
      const categories = await prisma.vehicleWasteCategory.findMany({
        where: { vehicle_id: id },
        include: { category: true },
      });
      return reply.send({ data: categories.map((r) => r.category) });
    }
  );

  /**
   * POST /api/v1/vehicles/:id/categories
   */
  fastify.post<{ Params: { id: string }; Body: { category_id: number } }>(
    "/vehicles/:id/categories",
    async (request, reply) => {
      const { id } = request.params;
      const { category_id } = request.body;
      try {
        const mapping = await prisma.vehicleWasteCategory.create({
          data: { vehicle_id: id, category_id },
        });
        return reply.code(201).send({ data: mapping });
      } catch (err: any) {
        if (err?.code === "P2002") return reply.code(409).send({ error: "Conflict", message: "Category already assigned to this vehicle." });
        if (err?.code === "P2003") return reply.code(400).send({ error: "Bad Request", message: "Vehicle or category does not exist." });
        throw err;
      }
    }
  );

  /**
   * DELETE /api/v1/vehicles/:id/categories/:category_id
   */
  fastify.delete<{ Params: { id: string; category_id: string } }>(
    "/vehicles/:id/categories/:category_id",
    async (request, reply) => {
      const { id, category_id } = request.params;
      try {
        await prisma.vehicleWasteCategory.delete({
          where: {
            vehicle_id_category_id: {
              vehicle_id: id,
              category_id: parseInt(category_id, 10),
            },
          },
        });
        return reply.code(204).send();
      } catch (err: any) {
        if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
        throw err;
      }
    }
  );
}
