import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function zonesRoutes(fastify: FastifyInstance) {
  // --- CityZone CRUD ---

  /**
   * GET /api/v1/city-zones
   * Supports filtering by active (default: true only).
   */
  fastify.get<{ Querystring: { active?: string } }>("/city-zones", async (request, reply) => {
    const showAll = request.query.active === "all";
    const zones = await prisma.cityZone.findMany({
      where: showAll ? {} : { active: true },
      orderBy: { id: "asc" },
    });
    return reply.send({ data: zones });
  });

  /**
   * GET /api/v1/city-zones/:id
   */
  fastify.get<{ Params: { id: string } }>("/city-zones/:id", async (request, reply) => {
    const id = parseInt(request.params.id, 10);
    const zone = await prisma.cityZone.findUnique({ where: { id } });
    if (!zone) return reply.code(404).send({ error: "Not Found" });
    return reply.send({ data: zone });
  });

  /**
   * POST /api/v1/city-zones
   */
  fastify.post<{ Body: any }>("/city-zones", async (request, reply) => {
    try {
      const zone = await prisma.cityZone.create({ data: request.body as any });
      return reply.code(201).send({ data: zone });
    } catch (err: any) {
      if (err?.code === "P2002") return reply.code(409).send({ error: "Conflict", message: "Zone code already exists." });
      throw err;
    }
  });

  /**
   * PATCH /api/v1/city-zones/:id
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/city-zones/:id", async (request, reply) => {
    const id = parseInt(request.params.id, 10);
    try {
      const zone = await prisma.cityZone.update({
        where: { id },
        data: { ...(request.body as any), updated_at: new Date() },
      });
      return reply.send({ data: zone });
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  /**
   * DELETE /api/v1/city-zones/:id
   */
  fastify.delete<{ Params: { id: string } }>("/city-zones/:id", async (request, reply) => {
    const id = parseInt(request.params.id, 10);
    try {
      await prisma.zoneSnapshot.deleteMany({ where: { zone_id: id } });
      await prisma.cityZone.delete({ where: { id } });
      return reply.code(204).send();
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  // --- ZoneSnapshot ---

  /**
   * GET /api/v1/zone-snapshots
   * Returns recent snapshots, optionally filtered by zone_id.
   * Supports limit (default 100, max 500).
   */
  fastify.get<{ Querystring: { zone_id?: string; limit?: string } }>(
    "/zone-snapshots",
    async (request, reply) => {
      const { zone_id, limit = "100" } = request.query;
      const limitNum = Math.min(500, Math.max(1, parseInt(limit, 10)));
      const snapshots = await prisma.zoneSnapshot.findMany({
        where: zone_id ? { zone_id: parseInt(zone_id, 10) } : {},
        orderBy: { snapshot_at: "desc" },
        take: limitNum,
      });
      return reply.send({ data: snapshots });
    }
  );

  /**
   * POST /api/v1/zone-snapshots
   * Written by Flink on every sliding-window tick.
   */
  fastify.post<{ Body: any }>("/zone-snapshots", async (request, reply) => {
    const snapshot = await prisma.zoneSnapshot.create({ data: request.body as any });
    return reply.code(201).send({ data: snapshot });
  });
}
