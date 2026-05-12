import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function zonesRoutes(fastify: FastifyInstance) {
  // --- CityZone CRUD ---

  /**
   * GET /api/v1/city-zones
   */
  fastify.get("/city-zones", async (_request, reply) => {
    const zones = await prisma.cityZone.findMany({
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
    const zone = await prisma.cityZone.create({
      data: request.body,
    });
    return reply.code(211).send({ data: zone });
  });

  /**
   * PATCH /api/v1/city-zones/:id
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/city-zones/:id", async (request, reply) => {
    const id = parseInt(request.params.id, 10);
    const zone = await prisma.cityZone.update({
      where: { id },
      data: request.body,
    });
    return reply.send({ data: zone });
  });

  /**
   * DELETE /api/v1/city-zones/:id
   */
  fastify.delete<{ Params: { id: string } }>("/city-zones/:id", async (request, reply) => {
    const id = parseInt(request.params.id, 10);
    await prisma.cityZone.delete({ where: { id } });
    return reply.code(204).send();
  });

  // --- ZoneSnapshot CRUD ---

  /**
   * GET /api/v1/zone-snapshots
   */
  fastify.get<{ Querystring: { zone_id?: string } }>("/zone-snapshots", async (request, reply) => {
    const { zone_id } = request.query;
    const snapshots = await prisma.zoneSnapshot.findMany({
      where: zone_id ? { zone_id: parseInt(zone_id, 10) } : {},
      orderBy: { snapshot_at: "desc" },
      take: 100,
    });
    return reply.send({ data: snapshots });
  });

  /**
   * POST /api/v1/zone-snapshots
   */
  fastify.post<{ Body: any }>("/zone-snapshots", async (request, reply) => {
    const snapshot = await prisma.zoneSnapshot.create({
      data: request.body,
    });
    return reply.code(211).send({ data: snapshot });
  });
}
