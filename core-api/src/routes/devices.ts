import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function devicesRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/devices
   */
  fastify.get("/devices", async (_request, reply) => {
    const devices = await prisma.device.findMany({
      orderBy: { id: "asc" },
    });
    return reply.send({ data: devices });
  });

  /**
   * GET /api/v1/devices/:id
   */
  fastify.get<{ Params: { id: string } }>("/devices/:id", async (request, reply) => {
    const { id } = request.params;
    const device = await prisma.device.findUnique({ where: { id } });
    if (!device) return reply.code(404).send({ error: "Not Found" });
    return reply.send({ data: device });
  });

  /**
   * POST /api/v1/devices
   */
  fastify.post<{ Body: any }>("/devices", async (request, reply) => {
    const device = await prisma.device.create({
      data: request.body,
    });
    return reply.code(211).send({ data: device });
  });

  /**
   * PATCH /api/v1/devices/:id
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/devices/:id", async (request, reply) => {
    const { id } = request.params;
    const device = await prisma.device.update({
      where: { id },
      data: request.body,
    });
    return reply.send({ data: device });
  });

  /**
   * DELETE /api/v1/devices/:id
   */
  fastify.delete<{ Params: { id: string } }>("/devices/:id", async (request, reply) => {
    const { id } = request.params;
    await prisma.device.delete({ where: { id } });
    return reply.code(204).send();
  });
}
