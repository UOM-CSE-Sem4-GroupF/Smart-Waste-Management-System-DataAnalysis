import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function devicesRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/devices
   * Supports filtering by status and bin_id.
   * Used by F4 device monitoring to find offline or unassigned devices.
   */
  fastify.get<{ Querystring: { status?: string; bin_id?: string } }>(
    "/devices",
    async (request, reply) => {
      const { status, bin_id } = request.query;
      const devices = await prisma.device.findMany({
        where: {
          ...(status ? { status } : {}),
          ...(bin_id !== undefined
            ? bin_id === "null"
              ? { bin_id: null }
              : { bin_id }
            : {}),
        },
        include: {
          bin: {
            select: { id: true, cluster_id: true, waste_category_id: true },
          },
        },
        orderBy: { id: "asc" },
      });
      return reply.send({ data: devices });
    }
  );

  /**
   * GET /api/v1/devices/:id
   */
  fastify.get<{ Params: { id: string } }>("/devices/:id", async (request, reply) => {
    const { id } = request.params;
    const device = await prisma.device.findUnique({
      where: { id },
      include: {
        bin: {
          select: {
            id: true,
            cluster_id: true,
            waste_category_id: true,
            depth_cm: true,
          },
        },
      },
    });
    if (!device) return reply.code(404).send({ error: "Not Found" });
    return reply.send({ data: device });
  });

  /**
   * POST /api/v1/devices
   */
  fastify.post<{ Body: any }>("/devices", async (request, reply) => {
    try {
      const device = await prisma.device.create({ data: request.body });
      return reply.code(201).send({ data: device });
    } catch (err: any) {
      if (err?.code === "P2002") return reply.code(409).send({ error: "Conflict", message: "Device ID or bin assignment already exists." });
      if (err?.code === "P2003") return reply.code(400).send({ error: "Bad Request", message: "Bin does not exist." });
      throw err;
    }
  });

  /**
   * PATCH /api/v1/devices/:id
   * Used by Leshan/F4 to update firmware status, last_seen_at, config timestamps, etc.
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/devices/:id", async (request, reply) => {
    const { id } = request.params;
    try {
      const device = await prisma.device.update({
        where: { id },
        data: { ...request.body, updated_at: new Date() },
      });
      return reply.send({ data: device });
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  /**
   * DELETE /api/v1/devices/:id
   */
  fastify.delete<{ Params: { id: string } }>("/devices/:id", async (request, reply) => {
    const { id } = request.params;
    try {
      await prisma.device.delete({ where: { id } });
      return reply.code(204).send();
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });
}
