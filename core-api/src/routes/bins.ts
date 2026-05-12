import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function binsRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/bins
   * Returns active bins joined with their current state.
   * Supports filtering by: zone_id, cluster_id, status, waste_category_id
   */
  fastify.get<{
    Querystring: {
      zone_id?: string;
      cluster_id?: string;
      status?: string;
      waste_category_id?: string;
      page?: string;
      limit?: string;
    };
  }>("/bins", async (request, reply) => {
    const {
      zone_id,
      cluster_id,
      status,
      waste_category_id,
      page = "1",
      limit = "50",
    } = request.query;

    const pageNum = Math.max(1, parseInt(page, 10));
    const limitNum = Math.min(200, Math.max(1, parseInt(limit, 10)));
    const skip = (pageNum - 1) * limitNum;

    const where = {
      active: true,
      ...(cluster_id ? { cluster_id } : {}),
      ...(waste_category_id ? { waste_category_id: parseInt(waste_category_id, 10) } : {}),
      ...(zone_id ? { cluster: { zone_id: parseInt(zone_id, 10) } } : {}),
      ...(status ? { current_state: { status } } : {}),
    };

    const [bins, total] = await Promise.all([
      prisma.bin.findMany({
        where,
        include: {
          cluster: {
            select: {
              id: true,
              name: true,
              lat: true,
              lng: true,
              zone_id: true,
              zone: { select: { id: true, name: true, code: true } },
            },
          },
          waste_category: {
            select: {
              id: true,
              name: true,
              avg_kg_per_litre: true,
              colour_code: true,
              recyclable: true,
              special_handling: true,
            },
          },
          current_state: true,
        },
        orderBy: [{ current_state: { urgency_score: "desc" } }, { id: "asc" }],
        skip,
        take: limitNum,
      }),
      prisma.bin.count({ where }),
    ]);

    return reply.send({
      data: bins,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        pages: Math.ceil(total / limitNum),
      },
    });
  });

  /**
   * GET /api/v1/bins/:bin_id
   * Returns a single bin with its full current state and metadata.
   */
  fastify.get<{ Params: { bin_id: string } }>(
    "/bins/:bin_id",
    async (request, reply) => {
      const { bin_id } = request.params;

      const bin = await prisma.bin.findUnique({
        where: { id: bin_id },
        include: {
          cluster: { include: { zone: true } },
          waste_category: true,
          current_state: true,
          device: {
            select: {
              id: true,
              device_type: true,
              status: true,
              last_seen_at: true,
              battery_level_pct: true,
              firmware_current_version: true,
              firmware_target_version: true,
            },
          },
        },
      });

      if (!bin) {
        return reply.code(404).send({ error: "Not Found", message: `Bin '${bin_id}' does not exist.` });
      }

      return reply.send({ data: bin });
    }
  );

  /**
   * POST /api/v1/bins
   */
  fastify.post<{ Body: any }>("/bins", async (request, reply) => {
    try {
      const bin = await prisma.bin.create({ data: request.body });
      return reply.code(201).send({ data: bin });
    } catch (err: any) {
      if (err?.code === "P2002") return reply.code(409).send({ error: "Conflict", message: "Bin ID already exists." });
      if (err?.code === "P2003") return reply.code(400).send({ error: "Bad Request", message: "Cluster or waste category does not exist." });
      throw err;
    }
  });

  /**
   * PATCH /api/v1/bins/:id
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/bins/:id", async (request, reply) => {
    const { id } = request.params;
    try {
      const bin = await prisma.bin.update({
        where: { id },
        data: { ...request.body, updated_at: new Date() },
      });
      return reply.send({ data: bin });
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  /**
   * DELETE /api/v1/bins/:id
   */
  fastify.delete<{ Params: { id: string } }>("/bins/:id", async (request, reply) => {
    const { id } = request.params;
    try {
      await prisma.bin.delete({ where: { id } });
      return reply.code(204).send();
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  // --- BinCurrentState ---

  /**
   * GET /api/v1/bins/:bin_id/state
   * Returns the current telemetry state for a bin.
   * Used by dashboard and the route optimizer when building a solve request.
   */
  fastify.get<{ Params: { bin_id: string } }>(
    "/bins/:bin_id/state",
    async (request, reply) => {
      const { bin_id } = request.params;
      const state = await prisma.binCurrentState.findUnique({ where: { bin_id } });
      if (!state) {
        return reply.code(404).send({ error: "Not Found", message: `No state recorded yet for bin '${bin_id}'.` });
      }
      return reply.send({ data: state });
    }
  );

  /**
   * PATCH /api/v1/bins/:bin_id/state
   * Upserts the current state for a bin. Called by Flink on every sensor reading.
   */
  fastify.patch<{ Params: { bin_id: string }; Body: any }>(
    "/bins/:bin_id/state",
    async (request, reply) => {
      const { bin_id } = request.params;
      const state = await prisma.binCurrentState.upsert({
        where: { bin_id },
        update: { ...request.body, updated_at: new Date() },
        create: { ...request.body, bin_id },
      });
      return reply.send({ data: state });
    }
  );
}
