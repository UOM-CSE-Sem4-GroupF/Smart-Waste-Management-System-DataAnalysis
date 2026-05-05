import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function binsRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/bins
   * Returns all bins joined with their current state.
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

    const bins = await prisma.bin.findMany({
      where: {
        active: true,
        ...(cluster_id ? { cluster_id } : {}),
        ...(waste_category_id
          ? { waste_category_id: parseInt(waste_category_id, 10) }
          : {}),
        ...(zone_id
          ? { cluster: { zone_id: parseInt(zone_id, 10) } }
          : {}),
        ...(status ? { current_state: { status } } : {}),
      },
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
      orderBy: [
        { current_state: { urgency_score: "desc" } },
        { id: "asc" },
      ],
      skip,
      take: limitNum,
    });

    const total = await prisma.bin.count({
      where: {
        active: true,
        ...(cluster_id ? { cluster_id } : {}),
        ...(waste_category_id
          ? { waste_category_id: parseInt(waste_category_id, 10) }
          : {}),
        ...(zone_id
          ? { cluster: { zone_id: parseInt(zone_id, 10) } }
          : {}),
        ...(status ? { current_state: { status } } : {}),
      },
    });

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
          cluster: {
            include: {
              zone: true,
            },
          },
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
        return reply.code(404).send({
          error: "Not Found",
          message: `Bin '${bin_id}' does not exist.`,
        });
      }

      return reply.send({ data: bin });
    }
  );
}
