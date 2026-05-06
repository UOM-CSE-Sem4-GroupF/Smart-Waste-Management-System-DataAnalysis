import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function metadataRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/zones
   * Returns all active city zones.
   */
  fastify.get("/zones", async (_request, reply) => {
    const zones = await prisma.cityZone.findMany({
      where: { active: true },
      orderBy: { id: "asc" },
    });
    return reply.send({ data: zones });
  });

  /**
   * GET /api/v1/zones/:zone_id/summary
   * Returns the latest zone snapshot and current bin stats for a zone.
   */
  fastify.get<{ Params: { zone_id: string } }>(
    "/zones/:zone_id/summary",
    async (request, reply) => {
      const zone_id = parseInt(request.params.zone_id, 10);

      const zone = await prisma.cityZone.findUnique({
        where: { id: zone_id },
      });

      if (!zone) {
        return reply.code(404).send({
          error: "Not Found",
          message: `Zone ${zone_id} does not exist.`,
        });
      }

      // Latest zone snapshot from Flink
      const latestSnapshot = await prisma.zoneSnapshot.findFirst({
        where: { zone_id },
        orderBy: { snapshot_at: "desc" },
      });

      // Live counts from bin_current_state
      const binStateCounts = await prisma.binCurrentState.groupBy({
        by: ["status"],
        where: { zone_id },
        _count: { status: true },
        orderBy: { status: "asc" },
      });

      const statusSummary = Object.fromEntries(
        binStateCounts.map((r) => [r.status, r._count.status])
      );

      return reply.send({
        data: {
          zone,
          latest_snapshot: latestSnapshot,
          live_bin_status_counts: statusSummary,
        },
      });
    }
  );

  /**
   * GET /api/v1/waste-categories
   * Returns all supported waste categories with density metadata.
   */
  fastify.get("/waste-categories", async (_request, reply) => {
    const categories = await prisma.wasteCategory.findMany({
      orderBy: { id: "asc" },
    });
    return reply.send({ data: categories });
  });

  /**
   * GET /api/v1/vehicles
   * Returns all active vehicles with their supported waste categories.
   */
  fastify.get<{
    Querystring: { status?: string; waste_category?: string };
  }>("/vehicles", async (request, reply) => {
    const { status, waste_category } = request.query;

    const vehicles = await prisma.vehicle.findMany({
      where: {
        active: true,
        ...(status ? { status } : {}),
        ...(waste_category
          ? {
              waste_categories: {
                some: { category: { name: waste_category } },
              },
            }
          : {}),
      },
      include: {
        waste_categories: {
          include: {
            category: {
              select: { id: true, name: true, colour_code: true },
            },
          },
        },
      },
      orderBy: { id: "asc" },
    });

    return reply.send({ data: vehicles });
  });
}
