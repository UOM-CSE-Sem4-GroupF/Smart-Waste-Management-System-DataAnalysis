import { FastifyInstance } from "fastify";
import { prisma } from "../db";

export async function clustersRoutes(fastify: FastifyInstance) {
  /**
   * GET /api/v1/clusters
   * Returns active bin clusters, optionally filtered by zone_id.
   */
  fastify.get<{
    Querystring: { zone_id?: string; page?: string; limit?: string };
  }>("/clusters", async (request, reply) => {
    const { zone_id, page = "1", limit = "50" } = request.query;

    const pageNum = Math.max(1, parseInt(page, 10));
    const limitNum = Math.min(200, Math.max(1, parseInt(limit, 10)));
    const skip = (pageNum - 1) * limitNum;

    const where = {
      active: true,
      ...(zone_id ? { zone_id: parseInt(zone_id, 10) } : {}),
    };

    const [clusters, total] = await Promise.all([
      prisma.binCluster.findMany({
        where,
        include: {
          zone: { select: { id: true, name: true, code: true } },
          bins: {
            where: { active: true },
            select: { id: true, waste_category_id: true, volume_litres: true },
          },
        },
        orderBy: { id: "asc" },
        skip,
        take: limitNum,
      }),
      prisma.binCluster.count({ where }),
    ]);

    return reply.send({
      data: clusters,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        pages: Math.ceil(total / limitNum),
      },
    });
  });

  /**
   * GET /api/v1/clusters/:cluster_id
   * Returns a cluster with all its active bins and their current states.
   */
  fastify.get<{ Params: { cluster_id: string } }>(
    "/clusters/:cluster_id",
    async (request, reply) => {
      const { cluster_id } = request.params;

      const cluster = await prisma.binCluster.findUnique({
        where: { id: cluster_id },
        include: {
          zone: true,
          bins: {
            where: { active: true },
            include: {
              waste_category: {
                select: {
                  id: true,
                  name: true,
                  avg_kg_per_litre: true,
                  colour_code: true,
                  special_handling: true,
                },
              },
              current_state: true,
            },
          },
        },
      });

      if (!cluster) {
        return reply.code(404).send({ error: "Not Found", message: `Cluster '${cluster_id}' does not exist.` });
      }

      return reply.send({ data: cluster });
    }
  );

  /**
   * GET /api/v1/clusters/:cluster_id/snapshot
   * Returns an urgency summary for a cluster.
   * Called by F3 orchestrator during job planning to assess weight and urgency.
   */
  fastify.get<{ Params: { cluster_id: string } }>(
    "/clusters/:cluster_id/snapshot",
    async (request, reply) => {
      const { cluster_id } = request.params;

      const cluster = await prisma.binCluster.findUnique({
        where: { id: cluster_id },
        select: {
          id: true,
          name: true,
          lat: true,
          lng: true,
          zone_id: true,
          bins: {
            where: { active: true },
            include: {
              current_state: true,
              waste_category: {
                select: { name: true, avg_kg_per_litre: true, special_handling: true },
              },
            },
          },
        },
      });

      if (!cluster) {
        return reply.code(404).send({ error: "Not Found", message: `Cluster '${cluster_id}' does not exist.` });
      }

      const binSnapshots = cluster.bins.map((bin) => {
        const state = bin.current_state;
        return {
          bin_id: bin.id,
          waste_category: bin.waste_category.name,
          special_handling: bin.waste_category.special_handling,
          fill_level_pct: state ? Number(state.fill_level_pct) : null,
          urgency_score: state?.urgency_score ?? 0,
          status: state?.status ?? "unknown",
          estimated_weight_kg: state ? Number(state.estimated_weight_kg ?? 0) : 0,
          predicted_full_at: state?.predicted_full_at ?? null,
        };
      });

      const urgentBins = binSnapshots.filter((b) => b.status === "urgent" || b.status === "critical");
      const maxUrgencyScore = Math.max(...binSnapshots.map((b) => b.urgency_score), 0);
      const totalEstimatedWeightKg = binSnapshots.reduce((sum, b) => sum + (b.estimated_weight_kg ?? 0), 0);
      const hasSpecialHandling = binSnapshots.some((b) => b.special_handling);

      return reply.send({
        data: {
          cluster_id: cluster.id,
          cluster_name: cluster.name,
          lat: Number(cluster.lat),
          lng: Number(cluster.lng),
          zone_id: cluster.zone_id,
          total_bins: cluster.bins.length,
          urgent_bin_count: urgentBins.length,
          max_urgency_score: maxUrgencyScore,
          total_estimated_weight_kg: Math.round(totalEstimatedWeightKg * 100) / 100,
          has_special_handling: hasSpecialHandling,
          bins: binSnapshots,
        },
      });
    }
  );

  /**
   * POST /api/v1/clusters
   */
  fastify.post<{ Body: any }>("/clusters", async (request, reply) => {
    try {
      const cluster = await prisma.binCluster.create({ data: request.body });
      return reply.code(201).send({ data: cluster });
    } catch (err: any) {
      if (err?.code === "P2002") return reply.code(409).send({ error: "Conflict", message: "Cluster ID already exists." });
      if (err?.code === "P2003") return reply.code(400).send({ error: "Bad Request", message: "Zone does not exist." });
      throw err;
    }
  });

  /**
   * PATCH /api/v1/clusters/:id
   */
  fastify.patch<{ Params: { id: string }; Body: any }>("/clusters/:id", async (request, reply) => {
    const { id } = request.params;
    try {
      const cluster = await prisma.binCluster.update({
        where: { id },
        data: { ...request.body, updated_at: new Date() },
      });
      return reply.send({ data: cluster });
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });

  /**
   * DELETE /api/v1/clusters/:id
   */
  fastify.delete<{ Params: { id: string } }>("/clusters/:id", async (request, reply) => {
    const { id } = request.params;
    try {
      await prisma.binCluster.delete({ where: { id } });
      return reply.code(204).send();
    } catch (err: any) {
      if (err?.code === "P2025") return reply.code(404).send({ error: "Not Found" });
      throw err;
    }
  });
}
