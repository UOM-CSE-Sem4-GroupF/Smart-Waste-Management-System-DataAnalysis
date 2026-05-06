import "dotenv/config";
import Fastify from "fastify";
import cors from "@fastify/cors";
import { prisma } from "./db";
import { binsRoutes } from "./routes/bins";
import { clustersRoutes } from "./routes/clusters";
import { metadataRoutes } from "./routes/metadata";
import fastifySwagger from "@fastify/swagger";
import fastifySwaggerUi from "@fastify/swagger-ui";

const PORT = parseInt(process.env.PORT ?? "8001", 10);
const HOST = process.env.HOST ?? "0.0.0.0";

const server = Fastify({
  logger: {
    level: "info",
    transport:
      process.env.NODE_ENV !== "production"
        ? { target: "pino-pretty" }
        : undefined,
  },
});

// ── Health Check ─────────────────────────────────────────────────────────────
server.get("/health", async () => {
  return {
    status: "ok",
    service: "core-api",
    version: "1.0.0",
    timestamp: new Date().toISOString(),
  };
});

// ── Graceful Shutdown ────────────────────────────────────────────────────────
const shutdown = async (signal: string) => {
  server.log.info(`Received ${signal}. Shutting down gracefully...`);
  await prisma.$disconnect();
  await server.close();
  process.exit(0);
};

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// ── Start ────────────────────────────────────────────────────────────────────
async function start() {
  try {
    // ── Plugins ─────────────────────────────────────────────────────────────────
    await server.register(cors, { origin: true });

    await server.register(fastifySwagger, {
      swagger: {
        info: {
          title: "SWMS Core API",
          description: "Metadata gateway for Smart Waste Management System",
          version: "1.0.0",
        },
        host: "localhost:8001",
        schemes: ["http"],
        consumes: ["application/json"],
        produces: ["application/json"],
      },
    });

    await server.register(fastifySwaggerUi, {
      routePrefix: "/docs",
      uiConfig: {
        docExpansion: "list",
        deepLinking: false,
      },
    });

    // ── Routes ──────────────────────────────────────────────────────────────────
    await server.register(binsRoutes, { prefix: "/api/v1" });
    await server.register(clustersRoutes, { prefix: "/api/v1" });
    await server.register(metadataRoutes, { prefix: "/api/v1" });

    await server.listen({ port: PORT, host: HOST });
    server.log.info(`🚀 Core API running at http://${HOST}:${PORT}`);
  } catch (err) {
    server.log.error(err);
    await prisma.$disconnect();
    process.exit(1);
  }
}

start();
