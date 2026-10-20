# One Node image for the gateway (ingest / persist / api / jobs roles), the simulator,
# the MCP server and the built dashboard. The ROLES env var picks what a container runs.
#   docker build -t terminalpulse .
FROM node:22-slim AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.base.json ./
COPY services/gateway/package.json services/gateway/
COPY services/simulator/package.json services/simulator/
COPY services/mcp-server/package.json services/mcp-server/
COPY dashboard/package.json dashboard/
RUN npm ci --no-audit --no-fund
COPY shared shared
COPY services/gateway services/gateway
COPY services/simulator services/simulator
COPY services/mcp-server services/mcp-server
COPY dashboard dashboard
RUN npm run build -w services/gateway -w services/simulator -w services/mcp-server -w dashboard \
 && npm prune --omit=dev --no-audit --no-fund

FROM node:22-slim
WORKDIR /app
ENV NODE_ENV=production \
    STATIC_DIR=/app/dashboard/dist \
    PROFILES_PATH=/app/shared/fault-profiles.json \
    PORT=8080
COPY --from=build /app/package.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/shared ./shared
COPY --from=build /app/services/gateway/package.json ./services/gateway/
COPY --from=build /app/services/gateway/dist ./services/gateway/dist
COPY --from=build /app/services/gateway/migrations ./services/gateway/migrations
COPY --from=build /app/services/simulator/package.json ./services/simulator/
COPY --from=build /app/services/simulator/dist ./services/simulator/dist
COPY --from=build /app/services/mcp-server/package.json ./services/mcp-server/
COPY --from=build /app/services/mcp-server/dist ./services/mcp-server/dist
COPY --from=build /app/dashboard/dist ./dashboard/dist
USER node
EXPOSE 8080
CMD ["node", "services/gateway/dist/index.js"]
