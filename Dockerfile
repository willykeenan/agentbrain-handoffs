# AgentBrain Handoffs: CLI, engine and MCP server.
#   MCP (stdio):  docker run -i --rm -v ~/.handoffs:/data ghcr.io/willykeenan/agentbrain-handoffs mcp --agent me
#   Live demo:    docker run --rm -p 8765:8765 ghcr.io/willykeenan/agentbrain-handoffs demo --host 0.0.0.0
FROM python:3.12-slim
LABEL io.modelcontextprotocol.server.name="io.github.willykeenan/agentbrain-handoffs"
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir . && useradd --create-home agent && mkdir -p /data && chown agent /data
USER agent
ENV HANDOFFS_DB=/data/handoffs.sqlite3
VOLUME ["/data"]
ENTRYPOINT ["handoffs"]
CMD ["--help"]
