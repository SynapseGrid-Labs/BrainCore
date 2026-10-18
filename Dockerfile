FROM oven/bun:1.4.3-alpine@sha256:629e17411f1f129dbec3af78d5af9c9f2a937435c80349437206c6b0b7422373 AS bun-runtime

FROM oven/bun:1.4.3-alpine@sha256:629e17411f1f129dbec3af78d5af9c9f2a937435c80349437206c6b0b7422373 AS dashboard-build
WORKDIR /build
COPY package.json bun.lock tsconfig.json ./
COPY dashboard/package.json dashboard/package.json
COPY packages ./packages
COPY worker ./worker
COPY tests-ts ./tests-ts
COPY dashboard ./dashboard
RUN bun install --frozen-lockfile
RUN bun run build:dashboard

FROM python:3.12-alpine@sha256:6d43704baacd1bfbe7c295d7f13079d5d8104ed33568873133f8fc69980419df AS full
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_NO_CACHE=1 \
    PATH="/app/.venv/bin:$PATH"
COPY --from=bun-runtime /usr/local/bin/bun /usr/local/bin/bun
RUN apk upgrade --no-cache \
    && apk add --no-cache libgcc libstdc++
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE THIRD_PARTY_NOTICES.md ./
COPY assets ./assets
COPY src ./src
COPY migrations ./migrations
COPY package.json bun.lock tsconfig.json ./
COPY dashboard/package.json dashboard/package.json
COPY packages ./packages
COPY worker ./worker
COPY --from=dashboard-build /build/dashboard/dist ./dashboard/dist
RUN python -m pip install --no-cache-dir --upgrade pip==26.2.0 \
    && python -m pip install --no-cache-dir uv==0.10.8 \
    && uv sync --locked --no-dev --no-editable --compile-bytecode \
    && bun install --production --frozen-lockfile \
    && /usr/local/bin/python -m pip uninstall --yes uv pip \
    && rm -rf /root/.cache/uv /root/.cache/pip /root/.bun/install/cache
RUN addgroup -g 10001 -S braincore \
    && adduser -u 10001 -S -D -H -G braincore braincore \
    && chown -R braincore:braincore /app
USER braincore
EXPOSE 8787
CMD ["uvicorn", "braincore_memory.api:app", "--host", "0.0.0.0", "--port", "8787"]
