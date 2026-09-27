# ============ Chat-service Dockerfile ============
FROM oven/bun:1.1 AS base

WORKDIR /app

# 安装依赖
COPY package.json ./
RUN bun install

# 复制源码
COPY . .

# 暴露端口
EXPOSE 3004

# 启动
CMD ["bun", "run", "start"]