# multica-dingding-service
# 构建: docker build -t multica-dingding-service .
# 多架构: buildx 自动注入 TARGETARCH (amd64/arm64)

FROM python:3.12-slim

ARG MULTICA_VERSION=0.10.0
# 国内网络可替换为镜像地址，如 ghproxy 类前缀
ARG MULTICA_DOWNLOAD_URL=https://github.com/multica-ai/multica/releases/download

# 安装 multica CLI（GitHub 官方 release，静态链接 Go 二进制）
# TARGETARCH 由 buildx 注入；普通 docker build 时按宿主架构探测
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/* \
    && ARCH="${TARGETARCH:-$(uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')}" \
    && curl -fsSL \
       "${MULTICA_DOWNLOAD_URL}/v${MULTICA_VERSION}/multica-cli-${MULTICA_VERSION}-linux-${ARCH}.tar.gz" \
       | tar -xz -C /tmp \
    && mv /tmp/multica /usr/local/bin/multica \
    && rm -f /tmp/LICENSE /tmp/NOTICE /tmp/README* \
    && chmod +x /usr/local/bin/multica \
    && useradd -m -u 10001 multica

WORKDIR /app
# 国内网络建议：--build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_INDEX_URL=https://pypi.org/simple
COPY requirements.txt .
RUN pip install --no-cache-dir -i ${PIP_INDEX_URL} -r requirements.txt

COPY app ./app

# 预建挂载点并归属运行用户：named volume 模式下首次挂载会继承镜像内目录属主，
# 不预建则 volume 以 root 初始化，容器内（uid 10001）无法写 data/logs
RUN mkdir -p /app/config /app/data /app/logs /home/multica/.multica \
    && chown -R multica:multica /app/config /app/data /app/logs /home/multica

ENV HOME=/home/multica \
    PYTHONUNBUFFERED=1 \
    MDS_MULTICA_BINARY=/usr/local/bin/multica

USER multica
EXPOSE 8300

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "app.main"]
