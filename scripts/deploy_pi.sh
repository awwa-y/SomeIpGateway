#!/usr/bin/env bash
# ============================================================================
# deploy_pi.sh — 在树莓派(Raspberry Pi OS / Debian)上构建并部署 someip_gateway
# 用法：
#   ./scripts/deploy_pi.sh              # 仅构建 + 前台提示运行
#   ./scripts/deploy_pi.sh --install    # 额外安装为 systemd 服务(开机自启)
# 环境变量：
#   PORT=9090  HB_MS=5000  PREFIX=~/someip_gateway
# ============================================================================
set -euo pipefail

PORT="${PORT:-9090}"
HB_MS="${HB_MS:-5000}"
PREFIX="${PREFIX:-$HOME/someip_gateway}"
REPO_URL="${REPO_URL:-https://github.com/awwa-y/SomeIpGateway.git}"
INSTALL_SERVICE=0

for arg in "$@"; do
    case "$arg" in
        --install) INSTALL_SERVICE=1 ;;
        *) echo "未知参数: $arg"; exit 2 ;;
    esac
done

echo "== [1/4] 安装编译依赖 =="
if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update
    sudo apt-get install -y build-essential git
else
    echo "非 Debian 系系统，请自行安装 build-essential/git"
fi

echo "== [2/4] 获取源码 =="
if [ ! -d "$PREFIX/.git" ]; then
    git clone --depth 1 "$REPO_URL" "$PREFIX"
else
    git -C "$PREFIX" fetch --depth 1 origin
    git -C "$PREFIX" reset --hard origin/main
fi

echo "== [3/4] 编译 =="
cd "$PREFIX"
make -j"$(nproc)"

echo "== [4/4] 部署 =="
BIN="$PREFIX/build/someip_gateway"
if [ "$INSTALL_SERVICE" -eq 1 ]; then
    echo "安装 systemd 服务..."
    sudo mkdir -p /etc/systemd/system
    sed -e "s#__PREFIX__#$PREFIX#g" \
        -e "s#__PORT__#$PORT#g" \
        -e "s#__HB_MS__#$HB_MS#g" \
        -e "s#__USER__#${SUDO_USER:-$USER}#g" \
        "$PREFIX/deploy/someip-gateway.service" | sudo tee /etc/systemd/system/someip-gateway.service >/dev/null
    sudo systemctl daemon-reload
    sudo systemctl enable --now someip-gateway.service
    echo "已启动并设为开机自启。查看状态:"
    sudo systemctl status someip-gateway.service --no-pager || true
else
    echo "构建完成。前台运行:"
    echo "  $BIN --port $PORT --hb $HB_MS"
    echo "需要开机自启可加 --install"
fi
