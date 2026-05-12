#!/usr/bin/env bash
set -euo pipefail

PRIVATE_IP="$(hostname -I | awk '{print $1}')"

echo "Listening sockets (expect :8787 for direct ALB targets, or :80/:443 if nginx fronts the API):"
ss -tlnp | grep -E ':80 |:443 |:8787 ' || true
echo

echo "API readiness via loopback (control database):"
curl -fsS "http://127.0.0.1:8787/api/health/ready"
echo

if [[ -n "${PRIVATE_IP}" ]]; then
  echo "API readiness via instance IP (${PRIVATE_IP}):"
  curl -fsS "http://${PRIVATE_IP}:8787/api/health/ready"
  echo
fi

if command -v ufw >/dev/null 2>&1; then
  echo "UFW status:"
  ufw status || true
  echo
fi

if command -v curl >/dev/null 2>&1; then
  echo "API liveness via loopback:"
  curl -fsS "http://127.0.0.1:8787/api/health" || echo 'Node API is not listening on 127.0.0.1:8787'
  echo

  echo "Nginx /api/health via local port 80 (ALB-style HTTP):"
  curl -fsS -H 'Host: apis.xerocode.ai' 'http://127.0.0.1/api/health' || echo 'nginx returned an error (502 usually means the Node API on :8787 is down)'
  echo
fi

cat <<'EOF'
If :8787 returns JSON but :80 through nginx does not, reload config/nginx-api.conf and restart nginx.
If :8787 fails, start or fix the API service:
  sudo systemctl enable --now xerocode-api
  sudo journalctl -u xerocode-api -n 50 --no-pager
EOF
