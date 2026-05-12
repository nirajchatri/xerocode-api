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
  echo "Nginx /api/health via local port 80 (ALB-style HTTP):"
  curl -fsS -H 'Host: apis.xerocode.ai' 'http://127.0.0.1/api/health' || echo 'nginx did not return JSON on :80'
  echo
fi

cat <<'EOF'
If Node curls return JSON but https://apis.xerocode.ai returns nginx 404, reload config/nginx-api.conf,
remove /etc/nginx/sites-enabled/default, and point the ALB target group at port 80 (HTTP) or 443 (HTTPS).
EOF
