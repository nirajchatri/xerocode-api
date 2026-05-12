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

cat <<'EOF'
If both curls return {"ok":true} but https://apis.xerocode.ai still returns 502 (server: awselb/2.0),
the AWS load balancer is not reaching this host on the target group port.

Check in AWS:
- Target group protocol HTTP, traffic port 8787 (or 80/443 only if nginx is running here).
- Health check: HTTP, path /api/health/ready, success codes 200 (use /api/health only for a liveness probe).
- Registered target is this instance on the same port as traffic.
- Instance security group allows TCP on that port from the load balancer security group.
EOF
