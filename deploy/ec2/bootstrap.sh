#!/usr/bin/env bash
# Bootstraps TerminalPulse on a single EC2 instance with k3s (Kubernetes) + HPA.
#
# Instance: Ubuntu 24.04, a free-tier-eligible type with >= 2 vCPU / 4 GB (e.g. m7i-flex.large or
# c7i-flex.large on the AWS Free Plan), 20 GB gp3 disk. Security group: allow TCP 22 and 30080.
#
#   GH_OWNER=<your-github-user> bash bootstrap.sh
#
# Images come from GHCR, published by .github/workflows/ci.yml on every push to main. Make the two
# packages (terminalpulse, terminalpulse-ml) public in GitHub -> Packages, or add an imagePullSecret.
set -euo pipefail

GH_OWNER="${GH_OWNER:?set GH_OWNER to your GitHub username}"
OWNER_LC="$(echo "$GH_OWNER" | tr '[:upper:]' '[:lower:]')"
REPO_URL="${REPO_URL:-https://github.com/${GH_OWNER}/terminalpulse.git}"
DIR="$HOME/terminalpulse"

sudo apt-get update -y
sudo apt-get install -y git curl

if ! command -v k3s >/dev/null; then
  # k3s bundles containerd, Traefik and metrics-server (which the HPA needs).
  curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 644
fi
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

if [ -d "$DIR/.git" ]; then git -C "$DIR" pull --ff-only; else git clone "$REPO_URL" "$DIR"; fi
cd "$DIR/deploy/k8s"
sed -i "s#ghcr.io/OWNER/#ghcr.io/${OWNER_LC}/#" kustomization.yaml loadtest/kustomization.yaml

kubectl apply -k .
kubectl -n terminalpulse rollout status statefulset/postgres --timeout=300s
kubectl -n terminalpulse rollout status deploy/gateway --timeout=300s
kubectl -n terminalpulse rollout status deploy/api --timeout=300s

IP="$(curl -s --max-time 3 http://checkip.amazonaws.com || echo '<public-ip>')"
cat <<MSG

TerminalPulse is running.
  Dashboard + API : http://${IP}:30080
  Watch autoscaling: kubectl -n terminalpulse get hpa gateway -w
  3x load test     : kubectl apply -k ${DIR}/deploy/k8s/loadtest && kubectl -n terminalpulse logs -f job/loadtest
MSG
