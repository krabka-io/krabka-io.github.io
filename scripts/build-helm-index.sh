#!/usr/bin/env bash
# Rebuild the aggregated Krabka Helm chart repository under public/charts.
#
# Charts live in the component repositories, one `charts/<name>/` directory per
# chart. This script discovers them, packages each one, and writes a single
# `index.yaml`. Users then add one Helm repository URL:
#
#   helm repo add krabka https://krabka-io.github.io/charts
#
# Discovery, not a hard-coded list: the script walks every repository in the
# krabka-io organisation and takes the ones that hold a `charts/` directory. A
# new component repository joins the index as soon as it has a chart. It needs
# no change here.
#
# `skip_repos` holds the repositories that carry a `charts/` directory but are
# not component repositories. Their charts must never reach the index.
#
# Chart and app versions come from the component repository's own
# `[workspace.package] version` in `Cargo.toml`, the same rule the monorepo used
# before the split. The version in `Chart.yaml` is ignored, because release-plz
# bumps the workspace version and nothing hand-edits the chart. A repository
# with no workspace version falls back to the `Chart.yaml` version.
#
# The index holds the current version of each chart only. The script clears the
# output directory first, so a renamed chart drops out instead of lingering
# under its old key.
#
# Requirements: helm, gh (authenticated), python3.
set -euo pipefail

org="krabka-io"
# krabka-io/gres is a snapshot of the pre-split monorepo. Its `charts/` holds
# the retired crabka-named charts, so the index must not take them.
skip_repos="gres"
repo_url="https://krabka-io.github.io/charts"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${root}/public/charts"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

for tool in helm gh python3; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "error: ${tool} is not on PATH." >&2
    exit 1
  fi
done

# Optional Helm-native PGP provenance (.prov). The private key is a repository
# secret. The matching public key lives in krabka-io/tooling, under charts/, and
# consumers import it from there to run `helm install --verify`. No key material
# is ever stored in this repository.
sign_args=()
if [[ -n "${HELM_GPG_KEY:-}" ]]; then
  keydir="$(mktemp -d)"
  chmod 700 "${keydir}"
  export GNUPGHOME="${keydir}"
  # Helm needs a binary secret keyring. `gpg --dearmor` converts the armored
  # private key directly. `gpg --import` plus `--export-secret-keys` routes
  # through gpg-agent, which has no TTY in CI and exports nothing for a
  # passphrase-protected key.
  echo "${HELM_GPG_KEY}" | base64 -d | gpg --dearmor > "${keydir}/secring.gpg"
  printf '%s' "${HELM_GPG_PASSPHRASE:-}" > "${keydir}/passphrase"
  sign_args=(--sign --key "${HELM_GPG_KEY_ID}"
    --keyring "${keydir}/secring.gpg"
    --passphrase-file "${keydir}/passphrase")
  echo "==> Helm PGP provenance signing is on"
else
  echo "==> HELM_GPG_KEY is not set. The script skips .prov signing."
fi

rm -rf "${out}"
mkdir -p "${out}"

packaged=0
sources=()

echo "==> Looking for charts in the ${org} organisation"
repos="$(gh api "orgs/${org}/repos?per_page=100" --paginate --jq '.[].name' | sort)"

for repo in ${repos}; do
  if [[ " ${skip_repos} " == *" ${repo} "* ]]; then
    echo "==> ${repo}: skipped, not a component repository"
    continue
  fi

  # `gh api` prints the error body on stdout for a 404, so take the output only
  # when the call succeeds. A repository with no `charts/` directory 404s here.
  chart_dirs=""
  if gh api "repos/${org}/${repo}/contents/charts" \
    --jq '.[] | select(.type == "dir") | .name' > "${work}/dirs.txt" 2>/dev/null; then
    chart_dirs="$(cat "${work}/dirs.txt")"
  fi
  [[ -z "${chart_dirs}" ]] && continue

  branch="$(gh api "repos/${org}/${repo}" --jq '.default_branch')"
  echo "==> ${repo}@${branch}: $(echo "${chart_dirs}" | tr '\n' ' ')"

  src="${work}/${repo}"
  git clone --quiet --depth 1 --branch "${branch}" \
    "https://github.com/${org}/${repo}.git" "${src}"

  version="$(
    python3 - "${src}/Cargo.toml" <<'PY'
import sys, pathlib, tomllib
path = pathlib.Path(sys.argv[1])
if not path.is_file():
    sys.exit(0)
manifest = tomllib.loads(path.read_text())
print(manifest.get("workspace", {}).get("package", {}).get("version", ""))
PY
  )"

  for chart in ${chart_dirs}; do
    if [[ "${chart}" != krabka-* ]]; then
      echo "    warning: ${chart} does not use the krabka- chart name prefix"
    fi
    chart_version="${version}"
    if [[ -z "${chart_version}" ]]; then
      chart_version="$(
        python3 -c \
          'import sys,yaml;print(yaml.safe_load(open(sys.argv[1]))["version"])' \
          "${src}/charts/${chart}/Chart.yaml"
      )"
      echo "    ${chart}: ${chart_version} (from Chart.yaml)"
    else
      echo "    ${chart}: ${chart_version} (from the workspace Cargo.toml)"
    fi
    helm package "${src}/charts/${chart}" \
      --version "${chart_version}" \
      --app-version "${chart_version}" \
      "${sign_args[@]}" \
      -d "${out}" >/dev/null
    packaged=$((packaged + 1))
    sources+=("${chart} ${chart_version} ${repo}")
  done
done

if [[ "${packaged}" -eq 0 ]]; then
  echo "error: no charts found. Refusing to write an empty index." >&2
  exit 1
fi

echo "==> Writing ${out}/index.yaml for ${packaged} chart(s)"
helm repo index "${out}" --url "${repo_url}/"

# GitHub Pages has no directory autoindex, so the bare /charts/ URL would 404.
# Emit a small landing page that documents the repository and lists the charts.
{
  echo '<!doctype html><meta charset="utf-8">'
  echo '<title>Krabka Helm charts</title>'
  echo '<h1>Krabka Helm chart repository</h1>'
  echo "<pre>helm repo add krabka ${repo_url}"
  echo 'helm repo update</pre>'
  echo '<p>Repository index: <a href="index.yaml">index.yaml</a></p>'
  echo '<p>Documentation: <a href="/docs/helm-charts">Helm chart repository</a></p>'
  echo '<h2>Charts</h2><ul>'
  for entry in "${sources[@]}"; do
    read -r name version repo <<<"${entry}"
    echo "<li><a href=\"${name}-${version}.tgz\">${name} ${version}</a>"
    echo " &mdash; <a href=\"https://github.com/${org}/${repo}\">${org}/${repo}</a></li>"
  done
  echo '</ul>'
  echo '<h2>Verify a chart</h2>'
  echo '<p>The chart signing public key lives in'
  echo '<a href="https://github.com/krabka-io/tooling">krabka-io/tooling</a>,'
  echo 'under <code>charts/</code>.</p>'
  echo '<pre>gpg --dearmor &lt; krabka-charts.pub.asc &gt; krabka-keyring.gpg'
  echo 'helm install my-op krabka/krabka-operator --verify --keyring ./krabka-keyring.gpg</pre>'
} > "${out}/index.html"

echo "==> Done"
ls -l "${out}"
