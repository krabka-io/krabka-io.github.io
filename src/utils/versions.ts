import { execSync } from 'node:child_process';
import fallbackData from '../data/ecosystem-versions.json' with { type: 'json' };

export type ReleaseTrack = '1.0+' | 'Pre-1.0' | 'Prerelease' | 'Unreleased';

export function releaseTrack(tag: string | null): ReleaseTrack {
  if (!tag) return 'Unreleased';
  const match = /^v?(\d+)\.\d+\.\d+$/.exec(tag);
  return match ? (Number(match[1]) === 0 ? 'Pre-1.0' : '1.0+') : 'Prerelease';
}

export interface EcosystemRepo {
  repo: string;
  target: string;
  provenance: string;
  manifestVersion: string;
  releaseVersion: string | null;
  releaseUrl: string | null;
  mainCommit: string;
  aheadBy: number | null;
  releaseTrack: ReleaseTrack;
  dataSource: 'GitHub' | 'Cached';
  changelogUrl: string;
  compareUrl: string | null;
}

export interface VersionSnapshot {
  repos: EcosystemRepo[];
  source: 'GitHub' | 'Mixed' | 'Cached';
  builtAt: string;
  checkedAt: string | null;
  snapshotCommitDate: string | null;
}

function resolveGitHubToken(): string | null {
  if (typeof process !== 'undefined' && process.env) {
    if (process.env.GITHUB_TOKEN) return process.env.GITHUB_TOKEN;
    if (process.env.GH_TOKEN) return process.env.GH_TOKEN;
  }

  // Attempt to resolve token from local gh CLI if available
  try {
    const token = execSync('gh auth token', {
      encoding: 'utf-8',
      stdio: ['pipe', 'pipe', 'ignore'],
      timeout: 2000,
    }).trim();
    if (token) return token;
  } catch {
    // gh CLI not available or not logged in
  }

  return null;
}

function highestSemverTag(tags: string[]): string | null {
  let best: { tag: string; parts: number[] } | null = null;
  for (const tag of tags) {
    const match = /^v(\d+)\.(\d+)\.(\d+)$/.exec(tag);
    if (!match) continue;
    const parts = [Number(match[1]), Number(match[2]), Number(match[3])];
    const newer =
      !best ||
      parts[0] > best.parts[0] ||
      (parts[0] === best.parts[0] && (parts[1] > best.parts[1] || (parts[1] === best.parts[1] && parts[2] > best.parts[2])));
    if (newer) best = { tag, parts };
  }
  return best?.tag ?? null;
}

function tagRelease(repo: string, tags: string[]): { tagName: string; url: string } | null {
  const tagName = highestSemverTag(tags);
  return tagName ? { tagName, url: `https://github.com/krabka-io/${repo}/releases/tag/${tagName}` } : null;
}

export async function getEcosystemVersions(): Promise<VersionSnapshot> {
  const builtAt = new Date().toISOString();
  let snapshotCommitDate: string | null = null;
  try {
    snapshotCommitDate = execSync('git log -1 --format=%cI -- src/data/ecosystem-versions.json', {
      encoding: 'utf-8', stdio: ['pipe', 'pipe', 'ignore'], timeout: 2000,
    }).trim() || null;
  } catch {
    // Source archives may not have Git metadata.
  }
  const cachedRepos: EcosystemRepo[] = fallbackData.map(({ status: _status, ...item }) => ({
    ...item,
    releaseTrack: releaseTrack(item.releaseVersion),
    dataSource: 'Cached',
    changelogUrl: item.releaseUrl || `https://github.com/krabka-io/${item.repo}/commits/main`,
    compareUrl: item.releaseVersion ? `https://github.com/krabka-io/${item.repo}/compare/${item.releaseVersion}...main` : null,
  }));
  const cached: VersionSnapshot = { repos: cachedRepos, source: 'Cached', builtAt, checkedAt: null, snapshotCommitDate };
  const token = resolveGitHubToken();

  // If no token is found, return the cached fallback data immediately
  if (!token) return cached;

  try {
    const graphqlQuery = {
      query: `query {
        organization(login: "krabka-io") {
          repositories(first: 30, orderBy: {field: NAME, direction: ASC}) {
            nodes {
              name
              latestRelease {
                tagName
                url
                publishedAt
              }
              tags: refs(refPrefix: "refs/tags/", first: 100) {
                nodes {
                  name
                }
              }
              defaultBranchRef {
                name
                target {
                  ... on Commit {
                    oid
                  }
                }
              }
            }
          }
        }
      }`,
    };

    const headers: Record<string, string> = {
      'User-Agent': 'krabka-website-builder',
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    };

    const response = await fetch('https://api.github.com/graphql', {
      method: 'POST',
      headers,
      body: JSON.stringify(graphqlQuery),
      signal: AbortSignal.timeout(5000),
    });

    if (!response.ok) {
      console.warn(`GitHub GraphQL responded with status ${response.status}; using cached versions.`);
      return cached;
    }

    const payload = await response.json();
    const liveNodes = payload?.data?.organization?.repositories?.nodes;
    if (payload.errors?.length || !Array.isArray(liveNodes) || !liveNodes.length) return cached;

    const resolved: EcosystemRepo[] = await Promise.all(
      fallbackData.map(async (fallbackItem) => {
        const liveRepo = liveNodes.find((n: { name: string }) => n.name === fallbackItem.repo);
        if (!liveRepo?.defaultBranchRef?.target?.oid) return cachedRepos.find((item) => item.repo === fallbackItem.repo)!;
        // A repository can tag a version without publishing a GitHub Release.
        // Fall back to its highest semver tag, so the page and `versions.json` agree.
        const release =
          liveRepo?.latestRelease ??
          tagRelease(fallbackItem.repo, liveRepo?.tags?.nodes?.map((n: { name: string }) => n.name) ?? []);
        const mainSha = liveRepo?.defaultBranchRef?.target?.oid?.slice(0, 7) || fallbackItem.mainCommit;

        let aheadBy: number | null = null;
        let compareUrl: string | null = null;

        if (release?.tagName) {
          compareUrl = `https://github.com/krabka-io/${fallbackItem.repo}/compare/${release.tagName}...main`;
          try {
            const cmpRes = await fetch(
              `https://api.github.com/repos/krabka-io/${fallbackItem.repo}/compare/${release.tagName}...main`,
              {
                headers,
                signal: AbortSignal.timeout(3000),
              }
            );
            if (cmpRes.ok) {
              const cmpData = await cmpRes.json();
              aheadBy = typeof cmpData.ahead_by === 'number' ? cmpData.ahead_by : aheadBy;
            }
          } catch {
            // A failed comparison has no current ahead count.
          }
        }

        const releaseVersion = release?.tagName || null;
        const releaseUrl = release?.url || null;
        // Dynamically compute provenance: if released, use verified artifact provenance; otherwise Planned (L3)
        let provenance = 'Planned (L3)';
        if (releaseVersion) {
          provenance = fallbackItem.provenance;
        }

        return {
          repo: fallbackItem.repo,
          target: fallbackItem.target,
          provenance,
          manifestVersion: fallbackItem.manifestVersion,
          releaseVersion,
          releaseUrl,
          mainCommit: mainSha,
          aheadBy: releaseVersion ? aheadBy : null,
          releaseTrack: releaseTrack(releaseVersion),
          dataSource: 'GitHub' as const,
          changelogUrl: releaseUrl || `https://github.com/krabka-io/${fallbackItem.repo}/commits/main`,
          compareUrl,
        };
      })
    );

    const liveCount = resolved.filter((item) => item.dataSource === 'GitHub').length;
    return { ...cached, repos: resolved, source: liveCount === resolved.length ? 'GitHub' : liveCount ? 'Mixed' : 'Cached', checkedAt: liveCount ? new Date().toISOString() : null };
  } catch (err) {
    console.warn('Failed to query GitHub for live repo status; using cached versions.', err);
    return cached;
  }
}

export interface Release {
  repo: string;
  tag: string;
  name: string;
  url: string;
  date: string | null;
  notes: string;
}

// Version keys in versions.json, by repository, for the release dates the
// offline fallback reads.
const versionKeys: Record<string, string> = {
  'krabka-broker': 'broker',
  'krabka-protocol': 'protocol',
  'krabka-client-rs': 'client-rs',
  'krabka-streams-java': 'streams-java',
  'krabka-streams-go': 'streams-go',
  'krabka-streams-rs': 'streams-rs',
  'krabka-cli': 'cli',
  'krabka-connect': 'connect',
  'krabka-operator': 'operator',
  'krabka-gateway': 'gateway',
  'krabka-rebalancer': 'rebalancer',
};

let releasesCache: Promise<Release[]> | null = null;

/** Recent releases across the ecosystem, newest first, for /releases and its feed. */
export function getReleases(): Promise<Release[]> {
  releasesCache ??= loadReleases();
  return releasesCache;
}

async function loadReleases(): Promise<Release[]> {
  const repos = fallbackData.map((item) => item.repo);
  const offline = async (): Promise<Release[]> => {
    const { default: versions } = await import('../data/versions.json');
    return fallbackData
      .filter((item) => item.releaseVersion && item.releaseUrl)
      .map((item) => ({
        repo: item.repo,
        tag: item.releaseVersion!,
        name: `${item.repo} ${item.releaseVersion}`,
        url: item.releaseUrl!,
        date: (versions as Record<string, { releaseDate?: string }>)[versionKeys[item.repo]]?.releaseDate ?? null,
        notes: '',
      }))
      .sort((a, b) => (b.date ?? '').localeCompare(a.date ?? ''));
  };

  const token = resolveGitHubToken();
  if (!token) return offline();
  try {
    const response = await fetch('https://api.github.com/graphql', {
      method: 'POST',
      headers: { 'User-Agent': 'krabka-website-builder', 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({
        query: `query {
          organization(login: "krabka-io") {
            repositories(first: 30) {
              nodes {
                name
                releases(first: 5, orderBy: {field: CREATED_AT, direction: DESC}) {
                  nodes { tagName name url publishedAt isDraft description }
                }
              }
            }
          }
        }`,
      }),
      signal: AbortSignal.timeout(8000),
    });
    if (!response.ok) return offline();
    const nodes = (await response.json())?.data?.organization?.repositories?.nodes ?? [];
    const releases: Release[] = nodes
      .filter((n: { name: string }) => repos.includes(n.name))
      .flatMap((n: { name: string; releases: { nodes: any[] } }) =>
        n.releases.nodes
          .filter((r) => !r.isDraft && r.publishedAt)
          .map((r) => ({
            repo: n.name,
            tag: r.tagName,
            // Some repositories title a release by its tag alone; name the repository too.
            name: r.name?.toLowerCase().includes(n.name.replace('krabka-', '')) ? r.name : `${n.name} ${r.name || r.tagName}`,
            url: r.url,
            date: r.publishedAt,
            notes: (r.description ?? '').trim(),
          })),
      )
      .sort((a: Release, b: Release) => (b.date ?? '').localeCompare(a.date ?? ''));
    return releases.length ? releases : offline();
  } catch {
    return offline();
  }
}
