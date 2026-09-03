import { execSync } from 'node:child_process';
import fallbackData from '../data/ecosystem-versions.json';

export interface EcosystemRepo {
  repo: string;
  target: string;
  provenance: string;
  manifestVersion: string;
  releaseVersion: string | null;
  releaseUrl: string | null;
  mainCommit: string;
  aheadBy: number | null;
  status: 'Stable' | 'Beta / Pre-1.0' | 'In Development';
  changelogUrl: string;
  compareUrl: string | null;
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

export async function getEcosystemVersions(): Promise<EcosystemRepo[]> {
  const token = resolveGitHubToken();

  // If no token is found, return the cached fallback data immediately
  if (!token) {
    return fallbackData.map((item) => ({
      ...item,
      changelogUrl: item.releaseUrl || `https://github.com/krabka-io/${item.repo}/commits/main`,
      compareUrl: item.releaseVersion
        ? `https://github.com/krabka-io/${item.repo}/compare/${item.releaseVersion}...main`
        : null,
    })) as EcosystemRepo[];
  }

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
      return fallbackData as unknown as EcosystemRepo[];
    }

    const payload = await response.json();
    const liveNodes = payload?.data?.organization?.repositories?.nodes || [];

    const resolved = await Promise.all(
      fallbackData.map(async (fallbackItem) => {
        const liveRepo = liveNodes.find((n: { name: string }) => n.name === fallbackItem.repo);
        const release = liveRepo?.latestRelease;
        const mainSha = liveRepo?.defaultBranchRef?.target?.oid?.slice(0, 7) || fallbackItem.mainCommit;

        let aheadBy = fallbackItem.aheadBy;
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
            // retain fallback aheadBy
          }
        }

        const releaseVersion = release?.tagName || null;
        const releaseUrl = release?.url || null;
        let status: 'Stable' | 'Beta / Pre-1.0' | 'In Development' = 'In Development';

        if (releaseVersion) {
          status = releaseVersion.startsWith('v1.') ? 'Stable' : 'Beta / Pre-1.0';
        }

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
          status,
          changelogUrl: releaseUrl || `https://github.com/krabka-io/${fallbackItem.repo}/commits/main`,
          compareUrl,
        };
      })
    );

    return resolved;
  } catch (err) {
    console.warn('Failed to query GitHub for live repo status; using cached versions.', err);
    return fallbackData.map((item) => ({
      ...item,
      changelogUrl: item.releaseUrl || `https://github.com/krabka-io/${item.repo}/commits/main`,
      compareUrl: item.releaseVersion
        ? `https://github.com/krabka-io/${item.repo}/compare/${item.releaseVersion}...main`
        : null,
    })) as EcosystemRepo[];
  }
}
