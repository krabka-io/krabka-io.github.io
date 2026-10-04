import { viewerCases, viewerData } from '../../../utils/benchmarks.mjs';
import type { APIRoute } from 'astro';

export function getStaticPaths() {
  return viewerCases.flatMap(workload => [1, 2, 3].map(repetition => ({
    params: { id: workload.key + '-' + repetition }, props: { key: workload.key, repetition },
  })));
}

export const GET: APIRoute = ({ props }) => Response.json(viewerData(props.key, props.repetition));
