// Keep the default local command complete; CI selects each suite explicitly.
export function siteCheckSuite(args = []) {
  args = args.filter(arg => arg !== '--headed');
  if (args.length > 1 || (args.length === 1 && !/^--suite=(all|site|lab|proofs)$/.test(args[0]))) {
    throw new Error('Usage: check-site.mjs [--suite=all|site|lab|proofs] [--headed]');
  }
  const suite = args[0]?.slice('--suite='.length) ?? 'all';
  return {
    suite,
    site: suite === 'all' || suite === 'site',
    toolRoutes: [
      ...(suite === 'all' || suite === 'lab' ? ['/docs/lab/'] : []),
      ...(suite === 'all' || suite === 'proofs' ? ['/docs/proof-explorer/'] : []),
    ],
  };
}
