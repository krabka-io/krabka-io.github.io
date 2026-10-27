// Archived reports reuse their heading across runs. Identify the report in
// page metadata without editing the synced source or its heading anchors.
export function docTitle(baseTitle, id, label) {
  const report =
    /^broker\/operations\/performance-budget-runs\/(\d{4}-\d{2}-\d{2})-.+-run-(\d+)\/performance-budget$/.exec(
      id,
    );
  const title = report
    ? `${baseTitle.replace(/^Performance budget:\s*/i, '')} (${report[1]}, run ${report[2]})`
    : baseTitle;
  return label ? `${title} (${label})` : title;
}
