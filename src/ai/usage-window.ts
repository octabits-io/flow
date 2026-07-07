/**
 * Calendar-window helpers shared by the quota and usage-aggregation engines.
 * All windows are computed in **UTC**, matching flow's existing rollup date
 * derivation (`now().toISOString().split('T')[0]`).
 */

/** Format a `Date` as a `YYYY-MM-DD` calendar date in UTC. */
export function toIsoDate(date: Date): string {
  return date.toISOString().split('T')[0]!;
}

/** First day (`YYYY-MM-01`) of the calendar month containing the given `YYYY-MM-DD` date. */
export function monthStartOf(isoDate: string): string {
  return isoDate.slice(0, 7) + '-01';
}
