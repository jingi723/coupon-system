import http from 'k6/http';
import exec from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';

// Run with --out json=<path>.json.gz to retain per-request send times.
const base = __ENV.BASE_URL || 'http://localhost:8080';
const phases = {
  baseline: { start: 0, duration: 20, rate: 100, vus: 50, userOffset: 0 },
  burst: { start: 20, duration: 3, rate: 1200, vus: 3600, userOffset: 1000000 },
  recovery: { start: 23, duration: 30, rate: 100, vus: 1000, userOffset: 2000000 },
};
export const options = {
  scenarios: Object.fromEntries(Object.entries(phases).map(([name, p]) => [name, {
    executor: 'constant-arrival-rate', startTime: `${p.start}s`,
    rate: p.rate, timeUnit: '1s', duration: `${p.duration}s`,
    preAllocatedVUs: p.vus, maxVUs: p.vus, gracefulStop: '65s',
  }])),
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
};
const started = new Counter('issue_started');
const outcomes = new Counter('issue_outcomes');
const responseTime = new Trend('issue_response_ms', true);

export function setup() {
  if (!__ENV.COUPON_ID || !__ENV.RUN_ID) throw new Error('COUPON_ID and RUN_ID required');
  return { couponId: Number(__ENV.COUPON_ID) };
}

export default function (ctx) {
  const scheduled = phases[exec.scenario.name];
  const origin = exec.scenario.startTime - scheduled.start * 1000;
  const userId = 2000000000 + scheduled.userOffset + exec.scenario.iterationInTest;
  const requestId = `${__ENV.RUN_ID}-0000-4000-8000-${String(userId).padStart(12, '0')}`;
  const sent = Date.now();
  const elapsed = (sent - origin) / 1000;
  const phase = elapsed < 20 ? 'baseline' : elapsed < 23 ? 'burst' : elapsed < 53 ? 'recovery' : 'late';
  if (exec.scenario.name === 'baseline' && exec.scenario.iterationInTest === 0) {
    console.log(`RUN_ORIGIN_MS=${origin}`);
  }
  started.add(1, { phase });
  const res = http.post(`${base}/api/coupons/${ctx.couponId}/issue-requests`, JSON.stringify({ requestId, userId }), {
    headers: { 'Content-Type': 'application/json' }, timeout: '60s', tags: { name: 'issue', phase },
  });
  let outcome = 'unexpected';
  if (res.status === 0) outcome = 'unknown';
  else if (res.status >= 500) outcome = 'server_error';
  else if (res.status >= 400) outcome = 'rejected';
  else if (res.status === 202) {
    try { outcome = res.json('success') === true ? 'accepted' : 'unexpected'; } catch (_) { /* malformed response */ }
  }
  outcomes.add(1, { phase, outcome });
  // Capture origin and send time, so completion during recovery never reclassifies a burst request.
  responseTime.add(Date.now() - sent, {
    phase, outcome, sent_ms: String(sent), origin_ms: String(origin), user_id: String(userId),
    request_id: requestId, status: String(res.status), error_code: String(res.error_code || 0),
  });
}

export function handleSummary(data) {
  return { [__ENV.RESULT_PATH || 'burst-summary.json']: JSON.stringify(data, null, 2) };
}
