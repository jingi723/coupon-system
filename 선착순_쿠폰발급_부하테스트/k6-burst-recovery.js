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
  const res = http.post(`${base}/api/coupons`, JSON.stringify({
    name: `burst-recovery-${Date.now()}`, description: 'synchronous burst recovery baseline',
    couponType: 'FIXED_AMOUNT', discountValue: 1000, minOrderAmount: 0,
    maxDiscountAmount: 1000, totalQuantity: 100000, validDays: 30,
    startDateTime: '2026-09-01T00:00:00', endDateTime: '2026-12-31T23:59:59',
  }), { headers: { 'Content-Type': 'application/json' } });
  if (res.status !== 200 || !res.json('success')) throw new Error(`Create failed: ${res.status} ${res.body}`);
  const couponId = res.json('data.id');
  const init = http.post(`${base}/api/coupons/${couponId}/stock/init`, null);
  if (init.status !== 200 || !init.json('success')) throw new Error(`Init failed: ${init.status}`);
  console.log(`TEST_COUPON_ID=${couponId}`);
  return { couponId };
}

export default function (ctx) {
  const scheduled = phases[exec.scenario.name];
  const origin = exec.scenario.startTime - scheduled.start * 1000;
  const userId = 2000000000 + scheduled.userOffset + exec.scenario.iterationInTest;
  const sent = Date.now();
  const elapsed = (sent - origin) / 1000;
  const phase = elapsed < 20 ? 'baseline' : elapsed < 23 ? 'burst' : elapsed < 53 ? 'recovery' : 'late';
  if (exec.scenario.name === 'baseline' && exec.scenario.iterationInTest === 0) {
    console.log(`RUN_ORIGIN_MS=${origin}`);
  }
  started.add(1, { phase });
  const res = http.post(`${base}/api/coupons/${ctx.couponId}/issue`, JSON.stringify({ userId }), {
    headers: { 'Content-Type': 'application/json' }, timeout: '60s', tags: { name: 'issue', phase },
  });
  let outcome = 'unexpected';
  if (res.status === 0) outcome = 'unknown';
  else if (res.status >= 500) outcome = 'server_error';
  else if (res.status >= 400) outcome = 'rejected';
  else if (res.status === 200) {
    try { outcome = res.json('success') === true ? 'completed' : 'unexpected'; } catch (_) { /* malformed response */ }
  }
  outcomes.add(1, { phase, outcome });
  // Capture origin and send time, so completion during recovery never reclassifies a burst request.
  responseTime.add(Date.now() - sent, {
    phase, outcome, sent_ms: String(sent), origin_ms: String(origin), user_id: String(userId),
    status: String(res.status), error_code: String(res.error_code || 0),
  });
}

export function handleSummary(data) {
  return { [__ENV.RESULT_PATH || 'burst-summary.json']: JSON.stringify(data, null, 2) };
}
