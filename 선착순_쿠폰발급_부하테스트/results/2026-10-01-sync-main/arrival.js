import http from 'k6/http';
import exec from 'k6/execution';
import { Counter, Trend } from 'k6/metrics';
const rate = Number(__ENV.RATE || 300);
const duration = __ENV.DURATION || '30s';
const base = 'http://localhost:8080';
const issued = new Counter('issue_success');
const serverError = new Counter('issue_5xx');
const transportError = new Counter('issue_transport_error');
const timeout = new Counter('issue_timeout');
const other = new Counter('issue_other');
const latency = new Trend('issue_latency', true);
export const options = {
  scenarios: { issue: { executor: 'constant-arrival-rate', rate, timeUnit: '1s', duration,
    preAllocatedVUs: 600, maxVUs: 1500, gracefulStop: '65s' } },
  summaryTrendStats: ['avg','med','p(95)','p(99)','max'],
};
export function setup() {
  const res = http.post(`${base}/api/coupons`, JSON.stringify({
    name: `sync-arrival-${rate}-${Date.now()}`, description: 'synchronous load experiment',
    couponType: 'FIXED_AMOUNT', discountValue: 1000, minOrderAmount: 0,
    maxDiscountAmount: 1000, totalQuantity: 100000, validDays: 30,
    startDateTime: '2026-09-01T00:00:00', endDateTime: '2026-12-31T23:59:59',
  }), {headers: {'Content-Type':'application/json'}});
  if (res.status !== 200 || !res.json('success')) throw new Error(`Create failed: ${res.status} ${res.body}`);
  const couponId = res.json('data.id');
  const init = http.post(`${base}/api/coupons/${couponId}/stock/init`, null);
  if (init.status !== 200 || !init.json('success')) throw new Error(`Init failed: ${init.status}`);
  console.log(`TEST_COUPON_ID=${couponId}`);
  return {couponId};
}
export default function(ctx) {
  const res = http.post(`${base}/api/coupons/${ctx.couponId}/issue`,
    JSON.stringify({userId: 1000000000 + exec.scenario.iterationInTest}),
    {headers: {'Content-Type':'application/json'}, timeout: '60s', tags: {name:'issue'}});
  latency.add(res.timings.duration);
  if (res.status === 0) {
    transportError.add(1);
    if (res.error_code === 1050) timeout.add(1);
  } else if (res.status >= 500) serverError.add(1);
  else if (res.status === 200 && res.json('success') === true) issued.add(1);
  else other.add(1);
}
export function handleSummary(data) {
  const m = data.metrics;
  const count = name => m[name] ? m[name].values.count : 0;
  const result = { rate, duration, success: count('issue_success'), errors_5xx: count('issue_5xx'),
    transport_errors: count('issue_transport_error'), timeouts: count('issue_timeout'),
    other: count('issue_other'), dropped: count('dropped_iterations'),
    completed_iterations: count('iterations'), latency: m.issue_latency?.values,
    test_duration_ms: data.state.testRunDurationMs };
  return {stdout: '\nRESULT_JSON ' + JSON.stringify(result) + '\n',
    [__ENV.RESULT_PATH || '/tmp/coupon-sync-load-test/result.json']: JSON.stringify(data, null, 2)};
}
