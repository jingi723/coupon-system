import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const STOCK = parseInt(__ENV.STOCK || '100000');
const VUS = parseInt(__ENV.VUS || '300');
const DURATION = __ENV.DURATION || '30s';
// 실행마다 겹치지 않는 userId 대역 (재실행 시 RUN_SEED 변경)
const RUN_SEED = parseInt(__ENV.RUN_SEED || '1');

export const options = {
  scenarios: {
    issue: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
    },
  },
};

const issued = new Counter('coupon_issued');
const duplicated = new Counter('coupon_duplicate');
const soldout = new Counter('coupon_soldout');
const serverErr = new Counter('coupon_server_error');

export function setup() {
  const body = JSON.stringify({
    name: `load-test-${RUN_SEED}`,
    description: 'k6 load test',
    couponType: 'FIXED_AMOUNT',
    discountValue: 1000,
    minOrderAmount: 0,
    maxDiscountAmount: 1000,
    totalQuantity: STOCK,
    validDays: 30,
    startDateTime: '2026-08-01T00:00:00',
    endDateTime: '2026-08-31T00:00:00',
  });
  const res = http.post(`${BASE}/api/coupons`, body, {
    headers: { 'Content-Type': 'application/json' },
  });
  const parsed = JSON.parse(res.body);
  const data = parsed.data || parsed;
  const couponId = data.id || data.couponId;
  if (!couponId) {
    throw new Error(`coupon create failed: ${res.status} ${res.body}`);
  }
  const init = http.post(`${BASE}/api/coupons/${couponId}/stock/init`, null);
  console.log(`coupon created: id=${couponId}, stock=${STOCK}, stock_init=${init.status}`);
  return { couponId };
}

export default function (ctx) {
  // VU·iter 조합으로 전 실행에서 고유한 userId 생성
  const userId = RUN_SEED * 100000000 + __VU * 100000 + __ITER;
  const res = http.post(
    `${BASE}/api/coupons/${ctx.couponId}/issue`,
    JSON.stringify({ userId: userId }),
    { headers: { 'Content-Type': 'application/json' }, timeout: '60s' }
  );
  if (res.status === 200) issued.add(1);
  else if (res.status >= 500) serverErr.add(1);
  else if (res.body && res.body.includes('uplicate')) duplicated.add(1);
  else if (res.body && (res.body.includes('xhaust') || res.body.includes('old out'))) soldout.add(1);
  check(res, { 'status is 2xx/4xx (no 5xx)': (r) => r.status < 500 });
}

export function handleSummary(data) {
  const m = data.metrics;
  const line = {
    vus: VUS,
    duration: DURATION,
    total_reqs: m.http_reqs ? m.http_reqs.values.count : 0,
    rps: m.http_reqs ? m.http_reqs.values.rate : 0,
    p50_ms: m.http_req_duration ? m.http_req_duration.values.med : null,
    p95_ms: m.http_req_duration ? m.http_req_duration.values['p(95)'] : null,
    max_ms: m.http_req_duration ? m.http_req_duration.values.max : null,
    issued: m.coupon_issued ? m.coupon_issued.values.count : 0,
    server_errors: m.coupon_server_error ? m.coupon_server_error.values.count : 0,
  };
  return {
    stdout: '\nRESULT_JSON ' + JSON.stringify(line) + '\n',
  };
}
