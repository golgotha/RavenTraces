import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Rate } from "k6/metrics";
import { randomItem } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

// ─── Config ─────────────────────────────────────────────────

const BASE_URL = __ENV.BASE_URL || "http://localhost:9876";
const INGEST_ENDPOINT = `${BASE_URL}/zipkin/api/v2/spans`;
const READ_ENDPOINT_TRACE   = `${BASE_URL}/api/v1/trace`;
const READ_ENDPOINT_TRACES   = `${BASE_URL}/api/v1/traces`;

// ─── Load Profile ───────────────────────────────────────────

export const options = {
    stages: [
        { duration: "20s", target: 10 },
        { duration: "1m",  target: 30 },
        { duration: "2m",  target: 30 },
        { duration: "20s", target: 0 },
    ],
    thresholds: {
        http_req_duration: ["p(95)<300"],
        read_errors: ["rate<0.01"],
        checks: ["rate>0.99"],
    },
};

// ─── Metrics ────────────────────────────────────────────────

const readTrend = new Trend("trace_read_duration", true);
const errorRate = new Rate("read_errors");

// ─── Helpers (reuse from your ingest test) ──────────────────

function hexId16() {
    return Math.random().toString(16).slice(2).padEnd(16, "0").slice(0, 16);
}

function hexId32() {
    return hexId16() + hexId16();
}

function nowMicros() {
    return Date.now() * 1000;
}

function buildSpanBatch(traceId, batchSize = 5) {
    const rootId = hexId16();

    return Array.from({ length: batchSize }, (_, i) => ({
        traceId,
        id: hexId16(),
        parentId: i === 0 ? undefined : rootId,
        name: "test-span",
        timestamp: nowMicros(),
        duration: 1000,
        localEndpoint: { serviceName: "test-service" },
    }));
}

// ─── Setup: ingest known traces ─────────────────────────────

export function setup() {
    const traceIds = [];

    console.log("🔧 Preparing test data...");

    for (let i = 0; i < 100; i++) {
        const traceId = hexId32();
        traceIds.push(traceId);

        const payload = JSON.stringify(buildSpanBatch(traceId, 5));

        const res = http.post(INGEST_ENDPOINT, payload, {
            headers: { "Content-Type": "application/json" },
        });

        if (res.status >= 400) {
            throw new Error(`Failed to ingest trace ${traceId}`);
        }
    }

    console.log(`✅ Prepared ${traceIds.length} traces`);

    return { traceIds };
}

// ─── Test ──────────────────────────────────────────────────

export default function (data) {
    const traceId = randomItem(data.traceIds);

    const res = http.get(`${READ_ENDPOINT_TRACE}/${traceId}`, {
        tags: { endpoint: "read_trace" },
    });

    readTrend.add(res.timings.duration);
    errorRate.add(res.status >= 400);

    check(res, {
        "status is 200": (r) => r.status === 200,
        "response < 500ms": (r) => r.timings.duration < 500,
        "has spans": (r) => {
            try {
                const body = JSON.parse(r.body);
                return body && body.length > 0;
            } catch {
                return false;
            }
        },
    });

    sleep(Math.random() * 0.2);
}