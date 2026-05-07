import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend, Counter } from "k6/metrics";
import { randomIntBetween } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

const BASE_URL = __ENV.BASE_URL || "http://localhost:9876/otlp";
// const BASE_URL = __ENV.BASE_URL || "http://localhost:10428/insert/opentelemetry";
const ENDPOINT = `${BASE_URL}/v1/traces`;

export const options = {
    stages: [
        { duration: "30s", target: 100 },   // warm-up: ramp to 10 VUs
        { duration: "1m",  target: 500 },   // ramp to target load
        { duration: "10m",  target: 2500 },   // steady state
        { duration: "30s", target: 0 },
    ],
    thresholds: {
        // 95th-percentile response time under 500 ms
        http_req_duration: ["p(95)<500"],
        // Error rate below 1 %
        otlp_ingest_errors: ["rate<0.01"],
        // At least 99 % of checks pass
        checks: ["rate>0.99"],
    },
};

const errorRate = new Rate("otlp_ingest_errors");
const ingestTrend = new Trend("otlp_ingest_duration_ms", true);
const spanCounter = new Counter("spans_sent");

// ---------- protobuf helpers ----------

function concatBytes(parts) {
    let len = parts.reduce((n, p) => n + p.length, 0);
    let out = new Uint8Array(len);
    let offset = 0;
    for (const p of parts) {
        out.set(p, offset);
        offset += p.length;
    }
    return out;
}

function strBytes(str) {
    const bytes = [];

    for (let i = 0; i < str.length; i++) {
        let codePoint = str.charCodeAt(i);

        if (codePoint < 0x80) {
            bytes.push(codePoint);
        } else if (codePoint < 0x800) {
            bytes.push(
                0xc0 | (codePoint >> 6),
                0x80 | (codePoint & 0x3f)
            );
        } else if (codePoint >= 0xd800 && codePoint <= 0xdbff) {
            const high = codePoint;
            const low = str.charCodeAt(++i);
            codePoint = 0x10000 + ((high & 0x3ff) << 10) + (low & 0x3ff);

            bytes.push(
                0xf0 | (codePoint >> 18),
                0x80 | ((codePoint >> 12) & 0x3f),
                0x80 | ((codePoint >> 6) & 0x3f),
                0x80 | (codePoint & 0x3f)
            );
        } else {
            bytes.push(
                0xe0 | (codePoint >> 12),
                0x80 | ((codePoint >> 6) & 0x3f),
                0x80 | (codePoint & 0x3f)
            );
        }
    }

    return new Uint8Array(bytes);
}

function varint(n) {
    let out = [];
    n = BigInt(n);
    while (n >= 0x80n) {
        out.push(Number((n & 0x7fn) | 0x80n));
        n >>= 7n;
    }
    out.push(Number(n));
    return new Uint8Array(out);
}

function tag(fieldNo, wireType) {
    return varint((fieldNo << 3) | wireType);
}

function lenDelimited(fieldNo, bytes) {
    return concatBytes([tag(fieldNo, 2), varint(bytes.length), bytes]);
}

function stringField(fieldNo, value) {
    return lenDelimited(fieldNo, strBytes(value));
}

function bytesField(fieldNo, bytes) {
    return lenDelimited(fieldNo, bytes);
}

function enumField(fieldNo, value) {
    return concatBytes([tag(fieldNo, 0), varint(value)]);
}

function fixed64Field(fieldNo, value) {
    let v = BigInt(value);
    let b = new Uint8Array(8);
    for (let i = 0; i < 8; i++) {
        b[i] = Number((v >> BigInt(i * 8)) & 0xffn);
    }
    return concatBytes([tag(fieldNo, 1), b]);
}

function hexBytes(byteCount) {
    let b = new Uint8Array(byteCount);
    for (let i = 0; i < byteCount; i++) {
        b[i] = Math.floor(Math.random() * 256);
    }
    return b;
}

function nowNanos() {
    return BigInt(Date.now()) * 1000000n;
}

// ---------- OTLP structures ----------

function anyString(value) {
    // AnyValue.string_value = 1
    return stringField(1, value);
}

function keyValue(key, value) {
    // KeyValue.key = 1
    // KeyValue.value = 2
    return concatBytes([
        stringField(1, key),
        lenDelimited(2, anyString(value)),
    ]);
}

function resource(attrs) {
    // Resource.attributes = 1 repeated KeyValue
    return concatBytes(attrs.map(([k, v]) => lenDelimited(1, keyValue(k, v))));
}

function instrumentationScope(name) {
    // InstrumentationScope.name = 1
    return stringField(1, name);
}

function span({
                  traceId,
                  spanId,
                  parentSpanId,
                  name,
                  kind,
                  startTimeUnixNano,
                  endTimeUnixNano,
                  attributes,
              }) {
    const fields = [
        bytesField(1, traceId),
        bytesField(2, spanId),
    ];

    if (parentSpanId) {
        fields.push(bytesField(4, parentSpanId));
    }

    fields.push(
        stringField(5, name),
        enumField(6, kind),
        fixed64Field(7, startTimeUnixNano),
        fixed64Field(8, endTimeUnixNano),
    );

    for (const [k, v] of attributes) {
        fields.push(lenDelimited(9, keyValue(k, v)));
    }

    return concatBytes(fields);
}

function scopeSpans(spans) {
    const fields = [
        lenDelimited(1, instrumentationScope("k6")),
    ];

    for (const s of spans) {
        fields.push(lenDelimited(2, s));
    }

    return concatBytes(fields);
}

function resourceSpans(spans) {
    return concatBytes([
        lenDelimited(1, resource([
            ["service.name", "k6-load-test"],
            ["deployment.environment", "local"],
        ])),
        lenDelimited(2, scopeSpans(spans)),
    ]);
}

function exportTraceServiceRequest(spans) {
    // ExportTraceServiceRequest.resource_spans = 1
    return lenDelimited(1, resourceSpans(spans));
}

function buildOtlpTraceBatch(batchSize = 5) {
    const traceId = hexBytes(16);
    const rootSpanId = hexBytes(8);
    const start = nowNanos();

    const spans = [];

    spans.push(span({
        traceId,
        spanId: rootSpanId,
        name: "GET /checkout",
        kind: 2, // SERVER
        startTimeUnixNano: start,
        endTimeUnixNano: start + 30000000n,
        attributes: [
            ["http.method", "GET"],
            ["http.route", "/checkout"],
            ["http.status_code", "200"],
        ],
    }));

    for (let i = 1; i < batchSize; i++) {
        const childStart = start + BigInt(i * 1000000);

        spans.push(span({
            traceId,
            spanId: hexBytes(8),
            parentSpanId: rootSpanId,
            name: `operation-${i}`,
            kind: i % 2 === 0 ? 3 : 1, // CLIENT or INTERNAL
            startTimeUnixNano: childStart,
            endTimeUnixNano: childStart + BigInt(randomIntBetween(1000000, 50000000)),
            attributes: [
                ["component", "k6"],
                ["env", "test"],
            ],
        }));
    }

    return exportTraceServiceRequest(spans);
}

export default function () {
    const batchSize = randomIntBetween(1, 10);
    const payload = buildOtlpTraceBatch(batchSize);

    const res = http.post(ENDPOINT, payload.buffer, {
        headers: {
            "Content-Type": "application/x-protobuf",
            "Accept": "application/x-protobuf",
        },
        tags: { endpoint: "otlp_traces" },
    });

    ingestTrend.add(res.timings.duration);
    spanCounter.add(batchSize);
    errorRate.add(res.status >= 400);

    check(res, {
        "status is 200": (r) => r.status === 200,
        "response time < 1s": (r) => r.timings.duration < 1000,
        "no server error": (r) => r.status < 500,
    });

    sleep(randomIntBetween(10, 200) / 1000);
}

export function setup() {
    const probe = http.post(
        ENDPOINT,
        buildOtlpTraceBatch(1).buffer,
        {
            headers: {
                "Content-Type": "application/x-protobuf",
                "Accept": "application/x-protobuf",
            },
        }
    );

    if (probe.status === 0 || probe.status >= 500) {
        throw new Error(`Setup failed. Status: ${probe.status}`);
    }

    return { baseUrl: BASE_URL };
}

export function teardown(data) {
    console.log(`OTLP protobuf load test complete against ${data.baseUrl}`);
}