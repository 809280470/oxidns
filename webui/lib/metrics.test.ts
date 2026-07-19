import { describe, expect, it } from "vitest";

import {
  calculateDnsTrafficMetrics,
  sumServerRequestTotal,
} from "./dashboard-traffic";
import type { PluginMetricsMap } from "./metrics";

describe("dashboard DNS traffic metrics", () => {
  it("sums inbound requests across server plugins only", () => {
    const metrics: PluginMetricsMap = {
      udp: [
        { name: "server_request_total", labels: {}, value: 120 },
        { name: "server_inflight", labels: {}, value: 2 },
      ],
      tcp: [{ name: "server_request_total", labels: {}, value: 80 }],
      cache: [{ name: "cache_hit_total", labels: {}, value: 999 }],
    };

    expect(sumServerRequestTotal(metrics)).toBe(200);
  });

  it("calculates QPS from the actual sampling window", () => {
    expect(
      calculateDnsTrafficMetrics(
        { requestTotal: 100, sampledAtMs: 1_000 },
        { requestTotal: 145, sampledAtMs: 4_000 },
      ),
    ).toEqual({
      status: "available",
      qps: 15,
      requestTotal: 145,
      sampleWindowSeconds: 3,
    });
  });

  it("reports zero QPS when the request counter did not change", () => {
    expect(
      calculateDnsTrafficMetrics(
        { requestTotal: 145, sampledAtMs: 1_000 },
        { requestTotal: 145, sampledAtMs: 4_000 },
      ),
    ).toEqual({
      status: "available",
      qps: 0,
      requestTotal: 145,
      sampleWindowSeconds: 3,
    });
  });

  it("does not invent a QPS value without a valid monotonic baseline", () => {
    expect(
      calculateDnsTrafficMetrics(null, {
        requestTotal: 145,
        sampledAtMs: 4_000,
      }),
    ).toEqual({
      status: "available",
      qps: null,
      requestTotal: 145,
      sampleWindowSeconds: null,
    });
    expect(
      calculateDnsTrafficMetrics(
        { requestTotal: 145, sampledAtMs: 1_000 },
        { requestTotal: 4, sampledAtMs: 4_000 },
      ),
    ).toEqual({
      status: "available",
      qps: null,
      requestTotal: 4,
      sampleWindowSeconds: null,
    });
  });
});
