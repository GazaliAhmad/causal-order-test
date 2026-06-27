#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { readdirSync, statSync } from "node:fs";
import { resolve } from "node:path";

const RUNS_DIR = resolve("artifacts/runs");

type JsonRecord = Record<string, any>;

function main(): void {
  const pathArg = process.argv.slice(2).find((token) => token !== "--help");

  if (process.argv.includes("--help")) {
    printHelp();
    return;
  }

  resolveSummaryPath(pathArg)
    .then(async (summaryPath) => {
      const summary = JSON.parse(await readFile(summaryPath, "utf8")) as JsonRecord;
      const runDir = resolve(summaryPath, "..");
      const runtimeConfig = await loadOptionalJson(resolve(runDir, "run-config.json"));
      const heartbeats = await loadNdjson(resolve(runDir, "heartbeats.ndjson"));
      const lifecycle = await loadNdjson(resolve(runDir, "lifecycle.ndjson"));
      console.log(buildReport(runDir, summary, runtimeConfig, heartbeats, lifecycle));
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}

function printHelp(): void {
  console.log("usage: causal-order-testing-summary [path]");
  console.log("");
  console.log("Print a human-readable report for the latest or selected run.");
  console.log("");
  console.log("positional arguments:");
  console.log("  path        Optional path to a run directory or summary.json.");
}

async function resolveSummaryPath(pathArg?: string): Promise<string> {
  if (!pathArg) {
    const latest = findLatestRun(RUNS_DIR);
    if (!latest) {
      throw new Error(`No run folders found in ${RUNS_DIR}`);
    }
    return resolve(latest, "summary.json");
  }

  const path = resolve(pathArg);
  const summaryPath = path.endsWith(".json") ? path : resolve(path, "summary.json");

  if (!existsSync(summaryPath)) {
    throw new Error(`Summary file not found: ${summaryPath}`);
  }

  return summaryPath;
}

function findLatestRun(runsDir: string): string | null {
  if (!existsSync(runsDir)) {
    return null;
  }

  const candidates: Array<{ modifiedMs: number; path: string }> = [];
  for (const entry of readdirSync(runsDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) {
      continue;
    }

    const path = resolve(runsDir, entry.name);
    const summaryPath = resolve(path, "summary.json");
    if (!existsSync(summaryPath)) {
      continue;
    }

    candidates.push({
      modifiedMs: statSync(summaryPath).mtimeMs,
      path,
    });
  }

  candidates.sort((left, right) => right.modifiedMs - left.modifiedMs);
  return candidates[0]?.path ?? null;
}

async function loadNdjson(path: string): Promise<JsonRecord[]> {
  if (!existsSync(path)) {
    return [];
  }

  const rows: JsonRecord[] = [];
  for (const line of (await readFile(path, "utf8")).split(/\r?\n/)) {
    if (line.trim()) {
      rows.push(JSON.parse(line) as JsonRecord);
    }
  }
  return rows;
}

async function loadOptionalJson(path: string): Promise<JsonRecord | null> {
  if (!existsSync(path)) {
    return null;
  }

  return JSON.parse(await readFile(path, "utf8")) as JsonRecord;
}

function buildReport(
  runDir: string,
  summary: JsonRecord,
  runtimeConfig: JsonRecord | null,
  heartbeats: JsonRecord[],
  lifecycle: JsonRecord[],
): string {
  const outcome = summary.outcome ?? {};
  const timing = summary.timing ?? {};
  const config = summary.config ?? {};
  const stream = summary.stream ?? {};
  const simulation = summary.simulation ?? {};
  const transport = summary.transport ?? {};
  const dedupe = summary.dedupe ?? {};
  const hasDedupeTelemetry = Object.keys(dedupe).length > 0;
  const dedupeWindow = resolveDedupeWindow(summary, runtimeConfig);

  const status = outcome.status ?? "unknown";
  const model = config.model ?? "single_process";
  const profileName = config.profileName ?? "unknown";
  const profileSource = config.profileSource ?? "unknown";
  const generated = Number(simulation.generated ?? 0);
  const delivered = Number(
    simulation.delivered ?? transport.receivedEvents ?? simulation.sent ?? 0,
  );
  const ordered = Number(stream.orderedEvents ?? 0);
  const anomalies = Number(stream.anomalies ?? 0);
  const late = Number(stream.byAnomalyType?.late_arrival ?? 0);
  const errors = Number(stream.byAnomalySeverity?.error ?? 0);
  const warnings = Number(stream.byAnomalySeverity?.warning ?? 0);
  const byType = stream.byAnomalyType ?? {};
  const sampleAnomalies = summary.samples?.anomalies ?? [];
  const maxQueue = Number(
    simulation.maxQueueDepth ?? simulation.maxPendingQueueDepth ?? 0,
  );
  const wallElapsedMs = Number(timing.wallElapsedMs ?? 0);

  let peakRssMb: number | null = null;
  let lastRssMb: number | null = null;
  if (heartbeats.length > 0) {
    peakRssMb =
      Math.max(...heartbeats.map((row) => Number(row.rssBytes ?? 0)), 0) /
      (1024 * 1024);
    lastRssMb = Number(heartbeats.at(-1)?.rssBytes ?? 0) / (1024 * 1024);
  }
  const peakDedupeCacheSize = Math.max(
    Number(dedupe.currentCacheSize ?? 0),
    ...heartbeats.map((row) => Number(row.dedupe?.currentCacheSize ?? 0)),
  );

  const { assessment, reasons } = assessRun({
    status,
    errors,
    late,
    anomalies,
    generated,
    maxQueue,
    delivered,
    ordered,
    dedupeWindow,
  });
  const outcomeLine = describeOutcome({ status, assessment });
  const verdict = deriveVerdict({ status, assessment, dedupeWindow });
  const realWorldMeaning = describeRealWorldMeaning({
    verdict,
    late,
    errors,
    maxQueue,
    hasDedupeTelemetry,
    dedupeWindow,
  });
  const likelyOperatorAction = describeOperatorAction({
    verdict,
    errors,
    late,
    hasDedupeTelemetry,
    dedupeWindow,
  });

  const lines: string[] = [];
  lines.push(`Run: ${runDir.split(/[\\/]/).at(-1) ?? runDir}`);
  lines.push(`Path: ${runDir}`);
  lines.push(`Verdict: ${verdict}`);
  lines.push(`Status: ${status} | Assessment: ${assessment} | Model: ${model}`);
  lines.push(`Outcome: ${outcomeLine}`);
  lines.push(`Profile: ${profileName} | Source: ${profileSource}`);
  lines.push(
    `Timing: wall=${formatDurationMs(wallElapsedMs)} | started=${timing.startedAtIso ?? "unknown"} | finished=${timing.finishedAtIso ?? "unknown"}`,
  );
  lines.push(
    `Traffic: generated=${generated} | delivered=${delivered} | ordered=${ordered} | duplicates=${Number(simulation.duplicatesInjected ?? 0)}`,
  );
  lines.push(
    `Anomalies: total=${anomalies} | late=${late} | warn=${warnings} | error=${errors}`,
  );
  lines.push(`Queue: max=${maxQueue}`);
  if (Object.keys(dedupe).length > 0) {
    lines.push("Validation:");
    for (const line of buildDedupeValidationLines({
      dedupe,
      dedupeWindow,
      generated,
      delivered,
      duplicatesInjected: Number(simulation.duplicatesInjected ?? 0),
      peakDedupeCacheSize,
    })) {
      lines.push(`  - ${line}`);
    }

    const dedupeParts = [
      `accepted=${Number(dedupe.acceptedEvents ?? 0)}`,
      `dropped=${Number(dedupe.droppedDuplicates ?? 0)}`,
      `cache=${Number(dedupe.currentCacheSize ?? 0)}`,
      `window=${Number(dedupe.activeWindowSeconds ?? 0)}s`,
    ];
    if (dedupeWindow.configuredFloorSeconds !== null) {
      dedupeParts.push(`configured_floor=${dedupeWindow.configuredFloorSeconds}s`);
    }
    if (dedupeWindow.configuredMaxSeconds !== null) {
      dedupeParts.push(`configured_max=${dedupeWindow.configuredMaxSeconds}s`);
    }
    if (dedupeWindow.violatesConfiguredFloor) {
      dedupeParts.push("config_drift=below_floor");
    }
    lines.push(
      `Dedupe: ${dedupeParts.join(" | ")}`,
    );
  }

  if (peakRssMb !== null && lastRssMb !== null) {
    lines.push(`Memory: last_rss=${lastRssMb.toFixed(1)}MB | peak_rss=${peakRssMb.toFixed(1)}MB`);
  }

  const nodeStats = transport.nodeStats ?? {};
  if (Object.keys(nodeStats).length > 0) {
    lines.push("Nodes:");
    for (const nodeId of Object.keys(nodeStats).sort()) {
      const stats = nodeStats[nodeId] ?? {};
      lines.push(
        `  ${nodeId}: generated=${Number(stats.generated ?? 0)} | sent=${Number(stats.sent ?? 0)} | crossNodeDeps=${Number(stats.crossNodeDependencies ?? 0)} | sameNodeDeps=${Number(stats.sameNodeDependencies ?? 0)} | remoteHints=${Number(stats.remoteHintsReceived ?? 0)} | maxPending=${Number(stats.maxPendingQueueDepth ?? 0)}`,
      );
    }
  }

  if (reasons.length > 0) {
    lines.push("Why:");
    for (const reason of reasons) {
      lines.push(`  - ${reason}`);
    }
  }

  const errorBreakdown = buildErrorBreakdown(byType, errors, sampleAnomalies);
  if (errorBreakdown.length > 0) {
    lines.push("Error breakdown:");
    for (const line of errorBreakdown) {
      lines.push(`  - ${line}`);
    }
  }

  lines.push("Real-world meaning:");
  for (const line of realWorldMeaning) {
    lines.push(`  - ${line}`);
  }

  lines.push("Likely operator action:");
  for (const line of likelyOperatorAction) {
    lines.push(`  - ${line}`);
  }

  const lifecycleEvents = lifecycle
    .map((entry) => entry.event)
    .filter((entry) => typeof entry === "string");
  if (lifecycleEvents.length > 0) {
    lines.push(`Lifecycle: ${lifecycleEvents.join(", ")}`);
  }

  return lines.join("\n");
}

function resolveDedupeWindow(
  summary: JsonRecord,
  runtimeConfig: JsonRecord | null,
): {
  activeWindowSeconds: number | null;
  configuredFloorSeconds: number | null;
  configuredMaxSeconds: number | null;
  violatesConfiguredFloor: boolean;
} {
  const dedupe = summary.dedupe ?? {};
  const dedupeConfig =
    summary.config?.dedupeConfig ?? runtimeConfig?.dedupeConfig ?? {};
  const activeWindowSeconds = toFiniteNumberOrNull(dedupe.activeWindowSeconds);
  const configuredFloorSeconds = toFiniteNumberOrNull(dedupeConfig.slidingWindowSeconds);
  const configuredMaxSeconds = toFiniteNumberOrNull(
    dedupeConfig.maxSlidingWindowSeconds,
  );

  return {
    activeWindowSeconds,
    configuredFloorSeconds,
    configuredMaxSeconds,
    violatesConfiguredFloor:
      activeWindowSeconds !== null &&
      configuredFloorSeconds !== null &&
      activeWindowSeconds + 1e-9 < configuredFloorSeconds,
  };
}

function toFiniteNumberOrNull(value: unknown): number | null {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function buildDedupeValidationLines(input: {
  dedupe: JsonRecord;
  dedupeWindow: {
    activeWindowSeconds: number | null;
    configuredFloorSeconds: number | null;
    configuredMaxSeconds: number | null;
    violatesConfiguredFloor: boolean;
  };
  generated: number;
  delivered: number;
  duplicatesInjected: number;
  peakDedupeCacheSize: number;
}): string[] {
  const accepted = Number(input.dedupe.acceptedEvents ?? 0);
  const dropped = Number(input.dedupe.droppedDuplicates ?? 0);
  const currentCacheSize = Number(input.dedupe.currentCacheSize ?? 0);
  const pressureRatio = accepted > 0 ? input.peakDedupeCacheSize / accepted : 0;

  const lines: string[] = [];

  if (input.dedupeWindow.violatesConfiguredFloor) {
    lines.push(
      `dedupe config: invalid (active window ${input.dedupeWindow.activeWindowSeconds}s below configured floor ${input.dedupeWindow.configuredFloorSeconds}s)`,
    );
  } else if (
    input.dedupeWindow.activeWindowSeconds !== null &&
    input.dedupeWindow.configuredFloorSeconds !== null
  ) {
    const upperBound =
      input.dedupeWindow.configuredMaxSeconds === null
        ? "unbounded"
        : `${input.dedupeWindow.configuredMaxSeconds}s`;
    lines.push(
      `dedupe config: ok (active window ${input.dedupeWindow.activeWindowSeconds}s within configured ${input.dedupeWindow.configuredFloorSeconds}s..${upperBound})`,
    );
  } else if (input.dedupeWindow.activeWindowSeconds !== null) {
    lines.push(
      `dedupe config: observed (active window ${input.dedupeWindow.activeWindowSeconds}s)`,
    );
  }

  if (accepted === 0 && input.generated > 0) {
    lines.push("dedupe traffic: suspicious (generated load was present, but no events were accepted)");
  } else if (accepted === 0) {
    lines.push("dedupe traffic: suspicious (no accepted events were recorded)");
  } else if (input.delivered > 0 && accepted !== input.delivered) {
    lines.push(
      `dedupe traffic: suspicious (accepted ${accepted} diverged from delivered ${input.delivered})`,
    );
  } else {
    lines.push(`dedupe traffic: ok (accepted ${accepted} events)`);
  }

  if (input.duplicatesInjected > 0 && dropped === 0) {
    lines.push(
      `dedupe suppression: warning (${input.duplicatesInjected} duplicates were injected, but none were dropped)`,
    );
  } else if (input.duplicatesInjected > 0) {
    lines.push(
      `dedupe suppression: active (dropped ${dropped} duplicates with ${input.duplicatesInjected} injected)`,
    );
  } else if (dropped > 0) {
    lines.push(
      `dedupe suppression: active (dropped ${dropped} duplicates without injected-load metadata)`,
    );
  } else {
    lines.push("dedupe suppression: not exercised (no dropped duplicates were recorded)");
  }

  if (accepted === 0) {
    lines.push(`dedupe pressure: unknown (cache=${currentCacheSize}, peak=${input.peakDedupeCacheSize})`);
  } else if (pressureRatio >= 0.01) {
    lines.push(
      `dedupe pressure: warning (peak cache ${input.peakDedupeCacheSize} ids, ${formatPercent(pressureRatio)} of accepted volume)`,
    );
  } else if (pressureRatio >= 0.002) {
    lines.push(
      `dedupe pressure: noticeable (peak cache ${input.peakDedupeCacheSize} ids, ${formatPercent(pressureRatio)} of accepted volume)`,
    );
  } else {
    lines.push(
      `dedupe pressure: controlled (cache=${currentCacheSize}, peak=${input.peakDedupeCacheSize})`,
    );
  }

  return lines;
}

function assessRun(input: {
  status: string;
  errors: number;
  late: number;
  anomalies: number;
  generated: number;
  maxQueue: number;
  delivered: number;
  ordered: number;
  dedupeWindow: {
    activeWindowSeconds: number | null;
    configuredFloorSeconds: number | null;
    configuredMaxSeconds: number | null;
    violatesConfiguredFloor: boolean;
  };
}): { assessment: string; reasons: string[] } {
  const reasons: string[] = [];

  if (input.status === "failed") {
    reasons.push("the run ended with a failed status");
    return { assessment: "failed", reasons };
  }

  if (input.dedupeWindow.violatesConfiguredFloor) {
    reasons.push(
      `active dedupe window fell below the configured floor (${input.dedupeWindow.activeWindowSeconds}s < ${input.dedupeWindow.configuredFloorSeconds}s)`,
    );
  }

  if (input.errors > 0) {
    reasons.push(`${input.errors} error-level anomalies were recorded`);
  }

  if (input.late > 0 && input.delivered > 0) {
    const lateRatio = input.late / input.delivered;
    if (lateRatio >= 0.5) {
      reasons.push(
        `late arrivals were very high (${input.late} of ${input.delivered}, ${lateRatio.toLocaleString(undefined, { style: "percent", maximumFractionDigits: 0 })})`,
      );
    } else if (lateRatio >= 0.2) {
      reasons.push(
        `late arrivals were elevated (${input.late} of ${input.delivered}, ${lateRatio.toLocaleString(undefined, { style: "percent", maximumFractionDigits: 0 })})`,
      );
    }
  }

  if (input.maxQueue > 0 && input.generated > 0) {
    const queueRatio = input.maxQueue / input.generated;
    if (queueRatio >= 0.5) {
      reasons.push(
        `queue backlog grew large relative to generated volume (${input.maxQueue})`,
      );
    } else if (queueRatio >= 0.2) {
      reasons.push(`queue backlog was noticeable (${input.maxQueue})`);
    }
  }

  if (input.ordered < input.delivered) {
    reasons.push("ordered count ended below delivered count");
  }

  if (input.status === "interrupted") {
    reasons.push("the run was interrupted before a normal completion");
  }

  if (input.status === "completed" && reasons.length === 0 && input.anomalies === 0) {
    return {
      assessment: "healthy",
      reasons: ["the run completed cleanly with no recorded anomalies"],
    };
  }

  if (input.status === "completed" && reasons.length === 0) {
    return {
      assessment: "healthy",
      reasons: ["the run completed and only low-signal anomalies were recorded"],
    };
  }

  if (input.status === "completed") {
    if (input.dedupeWindow.violatesConfiguredFloor) {
      return { assessment: "invalid", reasons };
    }
    return { assessment: "degraded", reasons };
  }

  return { assessment: input.status, reasons };
}

function describeOutcome(input: { status: string; assessment: string }): string {
  if (input.status === "failed") {
    return "choked";
  }
  if (input.status === "interrupted") {
    return "interrupted before a normal finish";
  }
  if (input.status === "completed" && input.assessment === "healthy") {
    return "survived cleanly";
  }
  if (input.status === "completed" && input.assessment === "degraded") {
    return "survived but stressed";
  }
  if (input.status === "completed" && input.assessment === "invalid") {
    return "completed, but not under the configured dedupe window";
  }
  return `${input.status} (${input.assessment})`;
}

function deriveVerdict(input: {
  status: string;
  assessment: string;
  dedupeWindow: {
    activeWindowSeconds: number | null;
    configuredFloorSeconds: number | null;
    configuredMaxSeconds: number | null;
    violatesConfiguredFloor: boolean;
  };
}): string {
  if (input.status === "failed") {
    return "FAIL";
  }
  if (input.status === "interrupted") {
    return "INTERRUPTED";
  }
  if (input.status === "completed" && input.dedupeWindow.violatesConfiguredFloor) {
    return "INVALID CONFIG";
  }
  if (input.status === "completed" && input.assessment === "healthy") {
    return "PASS";
  }
  if (input.status === "completed" && input.assessment === "degraded") {
    return "PASS WITH STRESS";
  }
  return input.status.toUpperCase();
}

function buildErrorBreakdown(
  byType: JsonRecord,
  totalErrors: number,
  sampleAnomalies: JsonRecord[],
): string[] {
  if (totalErrors <= 0) {
    return [];
  }

  const observedErrorTypes = new Set(
    sampleAnomalies
      .filter((entry) => entry.severity === "error" && entry.type)
      .map((entry) => String(entry.type)),
  );

  const knownErrorTypes = ["duplicate_event", "invalid_clock", "causal_inversion"];
  const breakdown: string[] = [];

  for (const anomalyType of knownErrorTypes) {
    if (observedErrorTypes.has(anomalyType) || Number(byType[anomalyType] ?? 0) > 0) {
      breakdown.push(`${anomalyType}: ${Number(byType[anomalyType] ?? 0)}`);
    }
  }

  const otherTypes = [...observedErrorTypes]
    .filter((key) => !knownErrorTypes.includes(key) && Number(byType[key] ?? 0) > 0)
    .sort();

  for (const anomalyType of otherTypes) {
    breakdown.push(`${anomalyType}: ${Number(byType[anomalyType] ?? 0)}`);
  }

  return breakdown;
}

function describeRealWorldMeaning(input: {
  verdict: string;
  late: number;
  errors: number;
  maxQueue: number;
  hasDedupeTelemetry: boolean;
  dedupeWindow: {
    activeWindowSeconds: number | null;
    configuredFloorSeconds: number | null;
    configuredMaxSeconds: number | null;
    violatesConfiguredFloor: boolean;
  };
}): string[] {
  if (input.verdict === "INVALID CONFIG") {
    return [
      "the runtime completed, but the validation signals show the dedupe layer did not stay within the requested config",
      "the observed dedupe window drifted below the configured floor, so this run is not a trustworthy tuning result",
      "duplicate leakage and anomaly outcomes may reflect the drifted live window rather than the intended manual setting",
    ];
  }

  if (input.verdict === "PASS") {
    const meaning = [
      "the runtime stayed up and the run looked operationally healthy",
      "the ordering layer handled the simulated workload without notable distress",
    ];
    if (input.hasDedupeTelemetry && !input.dedupeWindow.violatesConfiguredFloor) {
      meaning.unshift("the validation signals stayed within the configured dedupe behavior");
    }
    return meaning;
  }

  if (input.verdict === "PASS WITH STRESS") {
    const meaning = input.hasDedupeTelemetry && !input.dedupeWindow.violatesConfiguredFloor
      ? [
          "the validation signals stayed within the configured dedupe behavior, so the degraded reading reflects workload strain rather than config drift",
          "the runtime stayed up and continued processing, but the workload exposed operational strain",
          "this is closer to a degraded production period than a clean healthy one",
        ]
      : [
          "the runtime stayed up and continued processing, but the workload exposed operational strain",
          "this is closer to a degraded production period than a clean healthy one",
        ];
    if (input.errors > 0) {
      meaning.push("error-level anomalies suggest something would deserve operator review");
    }
    if (input.late > 0) {
      meaning.push("late arrivals or replay-like behavior were visible under load");
    }
    if (input.maxQueue > 0) {
      meaning.push("backlog built up enough to be worth watching in a real deployment");
    }
    return meaning;
  }

  if (input.verdict === "FAIL") {
    return [
      "the runtime did not complete normally under the tested conditions",
      "in production this would be treated as a service-impacting failure or choke point",
    ];
  }

  if (input.verdict === "INTERRUPTED") {
    return ["the run ended before a normal completion, so health conclusions are incomplete"];
  }

  return ["the run finished in an unclassified state"];
}

function describeOperatorAction(input: {
  verdict: string;
  errors: number;
  late: number;
  hasDedupeTelemetry: boolean;
  dedupeWindow: {
    activeWindowSeconds: number | null;
    configuredFloorSeconds: number | null;
    configuredMaxSeconds: number | null;
    violatesConfiguredFloor: boolean;
  };
}): string[] {
  if (input.verdict === "INVALID CONFIG") {
    return [
      "treat the validation failure as the primary issue and rerun after fixing the dedupe window enforcement",
      "compare configured floor versus observed active window before trusting any workload interpretation",
      "do not tighten the dedupe window based on this result because the runtime already drifted below the requested floor",
    ];
  }

  if (input.verdict === "PASS") {
    if (input.hasDedupeTelemetry) {
      return [
        "record the run as healthy only after confirming the Validation section stayed within the configured dedupe behavior",
        "keep the summary for baseline comparison",
      ];
    }
    return ["record the run as healthy and keep the summary for baseline comparison"];
  }

  if (input.verdict === "PASS WITH STRESS") {
    const actions = input.hasDedupeTelemetry
      ? [
          "start with the Validation section so you confirm the dedupe config, traffic, suppression, and pressure signals before reading the anomaly totals",
          "inspect anomaly types in anomalies.ndjson before treating the profile as production-ready",
          "compare this run against cleaner baseline runs to see whether the stress level is expected",
        ]
      : [
          "this run lacks dedupe validation telemetry, so avoid treating it as a final tuning baseline",
          "inspect anomaly types in anomalies.ndjson before treating the profile as production-ready",
          "compare this run against cleaner baseline runs to see whether the stress level is expected",
        ];
    if (input.errors > 0) {
      actions.push("focus next on the error-level anomalies because they drove the degraded verdict");
    }
    if (input.late > 0) {
      actions.push("review lateness and queue settings to decide whether the profile is too harsh or the runtime is too tight");
    }
    return actions;
  }

  if (input.verdict === "FAIL") {
    return [
      "treat the run as a blocker and inspect lifecycle.ndjson, stderr logs, and anomaly output immediately",
      "reduce the profile or isolate the failing anomaly pattern before trusting a longer overnight run",
    ];
  }

  if (input.verdict === "INTERRUPTED") {
    return ["rerun the test to completion before drawing conclusions"];
  }

  return ["inspect the raw run artifacts for more detail"];
}

function formatDurationMs(milliseconds: number): string {
  if (milliseconds <= 0) {
    return "0s";
  }

  const totalSeconds = Math.floor(milliseconds / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours) {
    return `${hours}h${minutes.toString().padStart(2, "0")}m`;
  }
  if (minutes) {
    return `${minutes}m${seconds.toString().padStart(2, "0")}s`;
  }
  return `${seconds}s`;
}

function formatPercent(value: number): string {
  return value.toLocaleString(undefined, {
    style: "percent",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

main();
