#!/usr/bin/env node
import { existsSync, readdirSync, statSync } from "node:fs";
import { readFile } from "node:fs/promises";
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
      const duplicateLeakPath = resolveDuplicateLeakPath(runDir, summary);
      const leakRows = await loadNdjson(duplicateLeakPath);
      console.log(buildReport(runDir, duplicateLeakPath, summary, leakRows));
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}

function printHelp(): void {
  console.log("usage: causal-order-testing-duplicates [path]");
  console.log("");
  console.log("Print duplicate leak diagnostics for the latest or selected run.");
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

function resolveDuplicateLeakPath(runDir: string, summary: JsonRecord): string {
  const configuredPath = summary.artifacts?.duplicateLeakPath;
  if (typeof configuredPath === "string" && configuredPath.trim().length > 0) {
    return resolve(configuredPath);
  }
  return resolve(runDir, "duplicate-leaks.ndjson");
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

function buildReport(
  runDir: string,
  duplicateLeakPath: string,
  summary: JsonRecord,
  leakRows: JsonRecord[],
): string {
  const runLabel = summary.artifacts?.runDir
    ? String(summary.artifacts.runDir).split(/[\\/]/).at(-1)
    : runDir.split(/[\\/]/).at(-1) ?? runDir;
  const delivered = Number(
    summary.simulation?.delivered ??
      summary.transport?.receivedEvents ??
      summary.stream?.orderedEvents ??
      0,
  );
  const duplicateErrors = Number(summary.stream?.byAnomalyType?.duplicate_event ?? 0);

  const lines = [
    `Run: ${runLabel}`,
    `Path: ${runDir}`,
    `Diagnostics: ${duplicateLeakPath}`,
  ];

  if (leakRows.length === 0) {
    if (duplicateErrors > 0) {
      lines.push(
        `Duplicate leak diagnostics not available for this run (instrumentation file missing), but summary reports ${duplicateErrors} duplicate_event anomalies.`,
      );
    } else {
      lines.push("Duplicate leaks: none observed");
    }
    return lines.join("\n");
  }

  const exceedingWindow = leakRows.filter(
    (row) => row.seenGapExceedsActiveWindowAtRepeat === true,
  ).length;
  const maxGapMs = leakRows.reduce((maxValue, row) => {
    const gap = toNumber(row.seenGapMs);
    return gap > maxValue ? gap : maxValue;
  }, 0);
  const byNode = new Map<string, number>();
  for (const row of leakRows) {
    const nodeId = String(row.nodeId ?? "unknown");
    byNode.set(nodeId, (byNode.get(nodeId) ?? 0) + 1);
  }

  lines.push(
    [
      `Duplicate leaks: total=${leakRows.length}`,
      `delivered=${delivered}`,
      `rate=${formatRatio(leakRows.length, delivered)}`,
      `gapOverWindow=${exceedingWindow}`,
      `maxGap=${formatDurationMs(maxGapMs)}`,
    ].join(" | "),
  );
  lines.push(
    `By node: ${[...byNode.entries()].map(([nodeId, count]) => `${nodeId}=${count}`).join(" | ")}`,
  );
  const shutdownClusterNote = buildShutdownClusterNote(summary, leakRows);
  if (shutdownClusterNote) {
    lines.push(shutdownClusterNote);
  }
  lines.push("Details:");

  for (const row of leakRows) {
    lines.push(
      [
        `- ${row.eventId ?? "unknown"}`,
        `node=${row.nodeId ?? "unknown"}`,
        `first=${row.firstSeen?.timestampIso ?? "unknown"}`,
        `repeat=${row.repeatedSeen?.timestampIso ?? "unknown"}`,
        `gap=${formatDurationMs(toNumber(row.seenGapMs))}`,
        `window=${formatSeconds(row.activeWindowSecondsAtRepeat)}`,
        `gap>window=${formatBoolean(row.seenGapExceedsActiveWindowAtRepeat)}`,
      ].join(" | "),
    );
  }

  return lines.join("\n");
}

function toNumber(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function formatRatio(numerator: number, denominator: number): string {
  if (!Number.isFinite(denominator) || denominator <= 0) {
    return "n/a";
  }
  return (numerator / denominator).toLocaleString(undefined, {
    style: "percent",
    maximumFractionDigits: 4,
  });
}

function formatSeconds(value: unknown): string {
  const seconds = toNumber(value);
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return "n/a";
  }
  if (Number.isInteger(seconds)) {
    return `${seconds}s`;
  }
  return `${seconds.toFixed(3)}s`;
}

function formatBoolean(value: unknown): string {
  if (value === true) {
    return "yes";
  }
  if (value === false) {
    return "no";
  }
  return "unknown";
}

function formatDurationMs(milliseconds: number): string {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) {
    return "0s";
  }

  const totalSeconds = Math.floor(milliseconds / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}h${minutes.toString().padStart(2, "0")}m${seconds.toString().padStart(2, "0")}s`;
  }
  if (minutes > 0) {
    return `${minutes}m${seconds.toString().padStart(2, "0")}s`;
  }
  if (totalSeconds > 0) {
    return `${totalSeconds}s`;
  }
  return `${milliseconds}ms`;
}

function buildShutdownClusterNote(
  summary: JsonRecord,
  leakRows: JsonRecord[],
): string | null {
  const finishedAt = Date.parse(String(summary.timing?.finishedAtIso ?? ""));
  if (!Number.isFinite(finishedAt)) {
    return null;
  }

  const repeatTimes = leakRows
    .map((row) => Date.parse(String(row.repeatedSeen?.timestampIso ?? "")))
    .filter((value) => Number.isFinite(value));

  if (repeatTimes.length !== leakRows.length || repeatTimes.length === 0) {
    return null;
  }

  const allNearShutdown = repeatTimes.every(
    (value) => finishedAt - value >= 0 && finishedAt - value <= 5_000,
  );
  if (!allNearShutdown) {
    return null;
  }

  return "Observation: all leaked duplicates clustered in the final drain just before collector shutdown.";
}

main();
