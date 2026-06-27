#!/usr/bin/env node
import { readFile, readdir, stat } from "node:fs/promises";
import { resolve } from "node:path";

const RUNS_DIR = resolve("artifacts/runs");

interface RunCandidate {
  name: string;
  path: string;
  modifiedMs: number;
}

async function main(): Promise<void> {
  const latestRun = await findLatestRun(RUNS_DIR);
  if (!latestRun) {
    console.error(`No run folders found in ${RUNS_DIR}`);
    process.exitCode = 1;
    return;
  }

  const summaryPath = resolve(latestRun.path, "summary.json");
  const summary = JSON.parse(await readFile(summaryPath, "utf8")) as Record<string, any>;

  const durationMs = Number(summary.timing?.wallElapsedMs ?? 0);
  const lateCount = summary.stream?.byAnomalyType?.late_arrival ?? 0;
  const errorCount = summary.stream?.byAnomalySeverity?.error ?? 0;
  const warningCount = summary.stream?.byAnomalySeverity?.warning ?? 0;
  const delivered =
    summary.simulation?.delivered ??
    summary.transport?.receivedEvents ??
    summary.simulation?.sent ??
    0;
  const maxQueue =
    summary.simulation?.maxQueueDepth ??
    summary.simulation?.maxPendingQueueDepth ??
    0;

  console.log(
    [
      `latest=${latestRun.name}`,
      `status=${summary.outcome?.status ?? "unknown"}`,
      `wall=${formatDuration(durationMs)}`,
      `generated=${summary.simulation?.generated ?? 0}`,
      `delivered=${delivered}`,
      `ordered=${summary.stream?.orderedEvents ?? 0}`,
      `anomalies=${summary.stream?.anomalies ?? 0}`,
      `late=${lateCount}`,
      `warn=${warningCount}`,
      `error=${errorCount}`,
      `maxQueue=${maxQueue}`,
      `runDir=${latestRun.path}`,
    ].join(" | "),
  );
}

async function findLatestRun(runsDir: string): Promise<RunCandidate | null> {
  let entries;
  try {
    entries = await readdir(runsDir, { withFileTypes: true });
  } catch (error: any) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
      return null;
    }
    throw error;
  }

  const candidates: RunCandidate[] = [];

  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }

    const path = resolve(runsDir, entry.name);
    const summaryPath = resolve(path, "summary.json");

    try {
      const summaryStat = await stat(summaryPath);
      candidates.push({
        name: entry.name,
        path,
        modifiedMs: summaryStat.mtimeMs,
      });
    } catch (error: any) {
      if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
        continue;
      }
      throw error;
    }
  }

  candidates.sort((left, right) => right.modifiedMs - left.modifiedMs);
  return candidates[0] ?? null;
}

function formatDuration(milliseconds: number): string {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) {
    return "0s";
  }

  const totalSeconds = Math.floor(milliseconds / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}h${minutes.toString().padStart(2, "0")}m`;
  }
  if (minutes > 0) {
    return `${minutes}m${seconds.toString().padStart(2, "0")}s`;
  }
  return `${seconds}s`;
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
