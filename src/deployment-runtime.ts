#!/usr/bin/env node
import { type ChildProcess, fork } from "node:child_process";
import { createWriteStream, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, extname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  HELP_TEXT,
  buildConfig,
  buildRunArtifacts,
  formatOperatorError,
  formatDuration,
  serializeConfig,
  type RuntimeArtifacts,
  type RuntimeConfig,
} from "./deployment-common.js";

interface RuntimeConfigWithArtifacts extends RuntimeConfig {
  wallStartMs: number;
  artifacts: RuntimeArtifacts;
}

const currentModulePath = fileURLToPath(import.meta.url);
const currentModuleDir = dirname(currentModulePath);
const currentModuleExt = extname(currentModulePath);

async function main(): Promise<void> {
  const maybeConfig = buildConfig(process.argv.slice(2));
  if ("help" in maybeConfig) {
    console.log(HELP_TEXT);
    return;
  }

  const config = maybeConfig;
  const wallStartMs = Date.now() + 2_000;
  const artifacts = buildRunArtifacts(config, wallStartMs);
  const runtimeConfig: RuntimeConfigWithArtifacts = {
    ...config,
    wallStartMs,
    artifacts,
  };
  const serializedConfig = JSON.stringify(serializeConfig(runtimeConfig));

  mkdirSync(artifacts.runDir, { recursive: true });
  mkdirSync(artifacts.nodesDir, { recursive: true });
  writeFileSync(
    artifacts.configPath,
    `${JSON.stringify(serializeConfig(runtimeConfig), null, 2)}\n`,
    "utf8",
  );

  const orchestratorLog = createWriteStream(artifacts.orchestratorLogPath, {
    flags: "a",
  });
  const log = (message: string) => {
    const line = `${new Date().toISOString()} ${message}\n`;
    process.stdout.write(line);
    orchestratorLog.write(line);
  };

  log(`starting deployment-style runtime in ${artifacts.runDir}`);
  log(
    [
      `duration=${formatDuration(config.durationMs)}`,
      `steady=${formatDuration(config.steadyForMs)}`,
      `rate=${config.eventsPerSecond}/s`,
      `chaosMultiplier=${config.chaosMultiplier}`,
      `latePolicy=${config.lateArrivalPolicy}`,
      `timeScale=${config.timeScale}x`,
    ].join(" | "),
  );

  const children: ChildProcess[] = [];
  const cleanup = (signal: NodeJS.Signals) => {
    for (const child of children) {
      if (!child.killed) {
        child.kill(signal);
      }
    }
  };

  process.once("SIGINT", () => cleanup("SIGINT"));
  process.once("SIGTERM", () => cleanup("SIGTERM"));

  const collector = spawnChild({
    name: "collector",
    script: siblingScriptPath("deployment-collector"),
    serializedConfig,
    stdoutPath: artifacts.collectorStdoutPath,
    stderrPath: artifacts.collectorStderrPath,
    log,
  });
  children.push(collector.child);

  const collectorReady = await waitForCollectorReady(collector.child);
  log(`collector ready on 127.0.0.1:${collectorReady.port}`);

  const nodeChildren = runtimeConfig.nodeIds.map((nodeId) => {
    const nodeLabel = resolve(artifacts.nodesDir, `${nodeId}.stdout.log`);
    const nodeErr = resolve(artifacts.nodesDir, `${nodeId}.stderr.log`);
    const childInfo = spawnChild({
      name: nodeId,
      script: siblingScriptPath("deployment-node"),
      serializedConfig,
      stdoutPath: nodeLabel,
      stderrPath: nodeErr,
      log,
      extraEnv: {
        RUNTIME_NODE_ID: nodeId,
        RUNTIME_COLLECTOR_PORT: String(collectorReady.port),
      },
    });
    children.push(childInfo.child);
    return childInfo.child;
  });

  const collectorExitPromise = waitForExit(collector.child, "collector");
  const nodeExitPromises = nodeChildren.map((child, index) =>
    waitForExit(child, runtimeConfig.nodeIds[index]),
  );

  try {
    await Promise.all(
      [collectorExitPromise, ...nodeExitPromises].map((promise) =>
        promise.then((result) => {
          if (result.code !== 0) {
            throw new Error(
              `${result.name} exited unexpectedly with code=${result.code}`,
            );
          }
          return result;
        }),
      ),
    );
    log("all processes completed cleanly");
  } catch (error) {
    log(formatOperatorError(error));
    cleanup("SIGTERM");
    await Promise.allSettled([collectorExitPromise, ...nodeExitPromises]);
    writeFallbackSummary(runtimeConfig, error);
    process.exitCode = 1;
  }
}

function spawnChild({
  name,
  script,
  serializedConfig,
  stdoutPath,
  stderrPath,
  log,
  extraEnv = {},
}: {
  name: string;
  script: string;
  serializedConfig: string;
  stdoutPath: string;
  stderrPath: string;
  log: (message: string) => void;
  extraEnv?: Record<string, string>;
}): { child: ChildProcess; stdoutStream: NodeJS.WritableStream; stderrStream: NodeJS.WritableStream } {
  mkdirSync(dirname(stdoutPath), { recursive: true });
  mkdirSync(dirname(stderrPath), { recursive: true });

  const stdoutStream = createWriteStream(stdoutPath, { flags: "a" });
  const stderrStream = createWriteStream(stderrPath, { flags: "a" });
  const child = fork(script, {
    cwd: process.cwd(),
    env: {
      ...process.env,
      RUNTIME_DEPLOYMENT_CONFIG: serializedConfig,
      ...extraEnv,
    },
    silent: true,
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });

  child.stdout?.pipe(stdoutStream);
  child.stderr?.pipe(stderrStream);
  log(`spawned ${name} pid=${child.pid}`);

  return { child, stdoutStream, stderrStream };
}

function siblingScriptPath(baseName: string): string {
  return resolve(currentModuleDir, `${baseName}${currentModuleExt}`);
}

function waitForCollectorReady(child: ChildProcess): Promise<{ port: number }> {
  return new Promise((resolveReady, rejectReady) => {
    const onMessage = (message: any) => {
      if (message?.type === "collector_ready") {
        child.off("message", onMessage);
        resolveReady(message);
      }
    };

    child.on("message", onMessage);
    child.once("exit", (code) => {
      rejectReady(new Error(`collector exited before ready with code ${code}`));
    });
  });
}

function waitForExit(
  child: ChildProcess,
  name: string,
): Promise<{ name: string; code: number; signal: NodeJS.Signals | null }> {
  return new Promise((resolveExit) => {
    child.once("exit", (code, signal) => {
      resolveExit({ name, code: code ?? 0, signal });
    });
  });
}

function writeFallbackSummary(
  config: RuntimeConfigWithArtifacts,
  error: unknown,
): void {
  if (existsSync(config.artifacts.summaryPath)) {
    return;
  }

  const lifecycle = loadNdjson(config.artifacts.lifecyclePath);
  const heartbeats = loadNdjson(config.artifacts.heartbeatPath);
  const anomalies = loadNdjson(config.artifacts.anomalyPath);

  const connectedNodes: Record<string, boolean> = {};
  const nodeStats: Record<string, Record<string, unknown>> = {};
  for (const row of lifecycle) {
    if (row.event === "node_connected" && typeof row.nodeId === "string") {
      connectedNodes[row.nodeId] = true;
    }
    if (row.event === "node_completed" && typeof row.nodeId === "string") {
      nodeStats[row.nodeId] =
        row.stats && typeof row.stats === "object"
          ? (row.stats as Record<string, unknown>)
          : {};
    }
  }

  const byAnomalyType: Record<string, number> = {};
  const byAnomalySeverity: Record<string, number> = {};
  const anomalySamples: Array<Record<string, unknown>> = [];
  for (const row of anomalies) {
    const type = typeof row.type === "string" ? row.type : "unknown";
    const severity = typeof row.severity === "string" ? row.severity : "warning";
    byAnomalyType[type] = (byAnomalyType[type] ?? 0) + 1;
    byAnomalySeverity[severity] = (byAnomalySeverity[severity] ?? 0) + 1;
    if (anomalySamples.length < config.sampleLimit) {
      anomalySamples.push({
        type,
        severity,
        eventId: row.eventId ?? null,
        relatedEventIds: Array.isArray(row.relatedEventIds) ? row.relatedEventIds : [],
        message: row.message ?? "",
      });
    }
  }

  const lastHeartbeat = heartbeats.at(-1) ?? {};
  const maxRssBytes = Math.max(
    0,
    ...heartbeats.map((row) => Number(row.rssBytes ?? 0)),
  );
  const completedNodeStats = Object.values(nodeStats);
  const dedupe = (lastHeartbeat.dedupe ?? {}) as Record<string, unknown>;

  const summary = {
    outcome: {
      status: "failed",
      failure: serializeFailure(error),
    },
    artifacts: {
      runDir: config.artifacts.runDir,
      summaryPath: config.artifacts.summaryPath,
      heartbeatPath: config.artifacts.heartbeatPath,
      anomalyPath: config.artifacts.anomalyPath,
      duplicateLeakPath: config.artifacts.duplicateLeakPath,
      lifecyclePath: config.artifacts.lifecyclePath,
      configPath: config.artifacts.configPath,
    },
    config: {
      durationMs: config.durationMs.toString(),
      steadyForMs: config.steadyForMs.toString(),
      eventsPerSecond: config.eventsPerSecond,
      chaosMultiplier: config.chaosMultiplier,
      batchSize: config.batchSize,
      maxLateArrivalMs: config.maxLateArrivalMs.toString(),
      maxTailDrainMs: config.maxTailDrainMs.toString(),
      lateArrivalPolicy: config.lateArrivalPolicy,
      reportEveryMs: config.reportEveryMs.toString(),
      timeScale: config.timeScale,
      outputDir: config.outputDir,
      runName: config.runName,
      strict: config.strict,
      allowUnknownOrder: config.allowUnknownOrder,
      detectAnomalies: config.detectAnomalies,
      tieBreaker: config.tieBreaker,
      nodeIds: config.nodeIds,
      model: "deployment_local_tcp",
      profileName: config.workloadProfile?.name ?? "unknown",
      profileDescription: config.workloadProfile?.description ?? "",
      profileSource: config.profileSource ?? null,
    },
    timing: {
      startedAtIso: new Date(config.wallStartMs).toISOString(),
      finishedAtIso: new Date().toISOString(),
      wallElapsedMs: Math.max(0, Date.now() - config.wallStartMs),
      simulatedElapsedMs: String(lastHeartbeat.simulatedElapsedMs ?? "0"),
      interrupted: false,
    },
    transport: {
      receivedEvents: Number(lastHeartbeat.received ?? 0),
      receivedByNode: {},
      connectedNodes,
      peerHintsBroadcast: 0,
      nodeStats,
      persistedLateArrivals: 0,
    },
    stream: {
      batches: 0,
      correctionBatches: 0,
      finalBatches: 0,
      orderedEvents: Number(lastHeartbeat.ordered ?? 0),
      anomalies: Number(lastHeartbeat.anomalies ?? anomalies.length),
      maxWatermarkMs: "0",
      lastWatermarkMs: "0",
      byAnomalyType,
      byAnomalySeverity,
      byOrderBasis: {},
      byConfidence: {},
    },
    dedupe,
    simulation: {
      generated: sumBy(completedNodeStats, "generated"),
      delivered: Number(lastHeartbeat.received ?? 0),
      sent: sumBy(completedNodeStats, "sent"),
      duplicatesInjected: sumBy(completedNodeStats, "duplicatesInjected"),
      sameNodeDependencies: sumBy(completedNodeStats, "sameNodeDependencies"),
      crossNodeDependencies: sumBy(completedNodeStats, "crossNodeDependencies"),
      remoteHintsReceived: sumBy(completedNodeStats, "remoteHintsReceived"),
      maxPendingQueueDepth: sumMaxBy(completedNodeStats, "maxPendingQueueDepth"),
      maxQueueDepth: sumMaxBy(completedNodeStats, "maxPendingQueueDepth"),
    },
    samples: {
      anomalies: anomalySamples,
      corrections: [],
    },
    fallback: {
      synthesizedBy: "deployment-runtime",
      reason: "collector exited before writing summary.json",
      maxRssBytes,
    },
  };

  writeFileSync(
    config.artifacts.summaryPath,
    `${JSON.stringify(summary, null, 2)}\n`,
    "utf8",
  );
}

function loadNdjson(path: string): Array<Record<string, unknown>> {
  if (!existsSync(path)) {
    return [];
  }

  const rows: Array<Record<string, unknown>> = [];
  for (const line of readFileSync(path, "utf8").split(/\r?\n/)) {
    if (!line.trim()) {
      continue;
    }
    rows.push(JSON.parse(line) as Record<string, unknown>);
  }
  return rows;
}

function serializeFailure(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack ?? null,
    };
  }
  return {
    name: "Error",
    message: String(error),
    stack: null,
  };
}

function sumBy(values: Array<Record<string, unknown>>, key: string): number {
  return values.reduce((total, entry) => total + Number(entry[key] ?? 0), 0);
}

function sumMaxBy(values: Array<Record<string, unknown>>, key: string): number {
  return values.reduce((maxValue, entry) => Math.max(maxValue, Number(entry[key] ?? 0)), 0);
}

main().catch((error) => {
  console.error(formatOperatorError(error));
  process.exitCode = 1;
});
