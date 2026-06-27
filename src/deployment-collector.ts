import { appendFile, writeFile } from "node:fs/promises";
import { createServer, type Socket } from "node:net";

import { orderEventStream } from "causal-order";

import { DedupeGateway } from "@causal-order/dedupe";
import {
  createSimulationClock,
  deserializeConfig,
  deserializeEventFromWire,
  formatDuration,
  serializeHintForWire,
  type RuntimeConfig,
  type SimulationEvent,
} from "./deployment-common.js";

type JsonRecord = Record<string, any>;

const rawConfig = process.env.RUNTIME_DEPLOYMENT_CONFIG;
if (!rawConfig) {
  throw new Error("Missing RUNTIME_DEPLOYMENT_CONFIG");
}

const config = deserializeConfig(JSON.parse(rawConfig) as JsonRecord) as RuntimeConfig & {
  wallStartMs: number;
  artifacts: {
    summaryPath: string;
    heartbeatPath: string;
    anomalyPath: string;
    duplicateLeakPath: string;
    lifecyclePath: string;
    configPath: string;
    runDir: string;
  };
};
const clock = createSimulationClock(config);

const dedupeGate = new DedupeGateway({
  ...config.dedupeConfig,
  nowProvider: () => clock.simulationNowMs(),
});

const expectedNodes = new Set(config.nodeIds);
const completedNodes = new Map<string, JsonRecord>();
const socketsByNode = new Map<string, Socket>();
const incoming = createAsyncQueue<SimulationEvent>();
const pendingBySocket = new WeakMap<Socket, string>();
const summary = createSummary();

let stopRequested = false;
let heartbeatTimer: NodeJS.Timeout | null = null;

process.once("SIGINT", () => {
  stopRequested = true;
});
process.once("SIGTERM", () => {
  stopRequested = true;
});

const server = createServer((socket) => {
  pendingBySocket.set(socket, "");
  socket.setEncoding("utf8");

  socket.on("data", (chunk) => {
    const nextBuffer = (pendingBySocket.get(socket) ?? "") + chunk;
    const lines = nextBuffer.split("\n");
    pendingBySocket.set(socket, lines.pop() ?? "");
    for (const line of lines) {
      if (!line.trim()) {
        continue;
      }
      handleMessage(socket, JSON.parse(line) as JsonRecord);
    }
  });

  socket.on("error", (error) => {
    log(`socket error: ${error.message}`);
  });

  socket.on("close", () => {
    for (const [nodeId, candidate] of socketsByNode.entries()) {
      if (candidate === socket) {
        socketsByNode.delete(nodeId);
      }
    }
  });
});

await new Promise<void>((resolveListen) => {
  server.listen(0, "127.0.0.1", resolveListen);
});

const address = server.address();
if (!address || typeof address === "string") {
  throw new Error("Failed to determine collector port");
}

process.send?.({
  type: "collector_ready",
  port: address.port,
});

await appendLifecycleEvent("collector_ready", { port: address.port });

heartbeatTimer = setInterval(() => {
  appendHeartbeat("progress").catch(() => {});
}, Math.max(1_000, Math.ceil(Number(config.reportEveryMs) / config.timeScale)));

try {
  for await (const batch of orderEventStream(source() as any, {
    batchSize: config.batchSize,
    maxLateArrivalMs: config.maxLateArrivalMs,
    lateArrivalPolicy: config.lateArrivalPolicy,
    strict: config.strict,
    allowUnknownOrder: config.allowUnknownOrder,
    detectAnomalies: config.detectAnomalies,
    tieBreaker: config.tieBreaker as any,
  })) {
    await ingestBatch(batch as JsonRecord);
    await maybePrintProgress();

    if (stopRequested && completedNodes.size === expectedNodes.size) {
      incoming.close();
    }
  }

  summary.outcome.status = stopRequested ? "interrupted" : "completed";
} catch (error) {
  summary.outcome.status = "failed";
  summary.outcome.failure = serializeError(error);
  await appendLifecycleEvent("collector_failed", {
    error: summary.outcome.failure,
  });
  throw error;
} finally {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
  }
  finalizeSummary();
  await appendHeartbeat("final");
  dedupeGate.destroy();
  server.close();
  await writeFile(
    config.artifacts.summaryPath,
    `${JSON.stringify(summary, null, 2)}\n`,
    "utf8",
  );
  await appendLifecycleEvent("collector_finished", {
    status: summary.outcome.status,
  });
  log(
    [
      "collector complete",
      `status=${summary.outcome.status}`,
      `received=${summary.transport.receivedEvents}`,
      `ordered=${summary.stream.orderedEvents}`,
      `anomalies=${summary.stream.anomalies}`,
    ].join(" | "),
  );
}

async function* source(): AsyncGenerator<SimulationEvent> {
  for await (const event of incoming) {
    yield event;
  }
}

function handleMessage(socket: Socket, message: JsonRecord): void {
  switch (message.type) {
    case "hello":
      socketsByNode.set(message.nodeId, socket);
      summary.transport.connectedNodes[message.nodeId] = true;
      appendLifecycleEvent("node_connected", { nodeId: message.nodeId }).catch(() => {});
      return;

    case "event": {
      const deserializedEvent = deserializeEventFromWire(message.event);

      if (!dedupeGate.filter(deserializedEvent)) {
        return;
      }

      receiveEvent(deserializedEvent);
      return;
    }

    case "complete":
      completedNodes.set(message.nodeId, message.stats ?? {});
      summary.transport.nodeStats[message.nodeId] = message.stats ?? {};
      appendLifecycleEvent("node_completed", {
        nodeId: message.nodeId,
        stats: message.stats ?? {},
      }).catch(() => {});
      if (completedNodes.size === expectedNodes.size) {
        incoming.close();
      }
      return;

    case "fault_state":
      countInto(summary.transport.faultEventsByType, message.state ?? "unknown");
      appendLifecycleEvent("node_fault_state", {
        nodeId: message.nodeId,
        state: message.state ?? "unknown",
      }).catch(() => {});
      return;

    default:
      log(`unknown message type: ${message.type}`);
  }
}

function receiveEvent(event: SimulationEvent): void {
  summary.transport.receivedEvents += 1;
  countInto(summary.transport.receivedByNode, event.nodeId);
  incoming.push(event);

  const hint = {
    type: "peer_event",
    event: serializeHintForWire({
      id: event.id,
      nodeId: event.nodeId,
      clock: event.clock,
      traceId: event.traceId ?? null,
      entityId: event.payload?.entityId ?? null,
    }),
  };

  for (const [nodeId, socket] of socketsByNode.entries()) {
    if (nodeId === event.nodeId) {
      continue;
    }
    socket.write(`${JSON.stringify(hint)}\n`);
    summary.transport.peerHintsBroadcast += 1;
  }
}

async function ingestBatch(batch: JsonRecord): Promise<void> {
  summary.stream.batches += 1;
  summary.stream.orderedEvents += batch.events.length;
  summary.stream.anomalies += batch.anomalies.length;
  summary.stream.lastWatermarkMs = batch.watermark.toString();

  if (BigInt(summary.stream.maxWatermarkMs) < batch.watermark) {
    summary.stream.maxWatermarkMs = batch.watermark.toString();
  }

  if (batch.correction) {
    summary.stream.correctionBatches += 1;
    pushLimited(
      summary.samples.corrections,
      {
        triggerEventId: batch.correction.triggerEventId,
        reason: batch.correction.reason,
        scope: batch.correction.scope,
        watermarkMs: batch.watermark.toString(),
      },
      config.sampleLimit,
    );
  }

  if (batch.isFinal) {
    summary.stream.finalBatches += 1;
  }

  for (const orderedEvent of batch.events) {
    countInto(summary.stream.byOrderBasis, orderedEvent.orderBasis);
    countInto(summary.stream.byConfidence, orderedEvent.confidence);
  }

  const anomalyRecords: JsonRecord[] = [];
  const duplicateLeakRecords: JsonRecord[] = [];
  for (const anomaly of batch.anomalies) {
    countInto(summary.stream.byAnomalyType, anomaly.type);
    countInto(summary.stream.byAnomalySeverity, anomaly.severity);
    pushLimited(
      summary.samples.anomalies,
      {
        type: anomaly.type,
        severity: anomaly.severity,
        eventId: anomaly.event?.id ?? null,
        relatedEventIds: anomaly.relatedEvents?.map((entry: JsonRecord) => entry.id) ?? [],
        message: anomaly.message,
      },
      config.sampleLimit,
    );

    if (
      anomaly.type === "late_arrival" &&
      anomaly.event?.ingestedAt &&
      anomaly.event?.clock?.physicalTimeMs
    ) {
      const latencyMs = Number(
        anomaly.event.ingestedAt - BigInt(anomaly.event.clock.physicalTimeMs),
      );
      if (latencyMs > 0) {
        const dynamicWindowSeconds = (latencyMs / 1000) * 1.2;
        dedupeGate.updateWindow(dynamicWindowSeconds);
      }
    }

    if (anomaly.type === "duplicate_event") {
      const duplicateLeakRecord = buildDuplicateLeakRecord(anomaly);
      if (duplicateLeakRecord) {
        duplicateLeakRecords.push(duplicateLeakRecord);
      }
    }

    if (shouldPersistAnomaly(anomaly)) {
      anomalyRecords.push({
        timestampIso: new Date().toISOString(),
        type: anomaly.type,
        severity: anomaly.severity,
        eventId: anomaly.event?.id ?? null,
        nodeId: anomaly.event?.nodeId ?? null,
        relatedEventIds:
          anomaly.relatedEvents?.map((entry: JsonRecord) => entry.id) ?? [],
        message: anomaly.message,
      });
    }
  }

  if (anomalyRecords.length > 0) {
    await appendFile(
      config.artifacts.anomalyPath,
      anomalyRecords.map((entry) => JSON.stringify(entry)).join("\n") + "\n",
      "utf8",
    );
  }

  if (duplicateLeakRecords.length > 0) {
    await appendFile(
      config.artifacts.duplicateLeakPath,
      duplicateLeakRecords.map((entry) => JSON.stringify(entry)).join("\n") + "\n",
      "utf8",
    );
  }
}

function shouldPersistAnomaly(anomaly: JsonRecord): boolean {
  if (anomaly.type !== "late_arrival") {
    return true;
  }
  if (summary.transport.persistedLateArrivals >= config.maxLateArrivalSamples) {
    return false;
  }
  summary.transport.persistedLateArrivals += 1;
  return true;
}

async function maybePrintProgress(): Promise<void> {
  const now = clock.simulationNowMs();
  if (now < summary.runtime.nextReportAtMs) {
    return;
  }

  summary.runtime.nextReportAtMs += config.reportEveryMs;
  log(
    [
      `[sim ${formatDuration(now - clock.simulatedStartMs)}]`,
      `received=${summary.transport.receivedEvents}`,
      `ordered=${summary.stream.orderedEvents}`,
      `anomalies=${summary.stream.anomalies}`,
      `late=${summary.stream.byAnomalyType.late_arrival ?? 0}`,
      `connected=${Object.keys(summary.transport.connectedNodes).length}`,
      `completed=${completedNodes.size}`,
    ].join(" "),
  );
}

async function appendHeartbeat(kind: string): Promise<void> {
  const memory = process.memoryUsage();
  const record = {
    timestampIso: new Date().toISOString(),
    kind,
    status: summary.outcome.status,
    wallElapsedMs: Date.now() - config.wallStartMs,
    simulatedElapsedMs: (
      clock.simulationNowMs() - clock.simulatedStartMs
    ).toString(),
    received: summary.transport.receivedEvents,
    ordered: summary.stream.orderedEvents,
    anomalies: summary.stream.anomalies,
    lateAnomalies: summary.stream.byAnomalyType.late_arrival ?? 0,
    connectedNodes: Object.keys(summary.transport.connectedNodes).length,
    completedNodes: completedNodes.size,
    dedupe: dedupeGate.getStats(),
    rssBytes: memory.rss,
    heapUsedBytes: memory.heapUsed,
    heapTotalBytes: memory.heapTotal,
  };

  await appendFile(
    config.artifacts.heartbeatPath,
    `${JSON.stringify(record)}\n`,
    "utf8",
  );
}

async function appendLifecycleEvent(
  event: string,
  details: JsonRecord = {},
): Promise<void> {
  await appendFile(
    config.artifacts.lifecyclePath,
    `${JSON.stringify({
      timestampIso: new Date().toISOString(),
      event,
      wallElapsedMs: Date.now() - config.wallStartMs,
      simulatedElapsedMs: (
        clock.simulationNowMs() - clock.simulatedStartMs
      ).toString(),
      ...details,
    })}\n`,
    "utf8",
  );
}

function createSummary(): JsonRecord {
  return {
    outcome: {
      status: "running",
      failure: null,
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
      faultInjection: {
        darkNodeIds: config.faultInjection.darkNodeIds,
        darkIntervalMs: config.faultInjection.darkIntervalMs.toString(),
        darkDurationMs: config.faultInjection.darkDurationMs.toString(),
        darkStartAfterMs: config.faultInjection.darkStartAfterMs.toString(),
        darkStaggerMs: config.faultInjection.darkStaggerMs.toString(),
        jitterNodeIds: config.faultInjection.jitterNodeIds,
        jitterExtraDelayMinMs:
          config.faultInjection.jitterExtraDelayMinMs.toString(),
        jitterExtraDelayMaxMs:
          config.faultInjection.jitterExtraDelayMaxMs.toString(),
        jitterSpikeChance: config.faultInjection.jitterSpikeChance,
        jitterSpikeMinMs: config.faultInjection.jitterSpikeMinMs.toString(),
        jitterSpikeMaxMs: config.faultInjection.jitterSpikeMaxMs.toString(),
      },
      model: "deployment_local_tcp",
      profileName: config.workloadProfile?.name ?? "unknown",
      profileDescription: config.workloadProfile?.description ?? "",
      profileSource: config.profileSource ?? null,
    },
    timing: {
      startedAtIso: new Date(config.wallStartMs).toISOString(),
      finishedAtIso: null,
      wallElapsedMs: null,
      simulatedElapsedMs: null,
      interrupted: false,
    },
    transport: {
      receivedEvents: 0,
      receivedByNode: {} as Record<string, number>,
      connectedNodes: {} as Record<string, boolean>,
      peerHintsBroadcast: 0,
      nodeStats: {} as Record<string, JsonRecord>,
      persistedLateArrivals: 0,
      faultEventsByType: {} as Record<string, number>,
    },
    stream: {
      batches: 0,
      correctionBatches: 0,
      finalBatches: 0,
      orderedEvents: 0,
      anomalies: 0,
      maxWatermarkMs: "0",
      lastWatermarkMs: "0",
      byAnomalyType: {} as Record<string, number>,
      byAnomalySeverity: {} as Record<string, number>,
      byOrderBasis: {} as Record<string, number>,
      byConfidence: {} as Record<string, number>,
    },
    dedupe: dedupeGate.getStats(),
    runtime: {
      nextReportAtMs: clock.simulationNowMs() + config.reportEveryMs,
    },
    samples: {
      anomalies: [] as JsonRecord[],
      corrections: [] as JsonRecord[],
    },
  };
}

function finalizeSummary(): void {
  summary.dedupe = dedupeGate.getStats();
  const nodeStats = Object.values(summary.transport.nodeStats);
  summary.simulation = {
    generated: sumBy(nodeStats, "generated"),
    delivered: summary.transport.receivedEvents,
    sent: sumBy(nodeStats, "sent"),
    duplicatesInjected: sumBy(nodeStats, "duplicatesInjected"),
    sameNodeDependencies: sumBy(nodeStats, "sameNodeDependencies"),
    crossNodeDependencies: sumBy(nodeStats, "crossNodeDependencies"),
    remoteHintsReceived: sumBy(nodeStats, "remoteHintsReceived"),
    maxPendingQueueDepth: maxBy(nodeStats, "maxPendingQueueDepth"),
    maxQueueDepth: maxBy(nodeStats, "maxPendingQueueDepth"),
    darkWindowsEntered: sumBy(nodeStats, "darkWindowsEntered"),
    reconnects: sumBy(nodeStats, "reconnects"),
    connectionOpens: sumBy(nodeStats, "connectionOpens"),
    jitterExtraDelaysApplied: sumBy(nodeStats, "jitterExtraDelaysApplied"),
    jitterSpikeDelaysApplied: sumBy(nodeStats, "jitterSpikeDelaysApplied"),
  };
  summary.timing.finishedAtIso = new Date().toISOString();
  summary.timing.wallElapsedMs = Date.now() - config.wallStartMs;
  summary.timing.simulatedElapsedMs = (
    clock.simulationNowMs() - clock.simulatedStartMs
  ).toString();
  summary.timing.interrupted = stopRequested;
  delete summary.runtime;
}

function sumBy(values: JsonRecord[], key: string): number {
  return values.reduce((total, entry) => total + Number(entry?.[key] ?? 0), 0);
}

function maxBy(values: JsonRecord[], key: string): number {
  return values.reduce(
    (maxValue, entry) => Math.max(maxValue, Number(entry?.[key] ?? 0)),
    0,
  );
}

function countInto(bucket: Record<string, number>, key: string): void {
  bucket[key] = (bucket[key] ?? 0) + 1;
}

function pushLimited(bucket: JsonRecord[], value: JsonRecord, limit: number): void {
  if (bucket.length < limit) {
    bucket.push(value);
  }
}

function log(message: string): void {
  process.stdout.write(`${message}\n`);
}

function buildDuplicateLeakRecord(anomaly: JsonRecord): JsonRecord | null {
  const repeatedEvent = anomaly.event as JsonRecord | undefined;
  const firstEvent = Array.isArray(anomaly.relatedEvents)
    ? (anomaly.relatedEvents[0] as JsonRecord | undefined)
    : undefined;

  if (!repeatedEvent?.id) {
    return null;
  }

  const firstSeenAtMs = toBigIntOrNull(firstEvent?.ingestedAt);
  const repeatedSeenAtMs = toBigIntOrNull(repeatedEvent.ingestedAt);
  const firstEventTimeMs = toBigIntOrNull(firstEvent?.clock?.physicalTimeMs);
  const repeatedEventTimeMs = toBigIntOrNull(repeatedEvent.clock?.physicalTimeMs);
  const seenGapMs =
    firstSeenAtMs !== null && repeatedSeenAtMs !== null
      ? repeatedSeenAtMs - firstSeenAtMs
      : null;
  const firstSeenLatencyMs =
    firstSeenAtMs !== null && firstEventTimeMs !== null
      ? firstSeenAtMs - firstEventTimeMs
      : null;
  const repeatedSeenLatencyMs =
    repeatedSeenAtMs !== null && repeatedEventTimeMs !== null
      ? repeatedSeenAtMs - repeatedEventTimeMs
      : null;
  const activeWindowSeconds = dedupeGate.getStats().activeWindowSeconds;
  const activeWindowMs = BigInt(Math.round(activeWindowSeconds * 1000));

  return {
    timestampIso: new Date().toISOString(),
    eventId: repeatedEvent.id,
    nodeId: repeatedEvent.nodeId ?? firstEvent?.nodeId ?? null,
    activeWindowSecondsAtRepeat: activeWindowSeconds,
    firstSeen: {
      ingestedAtMs: bigintToStringOrNull(firstSeenAtMs),
      timestampIso: epochMsToIso(firstSeenAtMs),
      eventTimeMs: bigintToStringOrNull(firstEventTimeMs),
      eventTimeIso: epochMsToIso(firstEventTimeMs),
      arrivalLatencyMs: bigintToStringOrNull(firstSeenLatencyMs),
    },
    repeatedSeen: {
      ingestedAtMs: bigintToStringOrNull(repeatedSeenAtMs),
      timestampIso: epochMsToIso(repeatedSeenAtMs),
      eventTimeMs: bigintToStringOrNull(repeatedEventTimeMs),
      eventTimeIso: epochMsToIso(repeatedEventTimeMs),
      arrivalLatencyMs: bigintToStringOrNull(repeatedSeenLatencyMs),
    },
    seenGapMs: bigintToStringOrNull(seenGapMs),
    seenGapExceedsActiveWindowAtRepeat:
      seenGapMs !== null ? seenGapMs > activeWindowMs : null,
  };
}

function toBigIntOrNull(value: unknown): bigint | null {
  if (typeof value === "bigint") {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return BigInt(Math.trunc(value));
  }
  if (typeof value === "string" && value.trim().length > 0) {
    return BigInt(value);
  }
  return null;
}

function bigintToStringOrNull(value: bigint | null): string | null {
  return value === null ? null : value.toString();
}

function epochMsToIso(value: bigint | null): string | null {
  return value === null ? null : new Date(Number(value)).toISOString();
}

function serializeError(error: unknown): JsonRecord {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack ?? null,
    };
  }

  return {
    name: "NonError",
    message: String(error),
    stack: null,
  };
}

function createAsyncQueue<T>() {
  const values: T[] = [];
  const waiters: Array<(result: IteratorResult<T>) => void> = [];
  let closed = false;

  return {
    push(value: T) {
      if (closed) {
        return;
      }
      if (waiters.length > 0) {
        const waiter = waiters.shift();
        waiter?.({ value, done: false });
        return;
      }
      values.push(value);
    },
    close() {
      closed = true;
      while (waiters.length > 0) {
        const waiter = waiters.shift();
        waiter?.({ value: undefined as T, done: true });
      }
    },
    async *[Symbol.asyncIterator]() {
      while (true) {
        if (values.length > 0) {
          yield values.shift() as T;
          continue;
        }
        if (closed) {
          return;
        }
        const result = await new Promise<IteratorResult<T>>((resolveWaiter) => {
          waiters.push(resolveWaiter);
        });
        if (result.done) {
          return;
        }
        yield result.value;
      }
    },
  };
}
