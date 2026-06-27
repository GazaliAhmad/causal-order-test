import { createConnection } from "node:net";

import { createHlcClock } from "causal-order";

import {
  createSimulationClock,
  deserializeConfig,
  deserializeHintFromWire,
  formatDuration,
  randomBetween,
  randomInt,
  resolveConfiguredNodeRateShare,
  sampleIntervalMs,
  serializeEventForWire,
  sleepForSimulatedGap,
  type HintEvent,
  type RuntimeConfig,
  type SimulationEvent,
} from "./deployment-common.js";

type JsonRecord = Record<string, any>;

const rawConfig = process.env.RUNTIME_DEPLOYMENT_CONFIG;
const nodeId = process.env.RUNTIME_NODE_ID;
const collectorPort = Number(process.env.RUNTIME_COLLECTOR_PORT);

if (!rawConfig || !nodeId || !Number.isFinite(collectorPort)) {
  throw new Error("Missing node runtime environment");
}

const config = deserializeConfig(JSON.parse(rawConfig) as JsonRecord) as RuntimeConfig & {
  wallStartMs: number;
};
const clock = createSimulationClock(config);
const hlc = createHlcClock({ nodeId, now: clock.simulationNowMs });
const profile = config.workloadProfile;
const faultInjection = config.faultInjection;
const isDarkNode = faultInjection.darkNodeIds.includes(nodeId);
const darkNodeIndex = faultInjection.darkNodeIds.indexOf(nodeId);
const isJitterNode = faultInjection.jitterNodeIds.includes(nodeId);

const state = {
  sequence: 0n,
  nextEmitAtMs: clock.simulationNowMs() + sampleIntervalMs(8),
  lastScheduledSendAtMs: 0n,
  stopRequested: false,
  generated: 0,
  sent: 0,
  duplicatesInjected: 0,
  sameNodeDependencies: 0,
  crossNodeDependencies: 0,
  remoteHintsReceived: 0,
  maxPendingQueueDepth: 0,
  nextReportAtMs: clock.simulationNowMs() + config.reportEveryMs,
  recentLocal: [] as SimulationEvent[],
  recentRemote: [] as HintEvent[],
  flowCounter: 0,
  entityCounter: 0,
  darkActive: false,
  darkWindowsEntered: 0,
  reconnects: 0,
  connectionOpens: 0,
  jitterExtraDelaysApplied: 0,
  jitterSpikeDelaysApplied: 0,
};

process.once("SIGINT", () => {
  state.stopRequested = true;
});
process.once("SIGTERM", () => {
  state.stopRequested = true;
});
process.once("uncaughtException", (error) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
  process.exitCode = 1;
});
process.once("unhandledRejection", (reason) => {
  process.stderr.write(`${reason instanceof Error ? reason.stack ?? reason.message : String(reason)}\n`);
  process.exitCode = 1;
});

let socket: ReturnType<typeof createConnection> | null = null;
let pendingBuffer = "";
let connectionPromise: Promise<void> | null = null;

if (shouldBeDark(clock.simulationNowMs())) {
  state.darkActive = true;
  state.darkWindowsEntered = 1;
}

if (!state.darkActive) {
  await ensureConnected();
}

log(`node started on port ${collectorPort}`);

const pending: Array<{ event: SimulationEvent; sendAtMs: bigint }> = [];

while (true) {
  const now = clock.simulationNowMs();
  syncDarkState(now);

  if (!state.darkActive && !socket) {
    await ensureConnected();
  }

  const inMainWindow = now < clock.simulatedEndMs && !state.stopRequested;

  if (inMainWindow && !state.darkActive) {
    scheduleDueEvents(now, pending);
  }

  pending.sort((left, right) =>
    left.sendAtMs < right.sendAtMs ? -1 : left.sendAtMs > right.sendAtMs ? 1 : 0,
  );

  if (pending.length > state.maxPendingQueueDepth) {
    state.maxPendingQueueDepth = pending.length;
  }

  let sentAny = false;
  while (
    socket &&
    pending[0] &&
    pending[0].sendAtMs <= clock.simulationNowMs()
  ) {
    const delivery = pending.shift()!;
    sendMessage({
      type: "event",
      nodeId,
      event: serializeEventForWire(
        materializeDelivery(delivery.event, clock.simulationNowMs()),
      ),
    });
    state.sent += 1;
    sentAny = true;
  }

  if (clock.simulationNowMs() >= state.nextReportAtMs) {
    state.nextReportAtMs += config.reportEveryMs;
    log(
      [
        `[sim ${formatDuration(clock.simulationNowMs() - clock.simulatedStartMs)}]`,
        `generated=${state.generated}`,
        `sent=${state.sent}`,
        `queued=${pending.length}`,
        `remoteHints=${state.remoteHintsReceived}`,
        `phase=${getPhase(clock.simulationNowMs())}`,
      ].join(" "),
    );
  }

  const shouldStop =
    (!inMainWindow && pending.length === 0) ||
    (state.stopRequested && pending.length === 0);
  if (shouldStop) {
    break;
  }

  if (sentAny) {
    continue;
  }

  const nextActionAtMs = findNextActionAtMs(pending);
  if (nextActionAtMs === null) {
    break;
  }
  await sleepForSimulatedGap(
    nextActionAtMs - clock.simulationNowMs(),
    config.timeScale,
  );
}

if (!socket) {
  state.darkActive = false;
  await ensureConnected();
}

sendMessage({
  type: "complete",
  nodeId,
  stats: {
      generated: state.generated,
    sent: state.sent,
    duplicatesInjected: state.duplicatesInjected,
    sameNodeDependencies: state.sameNodeDependencies,
      crossNodeDependencies: state.crossNodeDependencies,
      remoteHintsReceived: state.remoteHintsReceived,
      maxPendingQueueDepth: state.maxPendingQueueDepth,
      darkWindowsEntered: state.darkWindowsEntered,
      reconnects: state.reconnects,
      connectionOpens: state.connectionOpens,
      jitterExtraDelaysApplied: state.jitterExtraDelaysApplied,
      jitterSpikeDelaysApplied: state.jitterSpikeDelaysApplied,
    },
  });
socket?.end();
log(
  [
    "node complete",
    `generated=${state.generated}`,
    `sent=${state.sent}`,
    `remoteHints=${state.remoteHintsReceived}`,
  ].join(" | "),
);

function scheduleDueEvents(
  now: bigint,
  pendingQueue: Array<{ event: SimulationEvent; sendAtMs: bigint }>,
): void {
  while (state.nextEmitAtMs <= now && now < clock.simulatedEndMs) {
    const phase = getPhase(now);
    const eventRecord = createEventRecord(now, phase);
    const remainingMainWindowMs = clock.simulatedEndMs - now;
    const baseDelayMs = sampleDeliveryDelayMs(phase);
    const maxAllowedDelayMs = remainingMainWindowMs + config.maxTailDrainMs;
    const appliedDelayMs =
      baseDelayMs > maxAllowedDelayMs ? maxAllowedDelayMs : baseDelayMs;
    const sendAtMs = chooseSendTime(phase, now + appliedDelayMs);

    pendingQueue.push({
      event: eventRecord.event,
      sendAtMs,
    });

    maybeInjectDuplicate(eventRecord, now, pendingQueue);
    rememberLocalEvent(eventRecord.event);
    state.nextEmitAtMs += sampleIntervalMs(resolveNodeRate(phase));
  }
}

function createEventRecord(now: bigint, phase: string): { event: SimulationEvent; createdAtMs: bigint } {
  const dependencyChoice = chooseDependency(phase);
  const traceId = dependencyChoice?.traceId ?? nextTraceId();
  const entityId = dependencyChoice?.entityId ?? nextEntityId();
  const eventId = `${nodeId}-${(state.sequence + 1n).toString().padStart(12, "0")}`;

  let eventClock;
  if (dependencyChoice?.isCrossNode) {
    eventClock = hlc.receive(dependencyChoice.event.clock as any);
    state.crossNodeDependencies += 1;
  } else {
    eventClock = hlc.now();
    if (dependencyChoice?.event) {
      state.sameNodeDependencies += 1;
    }
  }

  state.sequence += 1n;
  state.generated += 1;

  return {
    event: {
      id: eventId,
      nodeId,
      clock: eventClock as any,
      sequence: state.sequence,
      traceId,
      parentEventId:
        dependencyChoice?.relation === "parent"
          ? dependencyChoice.event.id
          : undefined,
      dependencyEventIds:
        dependencyChoice?.relation === "dependency"
          ? [dependencyChoice.event.id]
          : undefined,
      payload: {
        phase,
        service: nodeId,
        entityId,
        traceId,
        operation: chooseOperation(phase, dependencyChoice),
      },
    },
    createdAtMs: now,
  };
}

function chooseDependency(phase: string): JsonRecord | null {
  const sameNodeChance =
    phase === "steady"
      ? profile.dependencies.steadySameNodeChance
      : profile.dependencies.chaoticSameNodeChance;
  const crossNodeChance =
    phase === "steady"
      ? profile.dependencies.steadyCrossNodeChance
      : profile.dependencies.chaoticCrossNodeChance;
  const roll = Math.random();

  if (roll < crossNodeChance && state.recentRemote.length > 0) {
    const event = pickRecentEvent(state.recentRemote);
    return {
      event,
      isCrossNode: true,
      relation:
        Math.random() < profile.dependencies.crossNodeParentChance
          ? "parent"
          : "dependency",
      traceId: event.traceId ?? nextTraceId(),
      entityId: event.entityId ?? nextEntityId(),
    };
  }

  if (roll < crossNodeChance + sameNodeChance && state.recentLocal.length > 0) {
    const event = pickRecentEvent(state.recentLocal);
    return {
      event,
      isCrossNode: false,
      relation:
        Math.random() < profile.dependencies.sameNodeParentChance
          ? "parent"
          : "dependency",
      traceId: event.traceId,
      entityId: event.payload.entityId,
    };
  }

  return null;
}

function chooseOperation(phase: string, dependencyChoice: JsonRecord | null): string {
  if (dependencyChoice?.isCrossNode) {
    const operations = [
      "replica.applied",
      "payment.confirmed",
      "inventory.reserved",
      "projection.updated",
      "compensation.scheduled",
    ];
    return operations[Math.floor(Math.random() * operations.length)];
  }

  const steadyOperations = [
    "ingress.accepted",
    "order.created",
    "state.persisted",
    "workflow.advanced",
  ];
  const chaoticOperations = [
    "replay.applied",
    "reconciliation.requested",
    "projection.rebuilt",
    "device.resynced",
    "backfill.merged",
  ];

  const source = phase === "steady" ? steadyOperations : chaoticOperations;
  return source[Math.floor(Math.random() * source.length)];
}

function maybeInjectDuplicate(
  eventRecord: { event: SimulationEvent },
  now: bigint,
  pendingQueue: Array<{ event: SimulationEvent; sendAtMs: bigint }>,
): void {
  const phase = eventRecord.event.payload.phase ?? "steady";
  const duplicateChance =
    phase === "steady"
      ? profile.duplicates.steadyChance
      : profile.duplicates.chaoticChance;
  if (Math.random() >= duplicateChance) {
    return;
  }

  state.duplicatesInjected += 1;
  const duplicateDelayMs = sampleDeliveryDelayMs("chaotic") + 250n;
  const remainingMainWindowMs = clock.simulatedEndMs - now;
  const maxAllowedDelayMs = remainingMainWindowMs + config.maxTailDrainMs;
  const appliedDelayMs =
    duplicateDelayMs > maxAllowedDelayMs ? maxAllowedDelayMs : duplicateDelayMs;

  pendingQueue.push({
    event: eventRecord.event,
    sendAtMs: now + appliedDelayMs,
  });
}

function rememberLocalEvent(event: SimulationEvent): void {
  state.recentLocal.push(event);
  if (state.recentLocal.length > 128) {
    state.recentLocal.shift();
  }
}

function chooseSendTime(phase: string, candidateSendAtMs: bigint): bigint {
  const shouldPreserveNodeOrder =
    Math.random() <
    (phase === "steady"
      ? profile.ordering.steadyPreserveOrderChance
      : profile.ordering.chaoticPreserveOrderChance);

  if (!shouldPreserveNodeOrder) {
    if (candidateSendAtMs > state.lastScheduledSendAtMs) {
      state.lastScheduledSendAtMs = candidateSendAtMs;
    }
    return candidateSendAtMs;
  }

  const earliestSendAtMs =
    state.lastScheduledSendAtMs > 0n
      ? state.lastScheduledSendAtMs + BigInt(randomInt(1, 12))
      : candidateSendAtMs;
  const sendAtMs =
    candidateSendAtMs > earliestSendAtMs
      ? candidateSendAtMs
      : earliestSendAtMs;

  state.lastScheduledSendAtMs = sendAtMs;
  return sendAtMs;
}

function resolveNodeRate(phase: string): number {
  const steadyTotalRate = config.eventsPerSecond;
  const chaosTotalRate =
    config.eventsPerSecond *
    config.chaosMultiplier *
    randomBetween(
      profile.phaseRates.chaosJitterMin,
      profile.phaseRates.chaosJitterMax,
    );
  const nodeShare = resolveConfiguredNodeRateShare(
    config.nodeIds,
    profile.nodeWeights,
    nodeId,
  );
  const totalRate = phase === "steady" ? steadyTotalRate : chaosTotalRate;
  return Math.max(0.25, totalRate * nodeShare);
}

function sampleDeliveryDelayMs(phase: string): bigint {
  if (phase === "steady") {
    let delayMs = BigInt(
      randomInt(
        profile.delays.steady.baseMinMs,
        profile.delays.steady.baseMaxMs,
      ),
    );
    if (Math.random() < profile.delays.steady.spikeChance) {
      delayMs += BigInt(
        randomInt(
          profile.delays.steady.spikeMinMs,
          profile.delays.steady.spikeMaxMs,
        ),
      );
    }
    return applyJitterDelay(delayMs);
  }

  let delayMs = BigInt(
    randomInt(
      profile.delays.chaotic.baseMinMs,
      profile.delays.chaotic.baseMaxMs,
    ),
  );
  if (Math.random() < profile.delays.chaotic.slowSpikeChance) {
    delayMs += BigInt(
      randomInt(
        profile.delays.chaotic.slowSpikeMinMs,
        profile.delays.chaotic.slowSpikeMaxMs,
      ),
    );
  }
  if (Math.random() < profile.delays.chaotic.lateSpikeChance) {
    delayMs += BigInt(
      randomInt(
        profile.delays.chaotic.lateSpikeMinMs,
        profile.delays.chaotic.lateSpikeMaxMs,
      ),
    );
  }
  if (Math.random() < profile.delays.chaotic.extremeSpikeChance) {
    delayMs += BigInt(
      randomInt(
        profile.delays.chaotic.extremeSpikeMinMs,
        profile.delays.chaotic.extremeSpikeMaxMs,
      ),
    );
  }
  return applyJitterDelay(delayMs);
}

function pickRecentEvent<T>(history: T[]): T {
  const window = history.slice(-Math.min(history.length, 32));
  return window[Math.floor(Math.random() * window.length)];
}

function nextTraceId(): string {
  state.flowCounter += 1;
  return `${nodeId}-trace-${state.flowCounter.toString().padStart(8, "0")}`;
}

function nextEntityId(): string {
  state.entityCounter += 1;
  return `${nodeId}-entity-${state.entityCounter.toString().padStart(8, "0")}`;
}

function materializeDelivery(event: SimulationEvent, ingestedAt: bigint): SimulationEvent {
  return {
    ...event,
    clock: { ...event.clock },
    dependencyEventIds: event.dependencyEventIds
      ? [...event.dependencyEventIds]
      : undefined,
    payload: { ...event.payload },
    ingestedAt,
  };
}

function getPhase(now: bigint): string {
  return now - clock.simulatedStartMs < config.steadyForMs ? "steady" : "chaotic";
}

function findNextActionAtMs(
  pendingQueue: Array<{ event: SimulationEvent; sendAtMs: bigint }>,
): bigint | null {
  const now = clock.simulationNowMs();
  let nextAt: bigint | null = null;

  if (!state.stopRequested && now < clock.simulatedEndMs) {
    nextAt = state.nextEmitAtMs;
  }

  if (pendingQueue[0] && (nextAt === null || pendingQueue[0].sendAtMs < nextAt)) {
    nextAt = pendingQueue[0].sendAtMs;
  }

  return nextAt;
}

function sendMessage(message: JsonRecord): void {
  const activeSocket = socket;
  if (
    !activeSocket ||
    activeSocket.destroyed ||
    !activeSocket.writable ||
    activeSocket.writableEnded
  ) {
    return;
  }

  try {
    activeSocket.write(`${JSON.stringify(message)}\n`);
  } catch (error) {
    process.stderr.write(
      `${error instanceof Error ? error.message : String(error)}\n`,
    );
  }
}

function log(message: string): void {
  process.stdout.write(`${message}\n`);
}

async function ensureConnected(): Promise<void> {
  if (socket || state.stopRequested || state.darkActive) {
    return;
  }

  if (connectionPromise) {
    await connectionPromise;
    return;
  }

  const pendingConnection = (async () => {
    const nextSocket = createConnection({
      host: "127.0.0.1",
      port: collectorPort,
    });
    nextSocket.setEncoding("utf8");

    nextSocket.on("data", (chunk) => {
      pendingBuffer += chunk;
      const lines = pendingBuffer.split("\n");
      pendingBuffer = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) {
          continue;
        }
        const message = JSON.parse(line) as JsonRecord;
        if (message.type === "peer_event") {
          state.remoteHintsReceived += 1;
          state.recentRemote.push(deserializeHintFromWire(message.event));
          if (state.recentRemote.length > 128) {
            state.recentRemote.shift();
          }
        }
      }
    });

    nextSocket.on("error", (error) => {
      process.stderr.write(`${error.message}\n`);
    });

    nextSocket.on("close", () => {
      if (socket === nextSocket) {
        socket = null;
        pendingBuffer = "";
      }
    });

    await new Promise<void>((resolveOpen, rejectOpen) => {
      nextSocket.once("connect", () => resolveOpen());
      nextSocket.once("error", rejectOpen);
    });

    if (state.darkActive || state.stopRequested) {
      nextSocket.end();
      return;
    }

    socket = nextSocket;
    state.connectionOpens += 1;
    sendMessage({
      type: "hello",
      nodeId,
    });
  })();

  connectionPromise = pendingConnection;
  try {
    await pendingConnection;
  } finally {
    if (connectionPromise === pendingConnection) {
      connectionPromise = null;
    }
  }
}

function syncDarkState(now: bigint): void {
  const nextDarkState = shouldBeDark(now);

  if (nextDarkState === state.darkActive) {
    return;
  }

  if (nextDarkState) {
    state.darkActive = true;
    state.darkWindowsEntered += 1;
    const activeSocket = socket;
    socket = null;
    pendingBuffer = "";

    if (
      activeSocket &&
      !activeSocket.destroyed &&
      activeSocket.writable &&
      !activeSocket.writableEnded
    ) {
      try {
        activeSocket.write(
          `${JSON.stringify({
            type: "fault_state",
            nodeId,
            state: "dark_start",
          })}\n`,
        );
      } catch (error) {
        process.stderr.write(
          `${error instanceof Error ? error.message : String(error)}\n`,
        );
      }
      activeSocket.end();
    }

    log(`[sim ${formatDuration(now - clock.simulatedStartMs)}] dark_start`);
    return;
  }

  state.darkActive = false;
  state.reconnects += 1;
  pendingBuffer = "";
  void ensureConnected()
    .then(() => {
      sendMessage({
        type: "fault_state",
        nodeId,
        state: "dark_end",
      });
    })
    .catch((error) => {
      process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
      process.exitCode = 1;
    });
  log(`[sim ${formatDuration(now - clock.simulatedStartMs)}] dark_end`);
}

function shouldBeDark(now: bigint): boolean {
  if (!isDarkNode || faultInjection.darkDurationMs <= 0n) {
    return false;
  }

  const firstDarkStartMs =
    clock.simulatedStartMs +
    faultInjection.darkStartAfterMs +
    faultInjection.darkStaggerMs * BigInt(Math.max(0, darkNodeIndex));

  if (now < firstDarkStartMs) {
    return false;
  }

  const elapsedSinceFirstDarkMs = now - firstDarkStartMs;
  return elapsedSinceFirstDarkMs % faultInjection.darkIntervalMs <
    faultInjection.darkDurationMs;
}

function applyJitterDelay(delayMs: bigint): bigint {
  if (!isJitterNode) {
    return delayMs;
  }

  let adjustedDelayMs = delayMs;
  adjustedDelayMs += sampleConfiguredDelay(
    faultInjection.jitterExtraDelayMinMs,
    faultInjection.jitterExtraDelayMaxMs,
  );
  state.jitterExtraDelaysApplied += 1;

  if (Math.random() < faultInjection.jitterSpikeChance) {
    adjustedDelayMs += sampleConfiguredDelay(
      faultInjection.jitterSpikeMinMs,
      faultInjection.jitterSpikeMaxMs,
    );
    state.jitterSpikeDelaysApplied += 1;
  }

  return adjustedDelayMs;
}

function sampleConfiguredDelay(minMs: bigint, maxMs: bigint): bigint {
  return BigInt(randomInt(Number(minMs), Number(maxMs)));
}
