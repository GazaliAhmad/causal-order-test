import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { EventEnvelope, HlcTimestamp } from "causal-order/types";
import {
  loadDedupeGatewayConfigFile,
  type DedupeGatewayFileConfig,
  type DedupePreset,
} from "@causal-order/dedupe";

const PROFILE_DIR = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  "..",
  "profiles",
);

export type LateArrivalPolicy = "flag" | "drop" | "emit_correction" | "fail";

export interface WorkloadProfile {
  name: string;
  description: string;
  nodeWeights: Record<string, number>;
  phaseRates: {
    steadyEventsPerSecond: number;
    chaosMultiplier: number;
    chaosJitterMin: number;
    chaosJitterMax: number;
  };
  dependencies: {
    steadySameNodeChance: number;
    steadyCrossNodeChance: number;
    chaoticSameNodeChance: number;
    chaoticCrossNodeChance: number;
    sameNodeParentChance: number;
    crossNodeParentChance: number;
  };
  duplicates: {
    steadyChance: number;
    chaoticChance: number;
  };
  ordering: {
    steadyPreserveOrderChance: number;
    chaoticPreserveOrderChance: number;
  };
  delays: {
    steady: {
      baseMinMs: number;
      baseMaxMs: number;
      spikeChance: number;
      spikeMinMs: number;
      spikeMaxMs: number;
    };
    chaotic: {
      baseMinMs: number;
      baseMaxMs: number;
      slowSpikeChance: number;
      slowSpikeMinMs: number;
      slowSpikeMaxMs: number;
      lateSpikeChance: number;
      lateSpikeMinMs: number;
      lateSpikeMaxMs: number;
      extremeSpikeChance: number;
      extremeSpikeMinMs: number;
      extremeSpikeMaxMs: number;
    };
  };
}

export interface RuntimeArtifacts {
  runDir: string;
  summaryPath: string;
  heartbeatPath: string;
  anomalyPath: string;
  duplicateLeakPath: string;
  lifecyclePath: string;
  configPath: string;
  orchestratorLogPath: string;
  collectorStdoutPath: string;
  collectorStderrPath: string;
  nodesDir: string;
}

export interface FaultInjectionConfig {
  darkNodeIds: string[];
  darkIntervalMs: bigint;
  darkDurationMs: bigint;
  darkStartAfterMs: bigint;
  darkStaggerMs: bigint;
  jitterNodeIds: string[];
  jitterExtraDelayMinMs: bigint;
  jitterExtraDelayMaxMs: bigint;
  jitterSpikeChance: number;
  jitterSpikeMinMs: bigint;
  jitterSpikeMaxMs: bigint;
}

export type HybridClock = HlcTimestamp & Record<string, unknown>;

export interface EventPayload {
  phase?: string;
  service?: string;
  entityId?: string | null;
  traceId?: string | null;
  operation?: string;
  [key: string]: unknown;
}

export interface SimulationEvent extends EventEnvelope<EventPayload> {
  payload: EventPayload;
}

export interface HintEvent {
  id: string;
  nodeId: string;
  clock: HybridClock;
  traceId?: string | null;
  entityId?: string | null;
  [key: string]: unknown;
}

export interface RuntimeConfig {
  durationMs: bigint;
  steadyForMs: bigint;
  eventsPerSecond: number;
  chaosMultiplier: number;
  batchSize: number;
  maxLateArrivalMs: bigint;
  maxTailDrainMs: bigint;
  lateArrivalPolicy: LateArrivalPolicy;
  reportEveryMs: bigint;
  timeScale: number;
  outputPath: string | null;
  outputDir: string;
  runName: string | null;
  sampleLimit: number;
  maxLateArrivalSamples: number;
  strict: boolean;
  allowUnknownOrder: boolean;
  detectAnomalies: boolean;
  tieBreaker: string;
  dedupeConfig: DedupeGatewayFileConfig;
  nodeIds: string[];
  faultInjection: FaultInjectionConfig;
  workloadProfile: WorkloadProfile;
  profileSource: string | null;
  wallStartMs?: number;
  artifacts?: RuntimeArtifacts;
}

export const DEFAULT_WORKLOAD_PROFILE: WorkloadProfile = {
  name: "balanced-default",
  description: "Balanced synthetic workload with moderate cross-node pressure.",
  nodeWeights: {
    "edge-a": 1.0,
    "edge-b": 1.0,
    "edge-c": 1.0,
  },
  phaseRates: {
    steadyEventsPerSecond: 16,
    chaosMultiplier: 1.8,
    chaosJitterMin: 0.85,
    chaosJitterMax: 1.2,
  },
  dependencies: {
    steadySameNodeChance: 0.38,
    steadyCrossNodeChance: 0.24,
    chaoticSameNodeChance: 0.22,
    chaoticCrossNodeChance: 0.46,
    sameNodeParentChance: 0.8,
    crossNodeParentChance: 0.7,
  },
  duplicates: {
    steadyChance: 0.002,
    chaoticChance: 0.018,
  },
  ordering: {
    steadyPreserveOrderChance: 1,
    chaoticPreserveOrderChance: 0.88,
  },
  delays: {
    steady: {
      baseMinMs: 20,
      baseMaxMs: 120,
      spikeChance: 0.03,
      spikeMinMs: 250,
      spikeMaxMs: 800,
    },
    chaotic: {
      baseMinMs: 40,
      baseMaxMs: 250,
      slowSpikeChance: 0.08,
      slowSpikeMinMs: 1_500,
      slowSpikeMaxMs: 8_000,
      lateSpikeChance: 0.02,
      lateSpikeMinMs: 10_000,
      lateSpikeMaxMs: 45_000,
      extremeSpikeChance: 0.005,
      extremeSpikeMinMs: 60_000,
      extremeSpikeMaxMs: 180_000,
    },
  },
};

export const DEFAULT_SINGLE_CLUSTER_NODE_IDS = Object.freeze([
  "edge-a",
  "edge-b",
  "edge-c",
] as const);

export const DEFAULTS = {
  durationMs: parseDurationToMs("4h"),
  steadyRatio: 0.3,
  eventsPerSecond: DEFAULT_WORKLOAD_PROFILE.phaseRates.steadyEventsPerSecond,
  chaosMultiplier: DEFAULT_WORKLOAD_PROFILE.phaseRates.chaosMultiplier,
  batchSize: 200,
  maxLateArrivalMs: 60_000n,
  maxTailDrainMs: parseDurationToMs("5m"),
  lateArrivalPolicy: "flag" as LateArrivalPolicy,
  reportEveryMs: parseDurationToMs("30s"),
  timeScale: 1,
  outputDir: "artifacts/runs",
  sampleLimit: 20,
  maxLateArrivalSamples: 200,
  strict: false,
  allowUnknownOrder: true,
  detectAnomalies: true,
  tieBreaker: "ingestion_order",
  dedupePreset: "standard" as DedupePreset,
  nodeIds: [...DEFAULT_SINGLE_CLUSTER_NODE_IDS],
  profile: DEFAULT_WORKLOAD_PROFILE.name,
  profileFile: null as string | null,
  darkNodeIds: [] as string[],
  darkIntervalMs: parseDurationToMs("25m"),
  darkDurationMs: parseDurationToMs("4m"),
  darkStartAfterMs: parseDurationToMs("10m"),
  darkStaggerMs: parseDurationToMs("3m"),
  jitterNodeIds: [] as string[],
  jitterExtraDelayMinMs: parseDurationToMs("75ms"),
  jitterExtraDelayMaxMs: parseDurationToMs("1200ms"),
  jitterSpikeChance: 0.08,
  jitterSpikeMinMs: parseDurationToMs("3s"),
  jitterSpikeMaxMs: parseDurationToMs("12s"),
};

export const HELP_TEXT = `Usage:
  causal-order-testing-runtime [options]

Options:
  --duration <value>           Total simulated runtime. Supports ms, s, m, h. Default: 4h
  --steady-for <value>         Simulated steady phase duration before chaos begins
  --steady-ratio <0..1>        Portion of total duration spent steady when --steady-for is omitted
  --events-per-second <n>      Average total steady-state throughput across the whole cluster. Default: 16
  --chaos-multiplier <n>       Multiplier applied during chaotic phase. Default: 1.8
  --batch-size <n>             orderEventStream batch size. Default: 200
  --max-late-arrival-ms <n>    Late-arrival window in milliseconds. Default: 60000
  --max-tail-drain <value>     Max extra simulated drain time for delayed events. Default: 5m
  --late-policy <value>        flag | drop | emit_correction | fail. Default: flag
  --report-every <value>       Progress log interval in simulated time. Default: 30s
  --time-scale <n>             1 = realtime, 60 = one simulated minute per wall second
  --dedupe-preset <value>      standard | heavy-duplicates | high-latency | cross-node-busy. Default: standard
  --dedupe-config <path>       JSON file with either a preset or explicit sliding/max windows
  --node-ids <csv>             Comma-separated node IDs for one single-cluster run. Default: edge-a,edge-b,edge-c
  --dark-nodes <csv>           Nodes that periodically disconnect and reconnect
  --dark-interval <value>      Time between repeated dark windows. Default: 25m
  --dark-duration <value>      Length of each dark window. Default: 4m
  --dark-start-after <value>   Delay before the first dark window. Default: 10m
  --dark-stagger <value>       Extra offset applied between dark nodes. Default: 3m
  --jitter-nodes <csv>         Nodes that stay up but receive extra transport jitter
  --jitter-extra-delay-min <value>  Minimum extra delay added to jitter nodes. Default: 75ms
  --jitter-extra-delay-max <value>  Maximum extra delay added to jitter nodes. Default: 1200ms
  --jitter-spike-chance <0..1> Chance of an additional jitter spike on jitter nodes. Default: 0.08
  --jitter-spike-min <value>   Minimum extra jitter spike delay. Default: 3s
  --jitter-spike-max <value>   Maximum extra jitter spike delay. Default: 12s
  --output <path>              Explicit summary JSON path
  --output-dir <path>          Base directory for run artifacts. Default: artifacts/runs
  --run-name <value>           Optional label appended to the run folder name
  --profile <value>            Built-in or local profile name from profiles/. Default: balanced-default
  --profile-file <path>        Explicit workload profile JSON file
  --sample-limit <n>           Number of anomaly/correction samples to retain. Default: 20
  --strict                     Enable strict stream validation
  --disallow-unknown-order     Force allowUnknownOrder=false
  --help                       Show this message

Examples:
  causal-order-testing-runtime --duration 6h --steady-for 90m --time-scale 1
  causal-order-testing-runtime --duration 20m --time-scale 60 --report-every 2m
  causal-order-testing-runtime --duration 10h --steady-for 2h --run-name overnight-laptop
`;

export function buildConfig(argv: string[]): RuntimeConfig | { help: true } {
  const parsed = parseArgs(argv);
  if (parsed.help) {
    return { help: true };
  }

  const resolvedProfile = resolveWorkloadProfile({
    profileName: parsed.profile ?? DEFAULTS.profile,
    profileFile: parsed.profileFile ?? DEFAULTS.profileFile,
  });

  const durationMs = parsed.durationMs ?? DEFAULTS.durationMs;
  const steadyRatio = parsed.steadyRatio ?? DEFAULTS.steadyRatio;
  const steadyForMs =
    parsed.steadyForMs ?? BigInt(Math.floor(Number(durationMs) * steadyRatio));

  if (steadyForMs < 0n || steadyForMs > durationMs) {
    throw new Error("Steady phase must be between 0 and total duration");
  }

  const resolvedNodeIds = parsed.nodeIds ?? [...DEFAULTS.nodeIds];
  const alignedWorkloadProfile = alignWorkloadProfileToNodeIds(
    resolvedProfile,
    resolvedNodeIds,
  );
  const faultInjection = resolveFaultInjectionConfig(parsed, resolvedNodeIds);

  return {
    durationMs,
    steadyForMs,
    eventsPerSecond:
      parsed.eventsPerSecond ??
      alignedWorkloadProfile.phaseRates.steadyEventsPerSecond,
    chaosMultiplier:
      parsed.chaosMultiplier ?? alignedWorkloadProfile.phaseRates.chaosMultiplier,
    batchSize: parsed.batchSize ?? DEFAULTS.batchSize,
    maxLateArrivalMs: parsed.maxLateArrivalMs ?? DEFAULTS.maxLateArrivalMs,
    maxTailDrainMs: parsed.maxTailDrainMs ?? DEFAULTS.maxTailDrainMs,
    lateArrivalPolicy:
      parsed.lateArrivalPolicy ?? DEFAULTS.lateArrivalPolicy,
    reportEveryMs: parsed.reportEveryMs ?? DEFAULTS.reportEveryMs,
    timeScale: parsed.timeScale ?? DEFAULTS.timeScale,
    dedupeConfig: resolveRuntimeDedupeConfig(parsed),
    outputPath: parsed.outputPath ?? null,
    outputDir: parsed.outputDir ?? DEFAULTS.outputDir,
    runName: parsed.runName ?? null,
    sampleLimit: parsed.sampleLimit ?? DEFAULTS.sampleLimit,
    maxLateArrivalSamples: DEFAULTS.maxLateArrivalSamples,
    strict: parsed.strict ?? DEFAULTS.strict,
    allowUnknownOrder: parsed.allowUnknownOrder ?? DEFAULTS.allowUnknownOrder,
    detectAnomalies: parsed.detectAnomalies ?? DEFAULTS.detectAnomalies,
    tieBreaker: DEFAULTS.tieBreaker,
    nodeIds: resolvedNodeIds,
    faultInjection,
    workloadProfile: alignedWorkloadProfile,
    profileSource: parsed.profileFile
      ? resolve(parsed.profileFile)
      : parsed.profile && parsed.profile !== DEFAULT_WORKLOAD_PROFILE.name
        ? resolve(PROFILE_DIR, `${alignedWorkloadProfile.name}.json`)
        : "built-in",
  };
}

function parseArgs(argv: string[]) {
  const result: Record<string, any> = {};

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help") {
      return { help: true };
    }

    const [rawKey, inlineValue] = token.split("=", 2);
    const value = inlineValue !== undefined ? inlineValue : argv[index + 1];

    switch (rawKey) {
      case "--duration":
        result.durationMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--steady-for":
        result.steadyForMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--steady-ratio":
        result.steadyRatio = parseUnitInterval(
          requireValue(rawKey, value),
          rawKey,
        );
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--events-per-second":
        result.eventsPerSecond = parsePositiveNumber(
          requireValue(rawKey, value),
          rawKey,
        );
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--chaos-multiplier":
        result.chaosMultiplier = parsePositiveNumber(
          requireValue(rawKey, value),
          rawKey,
        );
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--batch-size":
        result.batchSize = parsePositiveInteger(
          requireValue(rawKey, value),
          rawKey,
        );
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--max-late-arrival-ms":
        result.maxLateArrivalMs = parseNonNegativeBigInt(
          requireValue(rawKey, value),
          rawKey,
        );
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--max-tail-drain":
        result.maxTailDrainMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--late-policy":
        result.lateArrivalPolicy = parseLatePolicy(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--report-every":
        result.reportEveryMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--time-scale":
        result.timeScale = parsePositiveNumber(requireValue(rawKey, value), rawKey);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--dedupe-preset":
        result.dedupePreset = parseDedupePreset(
          requireValue(rawKey, value),
          rawKey,
        );
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--dedupe-config":
        result.dedupeConfigPath = requireValue(rawKey, value);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--node-ids":
        result.nodeIds = parseNodeIds(requireValue(rawKey, value), rawKey);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--dark-nodes":
        result.darkNodeIds = parseNodeIds(requireValue(rawKey, value), rawKey);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--dark-interval":
        result.darkIntervalMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--dark-duration":
        result.darkDurationMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--dark-start-after":
        result.darkStartAfterMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--dark-stagger":
        result.darkStaggerMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--jitter-nodes":
        result.jitterNodeIds = parseNodeIds(requireValue(rawKey, value), rawKey);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--jitter-extra-delay-min":
        result.jitterExtraDelayMinMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--jitter-extra-delay-max":
        result.jitterExtraDelayMaxMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--jitter-spike-chance":
        result.jitterSpikeChance = parseUnitInterval(
          requireValue(rawKey, value),
          rawKey,
        );
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--jitter-spike-min":
        result.jitterSpikeMinMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--jitter-spike-max":
        result.jitterSpikeMaxMs = parseDurationToMs(requireValue(rawKey, value));
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--output":
        result.outputPath = requireValue(rawKey, value);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--output-dir":
        result.outputDir = requireValue(rawKey, value);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--run-name":
        result.runName = requireValue(rawKey, value);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--profile":
        result.profile = requireValue(rawKey, value);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--profile-file":
        result.profileFile = requireValue(rawKey, value);
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--sample-limit":
        result.sampleLimit = parsePositiveInteger(
          requireValue(rawKey, value),
          rawKey,
        );
        index += inlineValue === undefined ? 1 : 0;
        break;
      case "--strict":
        result.strict = true;
        break;
      case "--disallow-unknown-order":
        result.allowUnknownOrder = false;
        break;
      default:
        throw new Error(`Unknown option: ${token}`);
    }
  }

  return result;
}

function requireValue(flag: string, value?: string): string {
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`Missing value for ${flag}`);
  }
  return value;
}

function resolveRuntimeDedupeConfig(
  parsed: Record<string, any>,
): DedupeGatewayFileConfig {
  if (parsed.dedupeConfigPath && parsed.dedupePreset !== undefined) {
    throw new Error("Choose either --dedupe-config or --dedupe-preset, not both");
  }

  if (parsed.dedupeConfigPath) {
    return loadDedupeGatewayConfigFile(parsed.dedupeConfigPath);
  }

  return {
    preset: parsed.dedupePreset ?? DEFAULTS.dedupePreset,
  };
}

function resolveFaultInjectionConfig(
  parsed: Record<string, any>,
  nodeIds: string[],
): FaultInjectionConfig {
  const darkNodeIds = parsed.darkNodeIds ?? [...DEFAULTS.darkNodeIds];
  const jitterNodeIds = parsed.jitterNodeIds ?? [...DEFAULTS.jitterNodeIds];
  const activeNodeIdSet = new Set(nodeIds);

  for (const nodeId of darkNodeIds) {
    if (!activeNodeIdSet.has(nodeId)) {
      throw new Error(`--dark-nodes cannot include unknown node ID "${nodeId}"`);
    }
  }

  for (const nodeId of jitterNodeIds) {
    if (!activeNodeIdSet.has(nodeId)) {
      throw new Error(`--jitter-nodes cannot include unknown node ID "${nodeId}"`);
    }
  }

  for (const nodeId of darkNodeIds) {
    if (jitterNodeIds.includes(nodeId)) {
      throw new Error(
        `Fault injection node "${nodeId}" cannot be both dark and jitter-prone in the same run`,
      );
    }
  }

  const darkIntervalMs = parsed.darkIntervalMs ?? DEFAULTS.darkIntervalMs;
  const darkDurationMs = parsed.darkDurationMs ?? DEFAULTS.darkDurationMs;
  const darkStartAfterMs = parsed.darkStartAfterMs ?? DEFAULTS.darkStartAfterMs;
  const darkStaggerMs = parsed.darkStaggerMs ?? DEFAULTS.darkStaggerMs;
  const jitterExtraDelayMinMs =
    parsed.jitterExtraDelayMinMs ?? DEFAULTS.jitterExtraDelayMinMs;
  const jitterExtraDelayMaxMs =
    parsed.jitterExtraDelayMaxMs ?? DEFAULTS.jitterExtraDelayMaxMs;
  const jitterSpikeChance =
    parsed.jitterSpikeChance ?? DEFAULTS.jitterSpikeChance;
  const jitterSpikeMinMs = parsed.jitterSpikeMinMs ?? DEFAULTS.jitterSpikeMinMs;
  const jitterSpikeMaxMs = parsed.jitterSpikeMaxMs ?? DEFAULTS.jitterSpikeMaxMs;

  if (darkDurationMs > darkIntervalMs) {
    throw new Error("--dark-duration cannot be greater than --dark-interval");
  }

  if (jitterExtraDelayMinMs > jitterExtraDelayMaxMs) {
    throw new Error(
      "--jitter-extra-delay-min cannot be greater than --jitter-extra-delay-max",
    );
  }

  if (jitterSpikeMinMs > jitterSpikeMaxMs) {
    throw new Error("--jitter-spike-min cannot be greater than --jitter-spike-max");
  }

  return {
    darkNodeIds,
    darkIntervalMs,
    darkDurationMs,
    darkStartAfterMs,
    darkStaggerMs,
    jitterNodeIds,
    jitterExtraDelayMinMs,
    jitterExtraDelayMaxMs,
    jitterSpikeChance,
    jitterSpikeMinMs,
    jitterSpikeMaxMs,
  };
}

export function parseDurationToMs(input: string): bigint {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/i.exec(input.trim());
  if (!match) {
    throw new Error(
      `Invalid duration "${input}". Use values like 500ms, 30s, 15m, 4h`,
    );
  }

  const [, amountText, unit] = match;
  const amount = Number(amountText);
  const multiplierByUnit = {
    ms: 1,
    s: 1_000,
    m: 60_000,
    h: 3_600_000,
  };
  return BigInt(Math.floor(amount * multiplierByUnit[unit.toLowerCase()]));
}

function parsePositiveInteger(input: string, label: string): number {
  const value = Number.parseInt(input, 10);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive safe integer`);
  }
  return value;
}

function parsePositiveNumber(input: string, label: string): number {
  const value = Number(input);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${label} must be a positive number`);
  }
  return value;
}

function parseNonNegativeBigInt(input: string, label: string): bigint {
  if (!/^\d+$/.test(input)) {
    throw new Error(`${label} must be a non-negative integer`);
  }
  return BigInt(input);
}

function parseUnitInterval(input: string, label: string): number {
  const value = Number(input);
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new Error(`${label} must be between 0 and 1`);
  }
  return value;
}

function parseLatePolicy(input: string): LateArrivalPolicy {
  const value = input.trim() as LateArrivalPolicy;
  const supported = new Set<LateArrivalPolicy>([
    "flag",
    "drop",
    "emit_correction",
    "fail",
  ]);
  if (!supported.has(value)) {
    throw new Error(`Unsupported late policy: ${value}`);
  }
  return value;
}

function parseDedupePreset(input: string, label: string): DedupePreset {
  const value = input.trim() as DedupePreset;
  const supported = new Set<DedupePreset>([
    "standard",
    "heavy-duplicates",
    "high-latency",
    "cross-node-busy",
  ]);
  if (!supported.has(value)) {
    throw new Error(
      `${label} must be one of: standard, heavy-duplicates, high-latency, cross-node-busy`,
    );
  }
  return value;
}

function parseNodeIds(input: string, label: string): string[] {
  const nodeIds = input
    .split(",")
    .map((nodeId) => nodeId.trim())
    .filter((nodeId) => nodeId.length > 0);

  if (nodeIds.length === 0) {
    throw new Error(`${label} must include at least one node ID`);
  }

  const seen = new Set<string>();
  for (const nodeId of nodeIds) {
    if (seen.has(nodeId)) {
      throw new Error(`${label} cannot include duplicate node ID "${nodeId}"`);
    }
    seen.add(nodeId);
  }

  return nodeIds;
}

export function formatDuration(milliseconds: number | bigint): string {
  if (typeof milliseconds === "bigint") {
    milliseconds = Number(milliseconds);
  }

  if (!Number.isFinite(milliseconds) || milliseconds <= 0) {
    return "0s";
  }

  const hours = Math.floor(milliseconds / 3_600_000);
  const minutes = Math.floor((milliseconds % 3_600_000) / 60_000);
  const seconds = Math.floor((milliseconds % 60_000) / 1_000);
  const ms = Math.floor(milliseconds % 1_000);

  if (hours > 0) {
    return `${hours}h${minutes.toString().padStart(2, "0")}m`;
  }
  if (minutes > 0) {
    return `${minutes}m${seconds.toString().padStart(2, "0")}s`;
  }
  if (seconds > 0) {
    return `${seconds}s`;
  }
  return `${ms}ms`;
}

export function resolveConfiguredNodeRateShare(
  nodeIds: string[],
  nodeWeights: Record<string, number>,
  nodeId: string,
): number {
  const activeNodeIds = nodeIds.length > 0 ? nodeIds : [nodeId];
  let totalWeight = 0;

  for (const activeNodeId of activeNodeIds) {
    totalWeight += nodeWeights[activeNodeId] ?? 1;
  }

  if (!Number.isFinite(totalWeight) || totalWeight <= 0) {
    return 1 / activeNodeIds.length;
  }

  return (nodeWeights[nodeId] ?? 1) / totalWeight;
}

function alignWorkloadProfileToNodeIds(
  profile: WorkloadProfile,
  nodeIds: string[],
): WorkloadProfile {
  const nodeWeights = { ...profile.nodeWeights };

  for (const nodeId of nodeIds) {
    if (!(nodeId in nodeWeights)) {
      nodeWeights[nodeId] = 1;
    }
  }

  return {
    ...profile,
    nodeWeights,
  };
}

export function createRunLabel(wallStartMs: number, runName: string | null): string {
  const stamp = new Date(wallStartMs)
    .toISOString()
    .replace(/[:]/g, "-")
    .replace(/\.\d{3}Z$/, "Z");
  const safeRunName = runName ? `-${sanitizeRunName(runName)}` : "";
  return `${stamp}${safeRunName}`;
}

function sanitizeRunName(input: string): string {
  return (
    input
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9-_]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 64) || "run"
  );
}

export function buildRunArtifacts(
  config: RuntimeConfig,
  wallStartMs: number,
): RuntimeArtifacts {
  const explicitSummaryPath = config.outputPath ? resolve(config.outputPath) : null;
  const runDir = explicitSummaryPath
    ? dirname(explicitSummaryPath)
    : resolve(config.outputDir, createRunLabel(wallStartMs, config.runName));

  return {
    runDir,
    summaryPath: explicitSummaryPath ?? resolve(runDir, "summary.json"),
    heartbeatPath: resolve(runDir, "heartbeats.ndjson"),
    anomalyPath: resolve(runDir, "anomalies.ndjson"),
    duplicateLeakPath: resolve(runDir, "duplicate-leaks.ndjson"),
    lifecyclePath: resolve(runDir, "lifecycle.ndjson"),
    configPath: resolve(runDir, "run-config.json"),
    orchestratorLogPath: resolve(runDir, "orchestrator.log"),
    collectorStdoutPath: resolve(runDir, "collector.stdout.log"),
    collectorStderrPath: resolve(runDir, "collector.stderr.log"),
    nodesDir: resolve(runDir, "nodes"),
  };
}

export function serializeConfig(config: RuntimeConfig): Record<string, unknown> {
  return {
    ...config,
    durationMs: config.durationMs.toString(),
    steadyForMs: config.steadyForMs.toString(),
    maxLateArrivalMs: config.maxLateArrivalMs.toString(),
    maxTailDrainMs: config.maxTailDrainMs.toString(),
    reportEveryMs: config.reportEveryMs.toString(),
    faultInjection: {
      ...config.faultInjection,
      darkIntervalMs: config.faultInjection.darkIntervalMs.toString(),
      darkDurationMs: config.faultInjection.darkDurationMs.toString(),
      darkStartAfterMs: config.faultInjection.darkStartAfterMs.toString(),
      darkStaggerMs: config.faultInjection.darkStaggerMs.toString(),
      jitterExtraDelayMinMs: config.faultInjection.jitterExtraDelayMinMs.toString(),
      jitterExtraDelayMaxMs: config.faultInjection.jitterExtraDelayMaxMs.toString(),
      jitterSpikeMinMs: config.faultInjection.jitterSpikeMinMs.toString(),
      jitterSpikeMaxMs: config.faultInjection.jitterSpikeMaxMs.toString(),
    },
  };
}

export function deserializeConfig(serialized: Record<string, any>): RuntimeConfig {
  return {
    ...serialized,
    durationMs: BigInt(serialized.durationMs),
    steadyForMs: BigInt(serialized.steadyForMs),
    maxLateArrivalMs: BigInt(serialized.maxLateArrivalMs),
    maxTailDrainMs: BigInt(serialized.maxTailDrainMs),
    reportEveryMs: BigInt(serialized.reportEveryMs),
    faultInjection: {
      ...serialized.faultInjection,
      darkIntervalMs: BigInt(serialized.faultInjection.darkIntervalMs),
      darkDurationMs: BigInt(serialized.faultInjection.darkDurationMs),
      darkStartAfterMs: BigInt(serialized.faultInjection.darkStartAfterMs),
      darkStaggerMs: BigInt(serialized.faultInjection.darkStaggerMs),
      jitterExtraDelayMinMs: BigInt(
        serialized.faultInjection.jitterExtraDelayMinMs,
      ),
      jitterExtraDelayMaxMs: BigInt(
        serialized.faultInjection.jitterExtraDelayMaxMs,
      ),
      jitterSpikeMinMs: BigInt(serialized.faultInjection.jitterSpikeMinMs),
      jitterSpikeMaxMs: BigInt(serialized.faultInjection.jitterSpikeMaxMs),
    },
  } as RuntimeConfig;
}

export function createSimulationClock(config: RuntimeConfig & { wallStartMs: number }) {
  const simulatedStartMs = BigInt(config.wallStartMs);
  const simulatedEndMs = simulatedStartMs + config.durationMs;

  const simulationNowMs = () => {
    const elapsedWallMs = Math.max(0, Date.now() - config.wallStartMs);
    return simulatedStartMs + BigInt(Math.floor(elapsedWallMs * config.timeScale));
  };

  return {
    simulatedStartMs,
    simulatedEndMs,
    simulationNowMs,
  };
}

export function formatOperatorError(error: unknown): string {
  if (error instanceof Error) {
    return `Error: ${error.message}`;
  }

  return `Error: ${String(error)}`;
}

export async function sleepForSimulatedGap(
  gapMs: bigint,
  timeScale: number,
): Promise<void> {
  if (gapMs <= 0n) {
    return;
  }

  const wallDelayMs = Math.max(1, Math.min(1_000, Math.ceil(Number(gapMs) / timeScale)));
  await new Promise((resolveDelay) => {
    setTimeout(resolveDelay, wallDelayMs);
  });
}

export function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

export function randomBetween(min: number, max: number): number {
  return min + Math.random() * (max - min);
}

export function sampleIntervalMs(ratePerSecond: number): bigint {
  const u = Math.max(1e-12, Math.random());
  return BigInt(Math.max(1, Math.round((-Math.log(u) * 1_000) / ratePerSecond)));
}

export function serializeEventForWire(event: SimulationEvent): Record<string, unknown> {
  return {
    ...event,
    clock: {
      ...event.clock,
      physicalTimeMs: event.clock.physicalTimeMs.toString(),
    },
    sequence: event.sequence?.toString(),
    ingestedAt: event.ingestedAt?.toString(),
  };
}

export function deserializeEventFromWire(event: Record<string, any>): SimulationEvent {
  return {
    ...event,
    clock: {
      ...event.clock,
      physicalTimeMs: BigInt(event.clock.physicalTimeMs),
    },
    sequence: event.sequence !== undefined ? BigInt(event.sequence) : undefined,
    ingestedAt: event.ingestedAt !== undefined ? BigInt(event.ingestedAt) : undefined,
  } as SimulationEvent;
}

export function serializeHintForWire(hint: HintEvent): Record<string, unknown> {
  return {
    ...hint,
    clock: {
      ...hint.clock,
      physicalTimeMs: hint.clock.physicalTimeMs.toString(),
    },
  };
}

export function deserializeHintFromWire(hint: Record<string, any>): HintEvent {
  return {
    ...hint,
    clock: {
      ...hint.clock,
      physicalTimeMs: BigInt(hint.clock.physicalTimeMs),
    },
  } as HintEvent;
}

function resolveWorkloadProfile({
  profileName,
  profileFile,
}: {
  profileName: string;
  profileFile: string | null;
}): WorkloadProfile {
  if (profileFile) {
    const resolvedProfilePath = resolve(profileFile);
    const override = readJsonProfile(resolvedProfilePath);
    validateWorkloadProfileOverride(override, resolvedProfilePath);
    return validateWorkloadProfile(
      mergeWorkloadProfiles(DEFAULT_WORKLOAD_PROFILE, override),
      resolvedProfilePath,
    );
  }

  if (!profileName || profileName === DEFAULT_WORKLOAD_PROFILE.name) {
    return validateWorkloadProfile(
      DEFAULT_WORKLOAD_PROFILE,
      `built-in workload profile "${DEFAULT_WORKLOAD_PROFILE.name}"`,
    );
  }

  const profilePath = resolve(PROFILE_DIR, `${profileName}.json`);
  const override = readJsonProfile(profilePath);
  validateWorkloadProfileOverride(override, profilePath);
  return validateWorkloadProfile(
    mergeWorkloadProfiles(DEFAULT_WORKLOAD_PROFILE, override),
    profilePath,
  );
}

function readJsonProfile(path: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8")) as unknown;
    return expectObjectRecord(
      parsed,
      path,
      "workload profile root",
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Unable to read workload profile ${path}: ${message}`);
  }
}

function mergeWorkloadProfiles(
  base: WorkloadProfile,
  override: Record<string, unknown>,
): WorkloadProfile {
  const nodeWeights =
    override.nodeWeights && typeof override.nodeWeights === "object" && !Array.isArray(override.nodeWeights)
      ? (override.nodeWeights as Record<string, unknown>)
      : {};
  const phaseRates =
    override.phaseRates && typeof override.phaseRates === "object" && !Array.isArray(override.phaseRates)
      ? (override.phaseRates as Record<string, unknown>)
      : {};
  const dependencies =
    override.dependencies &&
    typeof override.dependencies === "object" &&
    !Array.isArray(override.dependencies)
      ? (override.dependencies as Record<string, unknown>)
      : {};
  const duplicates =
    override.duplicates && typeof override.duplicates === "object" && !Array.isArray(override.duplicates)
      ? (override.duplicates as Record<string, unknown>)
      : {};
  const ordering =
    override.ordering && typeof override.ordering === "object" && !Array.isArray(override.ordering)
      ? (override.ordering as Record<string, unknown>)
      : {};
  const delays =
    override.delays && typeof override.delays === "object" && !Array.isArray(override.delays)
      ? (override.delays as Record<string, unknown>)
      : {};
  const steadyDelays =
    delays.steady && typeof delays.steady === "object" && !Array.isArray(delays.steady)
      ? (delays.steady as Record<string, unknown>)
      : {};
  const chaoticDelays =
    delays.chaotic && typeof delays.chaotic === "object" && !Array.isArray(delays.chaotic)
      ? (delays.chaotic as Record<string, unknown>)
      : {};

  return {
    ...base,
    ...override,
    nodeWeights: {
      ...base.nodeWeights,
      ...nodeWeights,
    } as WorkloadProfile["nodeWeights"],
    phaseRates: {
      ...base.phaseRates,
      ...phaseRates,
    } as WorkloadProfile["phaseRates"],
    dependencies: {
      ...base.dependencies,
      ...dependencies,
    } as WorkloadProfile["dependencies"],
    duplicates: {
      ...base.duplicates,
      ...duplicates,
    } as WorkloadProfile["duplicates"],
    ordering: {
      ...base.ordering,
      ...ordering,
    } as WorkloadProfile["ordering"],
    delays: {
      ...base.delays,
      ...delays,
      steady: {
        ...base.delays.steady,
        ...steadyDelays,
      } as WorkloadProfile["delays"]["steady"],
      chaotic: {
        ...base.delays.chaotic,
        ...chaoticDelays,
      } as WorkloadProfile["delays"]["chaotic"],
    } as WorkloadProfile["delays"],
  } as WorkloadProfile;
}

function validateWorkloadProfileOverride(
  override: Record<string, unknown>,
  source: string,
): void {
  assertAllowedKeys(
    override,
    [
      "name",
      "description",
      "nodeWeights",
      "phaseRates",
      "dependencies",
      "duplicates",
      "ordering",
      "delays",
    ],
    source,
    "workload profile root",
  );

  if ("name" in override) {
    assertNonEmptyString(override.name, source, "name");
  }

  if ("description" in override) {
    assertString(override.description, source, "description");
  }

  if ("nodeWeights" in override) {
    const nodeWeights = expectObjectRecord(
      override.nodeWeights,
      source,
      "nodeWeights",
    );
    for (const [nodeId, weight] of Object.entries(nodeWeights)) {
      assertPositiveFiniteNumber(weight, source, `nodeWeights.${nodeId}`);
    }
  }

  if ("phaseRates" in override) {
    const phaseRates = expectObjectRecord(
      override.phaseRates,
      source,
      "phaseRates",
    );
    assertAllowedKeys(
      phaseRates,
      [
        "steadyEventsPerSecond",
        "chaosMultiplier",
        "chaosJitterMin",
        "chaosJitterMax",
      ],
      source,
      "phaseRates",
    );

    if ("steadyEventsPerSecond" in phaseRates) {
      assertPositiveFiniteNumber(
        phaseRates.steadyEventsPerSecond,
        source,
        "phaseRates.steadyEventsPerSecond",
      );
    }

    if ("chaosMultiplier" in phaseRates) {
      assertPositiveFiniteNumber(
        phaseRates.chaosMultiplier,
        source,
        "phaseRates.chaosMultiplier",
      );
    }

    if ("chaosJitterMin" in phaseRates) {
      assertPositiveFiniteNumber(
        phaseRates.chaosJitterMin,
        source,
        "phaseRates.chaosJitterMin",
      );
    }

    if ("chaosJitterMax" in phaseRates) {
      assertPositiveFiniteNumber(
        phaseRates.chaosJitterMax,
        source,
        "phaseRates.chaosJitterMax",
      );
    }
  }

  if ("dependencies" in override) {
    const dependencies = expectObjectRecord(
      override.dependencies,
      source,
      "dependencies",
    );
    assertAllowedKeys(
      dependencies,
      [
        "steadySameNodeChance",
        "steadyCrossNodeChance",
        "chaoticSameNodeChance",
        "chaoticCrossNodeChance",
        "sameNodeParentChance",
        "crossNodeParentChance",
      ],
      source,
      "dependencies",
    );

    for (const key of Object.keys(dependencies)) {
      assertUnitInterval(
        dependencies[key],
        source,
        `dependencies.${key}`,
      );
    }
  }

  if ("duplicates" in override) {
    const duplicates = expectObjectRecord(
      override.duplicates,
      source,
      "duplicates",
    );
    assertAllowedKeys(
      duplicates,
      ["steadyChance", "chaoticChance"],
      source,
      "duplicates",
    );

    for (const key of Object.keys(duplicates)) {
      assertUnitInterval(
        duplicates[key],
        source,
        `duplicates.${key}`,
      );
    }
  }

  if ("ordering" in override) {
    const ordering = expectObjectRecord(
      override.ordering,
      source,
      "ordering",
    );
    assertAllowedKeys(
      ordering,
      ["steadyPreserveOrderChance", "chaoticPreserveOrderChance"],
      source,
      "ordering",
    );

    for (const key of Object.keys(ordering)) {
      assertUnitInterval(
        ordering[key],
        source,
        `ordering.${key}`,
      );
    }
  }

  if ("delays" in override) {
    const delays = expectObjectRecord(
      override.delays,
      source,
      "delays",
    );
    assertAllowedKeys(
      delays,
      ["steady", "chaotic"],
      source,
      "delays",
    );

    if ("steady" in delays) {
      const steady = expectObjectRecord(
        delays.steady,
        source,
        "delays.steady",
      );
      assertAllowedKeys(
        steady,
        [
          "baseMinMs",
          "baseMaxMs",
          "spikeChance",
          "spikeMinMs",
          "spikeMaxMs",
        ],
        source,
        "delays.steady",
      );

      for (const key of ["baseMinMs", "baseMaxMs", "spikeMinMs", "spikeMaxMs"] as const) {
        if (key in steady) {
          assertNonNegativeFiniteNumber(
            steady[key],
            source,
            `delays.steady.${key}`,
          );
        }
      }

      if ("spikeChance" in steady) {
        assertUnitInterval(
          steady.spikeChance,
          source,
          "delays.steady.spikeChance",
        );
      }
    }

    if ("chaotic" in delays) {
      const chaotic = expectObjectRecord(
        delays.chaotic,
        source,
        "delays.chaotic",
      );
      assertAllowedKeys(
        chaotic,
        [
          "baseMinMs",
          "baseMaxMs",
          "slowSpikeChance",
          "slowSpikeMinMs",
          "slowSpikeMaxMs",
          "lateSpikeChance",
          "lateSpikeMinMs",
          "lateSpikeMaxMs",
          "extremeSpikeChance",
          "extremeSpikeMinMs",
          "extremeSpikeMaxMs",
        ],
        source,
        "delays.chaotic",
      );

      for (const key of [
        "baseMinMs",
        "baseMaxMs",
        "slowSpikeMinMs",
        "slowSpikeMaxMs",
        "lateSpikeMinMs",
        "lateSpikeMaxMs",
        "extremeSpikeMinMs",
        "extremeSpikeMaxMs",
      ] as const) {
        if (key in chaotic) {
          assertNonNegativeFiniteNumber(
            chaotic[key],
            source,
            `delays.chaotic.${key}`,
          );
        }
      }

      for (const key of [
        "slowSpikeChance",
        "lateSpikeChance",
        "extremeSpikeChance",
      ] as const) {
        if (key in chaotic) {
          assertUnitInterval(
            chaotic[key],
            source,
            `delays.chaotic.${key}`,
          );
        }
      }
    }
  }
}

function validateWorkloadProfile(
  profile: WorkloadProfile,
  source: string,
): WorkloadProfile {
  assertNonEmptyString(profile.name, source, "name");
  assertString(profile.description, source, "description");

  const nodeWeightEntries = Object.entries(profile.nodeWeights);
  if (nodeWeightEntries.length === 0) {
    throw invalidWorkloadProfileError(
      source,
      "nodeWeights must define at least one node",
    );
  }

  for (const [nodeId, weight] of nodeWeightEntries) {
    assertPositiveFiniteNumber(weight, source, `nodeWeights.${nodeId}`);
  }

  assertPositiveFiniteNumber(
    profile.phaseRates.steadyEventsPerSecond,
    source,
    "phaseRates.steadyEventsPerSecond",
  );
  assertPositiveFiniteNumber(
    profile.phaseRates.chaosMultiplier,
    source,
    "phaseRates.chaosMultiplier",
  );
  assertPositiveFiniteNumber(
    profile.phaseRates.chaosJitterMin,
    source,
    "phaseRates.chaosJitterMin",
  );
  assertPositiveFiniteNumber(
    profile.phaseRates.chaosJitterMax,
    source,
    "phaseRates.chaosJitterMax",
  );
  assertMinLessThanOrEqual(
    profile.phaseRates.chaosJitterMin,
    profile.phaseRates.chaosJitterMax,
    source,
    "phaseRates.chaosJitterMin",
    "phaseRates.chaosJitterMax",
  );

  const bounded = [
    [
      "dependencies.steadySameNodeChance",
      profile.dependencies.steadySameNodeChance,
    ],
    [
      "dependencies.steadyCrossNodeChance",
      profile.dependencies.steadyCrossNodeChance,
    ],
    [
      "dependencies.chaoticSameNodeChance",
      profile.dependencies.chaoticSameNodeChance,
    ],
    [
      "dependencies.chaoticCrossNodeChance",
      profile.dependencies.chaoticCrossNodeChance,
    ],
    ["dependencies.sameNodeParentChance", profile.dependencies.sameNodeParentChance],
    ["dependencies.crossNodeParentChance", profile.dependencies.crossNodeParentChance],
    ["duplicates.steadyChance", profile.duplicates.steadyChance],
    ["duplicates.chaoticChance", profile.duplicates.chaoticChance],
    [
      "ordering.steadyPreserveOrderChance",
      profile.ordering.steadyPreserveOrderChance,
    ],
    [
      "ordering.chaoticPreserveOrderChance",
      profile.ordering.chaoticPreserveOrderChance,
    ],
  ] as Array<[string, number]>;

  for (const [label, value] of bounded) {
    assertUnitInterval(value, source, label);
  }

  assertProbabilitySumAtMostOne(
    profile.dependencies.steadySameNodeChance,
    profile.dependencies.steadyCrossNodeChance,
    source,
    "dependencies.steadySameNodeChance",
    "dependencies.steadyCrossNodeChance",
  );
  assertProbabilitySumAtMostOne(
    profile.dependencies.chaoticSameNodeChance,
    profile.dependencies.chaoticCrossNodeChance,
    source,
    "dependencies.chaoticSameNodeChance",
    "dependencies.chaoticCrossNodeChance",
  );

  for (const [label, value] of [
    ["delays.steady.baseMinMs", profile.delays.steady.baseMinMs],
    ["delays.steady.baseMaxMs", profile.delays.steady.baseMaxMs],
    ["delays.steady.spikeMinMs", profile.delays.steady.spikeMinMs],
    ["delays.steady.spikeMaxMs", profile.delays.steady.spikeMaxMs],
    ["delays.chaotic.baseMinMs", profile.delays.chaotic.baseMinMs],
    ["delays.chaotic.baseMaxMs", profile.delays.chaotic.baseMaxMs],
    ["delays.chaotic.slowSpikeMinMs", profile.delays.chaotic.slowSpikeMinMs],
    ["delays.chaotic.slowSpikeMaxMs", profile.delays.chaotic.slowSpikeMaxMs],
    ["delays.chaotic.lateSpikeMinMs", profile.delays.chaotic.lateSpikeMinMs],
    ["delays.chaotic.lateSpikeMaxMs", profile.delays.chaotic.lateSpikeMaxMs],
    [
      "delays.chaotic.extremeSpikeMinMs",
      profile.delays.chaotic.extremeSpikeMinMs,
    ],
    [
      "delays.chaotic.extremeSpikeMaxMs",
      profile.delays.chaotic.extremeSpikeMaxMs,
    ],
  ] as Array<[string, number]>) {
    assertNonNegativeFiniteNumber(value, source, label);
  }

  for (const [label, value] of [
    ["delays.steady.spikeChance", profile.delays.steady.spikeChance],
    ["delays.chaotic.slowSpikeChance", profile.delays.chaotic.slowSpikeChance],
    ["delays.chaotic.lateSpikeChance", profile.delays.chaotic.lateSpikeChance],
    [
      "delays.chaotic.extremeSpikeChance",
      profile.delays.chaotic.extremeSpikeChance,
    ],
  ] as Array<[string, number]>) {
    assertUnitInterval(value, source, label);
  }

  assertMinLessThanOrEqual(
    profile.delays.steady.baseMinMs,
    profile.delays.steady.baseMaxMs,
    source,
    "delays.steady.baseMinMs",
    "delays.steady.baseMaxMs",
  );
  assertMinLessThanOrEqual(
    profile.delays.steady.spikeMinMs,
    profile.delays.steady.spikeMaxMs,
    source,
    "delays.steady.spikeMinMs",
    "delays.steady.spikeMaxMs",
  );
  assertMinLessThanOrEqual(
    profile.delays.chaotic.baseMinMs,
    profile.delays.chaotic.baseMaxMs,
    source,
    "delays.chaotic.baseMinMs",
    "delays.chaotic.baseMaxMs",
  );
  assertMinLessThanOrEqual(
    profile.delays.chaotic.slowSpikeMinMs,
    profile.delays.chaotic.slowSpikeMaxMs,
    source,
    "delays.chaotic.slowSpikeMinMs",
    "delays.chaotic.slowSpikeMaxMs",
  );
  assertMinLessThanOrEqual(
    profile.delays.chaotic.lateSpikeMinMs,
    profile.delays.chaotic.lateSpikeMaxMs,
    source,
    "delays.chaotic.lateSpikeMinMs",
    "delays.chaotic.lateSpikeMaxMs",
  );
  assertMinLessThanOrEqual(
    profile.delays.chaotic.extremeSpikeMinMs,
    profile.delays.chaotic.extremeSpikeMaxMs,
    source,
    "delays.chaotic.extremeSpikeMinMs",
    "delays.chaotic.extremeSpikeMaxMs",
  );

  return profile;
}

function expectObjectRecord(
  value: unknown,
  source: string,
  label: string,
): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw invalidWorkloadProfileError(source, `"${label}" must be a JSON object`);
  }

  return value as Record<string, unknown>;
}

function assertAllowedKeys(
  record: Record<string, unknown>,
  allowedKeys: string[],
  source: string,
  label: string,
): void {
  const allowed = new Set(allowedKeys);
  for (const key of Object.keys(record)) {
    if (!allowed.has(key)) {
      const detail =
        label === "workload profile root"
          ? `unknown field "${key}"`
          : `unknown field "${key}" under "${label}"`;
      throw invalidWorkloadProfileError(source, detail);
    }
  }
}

function assertString(value: unknown, source: string, label: string): void {
  if (typeof value !== "string") {
    throw invalidWorkloadProfileError(source, `"${label}" must be a string`);
  }
}

function assertNonEmptyString(value: unknown, source: string, label: string): void {
  if (typeof value !== "string" || value.trim() === "") {
    throw invalidWorkloadProfileError(
      source,
      `"${label}" must be a non-empty string`,
    );
  }
}

function assertPositiveFiniteNumber(
  value: unknown,
  source: string,
  label: string,
): void {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw invalidWorkloadProfileError(
      source,
      `"${label}" must be a positive finite number`,
    );
  }
}

function assertNonNegativeFiniteNumber(
  value: unknown,
  source: string,
  label: string,
): void {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw invalidWorkloadProfileError(
      source,
      `"${label}" must be a non-negative finite number`,
    );
  }
}

function assertUnitInterval(
  value: unknown,
  source: string,
  label: string,
): void {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > 1) {
    throw invalidWorkloadProfileError(
      source,
      `"${label}" must be between 0 and 1`,
    );
  }
}

function assertMinLessThanOrEqual(
  minValue: number,
  maxValue: number,
  source: string,
  minLabel: string,
  maxLabel: string,
): void {
  if (minValue > maxValue) {
    throw invalidWorkloadProfileError(
      source,
      `"${minLabel}" cannot be greater than "${maxLabel}"`,
    );
  }
}

function assertProbabilitySumAtMostOne(
  left: number,
  right: number,
  source: string,
  leftLabel: string,
  rightLabel: string,
): void {
  if (left + right > 1) {
    throw invalidWorkloadProfileError(
      source,
      `"${leftLabel}" + "${rightLabel}" cannot be greater than 1`,
    );
  }
}

function invalidWorkloadProfileError(source: string, detail: string): Error {
  return new Error(`Invalid workload profile ${source}: ${detail}`);
}
