import {
  HELP_TEXT,
  buildConfig,
  formatDuration,
  parseDurationToMs,
  type RuntimeConfig,
} from "./deployment-common.js";

export interface RejoinShapingConfig {
  recoveryWindowMs: bigint;
  tokenRatePerSecond: number;
  burstSize: number;
}

export const DEFAULT_REJOIN_SHAPING_CONFIG: RejoinShapingConfig = {
  recoveryWindowMs: parseDurationToMs("6m"),
  tokenRatePerSecond: 4,
  burstSize: 24,
};

export function buildRejoinHarnessConfig(
  argv: string[],
): { help: true; text: string } | { runtimeConfig: RuntimeConfig; rejoinShaping: RejoinShapingConfig } {
  const parsed = parseRejoinHarnessArgs(argv);
  if (parsed.help) {
    return {
      help: true,
      text: formatRejoinHarnessHelp(),
    };
  }

  const maybeRuntimeConfig = buildConfig(parsed.baseArgs);
  if ("help" in maybeRuntimeConfig) {
    return {
      help: true,
      text: formatRejoinHarnessHelp(),
    };
  }

  return {
    runtimeConfig: maybeRuntimeConfig,
    rejoinShaping: parsed.rejoinShaping,
  };
}

export function formatRejoinHarnessHelp(): string {
  return `${HELP_TEXT}

Rejoin-aware options:
  --recovery-window <value>         Duration to pace reconnect catch-up after dark_end. Default: 6m
  --recovery-rate-per-second <n>    Token refill rate for catch-up sends during recovery. Default: 4
  --recovery-burst-size <n>         Max queued catch-up sends released immediately after reconnect. Default: 24

Rejoin-aware behavior:
  - only applies to dark nodes after reconnect
  - targets catch-up and duplicate replay traffic first
  - allows live traffic to keep flowing while backlog drains more gradually

Examples:
  causal-order-testing-runtime-rejoin --duration 8h --dark-nodes edge-b,edge-f,edge-j
  causal-order-testing-runtime-rejoin --duration 8h --recovery-window 8m --recovery-rate-per-second 3 --recovery-burst-size 18`;
}

export function formatRejoinShapingSummary(config: RejoinShapingConfig): string {
  return [
    `recoveryWindow=${formatDuration(config.recoveryWindowMs)}`,
    `tokenRate=${trimNumeric(config.tokenRatePerSecond)}/s`,
    `burst=${config.burstSize}`,
  ].join(" | ");
}

export function serializeRejoinShapingConfig(
  config: RejoinShapingConfig,
): Record<string, unknown> {
  return {
    recoveryWindowMs: config.recoveryWindowMs.toString(),
    tokenRatePerSecond: config.tokenRatePerSecond,
    burstSize: config.burstSize,
  };
}

export function deserializeRejoinShapingConfig(
  serialized: Record<string, unknown>,
): RejoinShapingConfig {
  return {
    recoveryWindowMs: BigInt(serialized.recoveryWindowMs as string),
    tokenRatePerSecond: Number(serialized.tokenRatePerSecond),
    burstSize: Number(serialized.burstSize),
  };
}

function parseRejoinHarnessArgs(argv: string[]): {
  help: boolean;
  baseArgs: string[];
  rejoinShaping: RejoinShapingConfig;
} {
  const baseArgs: string[] = [];
  const rejoinShaping: RejoinShapingConfig = {
    ...DEFAULT_REJOIN_SHAPING_CONFIG,
  };
  let help = false;

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help") {
      help = true;
      continue;
    }

    const [rawKey, inlineValue] = token.split("=", 2);
    const hasInlineValue = inlineValue !== undefined;
    const value = hasInlineValue ? inlineValue : argv[index + 1];

    switch (rawKey) {
      case "--recovery-window":
        rejoinShaping.recoveryWindowMs = parsePositiveDuration(
          requireValue(rawKey, value),
          rawKey,
        );
        index += hasInlineValue ? 0 : 1;
        break;
      case "--recovery-rate-per-second":
        rejoinShaping.tokenRatePerSecond = parsePositiveFiniteNumber(
          requireValue(rawKey, value),
          rawKey,
        );
        index += hasInlineValue ? 0 : 1;
        break;
      case "--recovery-burst-size":
        rejoinShaping.burstSize = parsePositiveInteger(
          requireValue(rawKey, value),
          rawKey,
        );
        index += hasInlineValue ? 0 : 1;
        break;
      default:
        baseArgs.push(token);
        if (hasInlineValue) {
          break;
        }
        if (value !== undefined && !value.startsWith("--")) {
          baseArgs.push(value);
          index += 1;
        }
        break;
    }
  }

  return {
    help,
    baseArgs,
    rejoinShaping,
  };
}

function requireValue(flag: string, value?: string): string {
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`Missing value for ${flag}`);
  }
  return value;
}

function parsePositiveFiniteNumber(input: string, label: string): number {
  const parsed = Number(input);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive finite number`);
  }
  return parsed;
}

function parsePositiveDuration(input: string, label: string): bigint {
  const parsed = parseDurationToMs(input);
  if (parsed <= 0n) {
    throw new Error(`${label} must be a positive duration`);
  }
  return parsed;
}

function parsePositiveInteger(input: string, label: string): number {
  const parsed = Number(input);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }
  return parsed;
}

function trimNumeric(value: number): string {
  return Number.isInteger(value) ? String(value) : String(value);
}
