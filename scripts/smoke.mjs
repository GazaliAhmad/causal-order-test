import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const buildDir = resolve(repoRoot, ".build", "src");

function runNodeScript(scriptName, args = []) {
  const scriptPath = resolve(buildDir, scriptName);
  const result = spawnSync(process.execPath, [scriptPath, ...args], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  if (result.status !== 0) {
    const stderr = result.stderr?.trim() ?? "";
    const stdout = result.stdout?.trim() ?? "";
    throw new Error(
      `Smoke command failed: node ${scriptName} ${args.join(" ")}\nstdout:\n${stdout}\nstderr:\n${stderr}`,
    );
  }

  return result.stdout ?? "";
}

async function main() {
  const smokeRoot = await mkdtemp(join(tmpdir(), "causal-order-testing-smoke-"));

  try {
    const helpTargets = [
      "deployment-runtime.js",
      "deployment-runtime-rejoin.js",
      "summarize-run.js",
      "compare-runs.js",
      "duplicate-leak-summary.js",
    ];

    for (const target of helpTargets) {
      const output = runNodeScript(target, ["--help"]);
      if (!output.toLowerCase().includes("usage")) {
        throw new Error(`Expected help output from ${target} to include "usage".`);
      }
    }

    runNodeScript("deployment-runtime.js", [
      "--duration",
      "15s",
      "--time-scale",
      "10",
      "--report-every",
      "5s",
      "--output-dir",
      smokeRoot,
      "--run-name",
      "smoke",
      "--profile",
      "expected-production-3way-mesh",
    ]);

    const runFolders = await readdir(smokeRoot, { withFileTypes: true });
    const runDir = runFolders.find((entry) => entry.isDirectory());
    if (!runDir) {
      throw new Error("Smoke runtime did not produce a run directory.");
    }

    const resolvedRunDir = resolve(smokeRoot, runDir.name);
    const summaryPath = resolve(resolvedRunDir, "summary.json");
    const summary = JSON.parse(await readFile(summaryPath, "utf8"));

    if (!summary?.outcome?.status) {
      throw new Error("Smoke runtime produced a summary without outcome.status.");
    }

    const reportOutput = runNodeScript("summarize-run.js", [resolvedRunDir]);
    if (!reportOutput.includes("Verdict:")) {
      throw new Error('Expected summarize-run output to include "Verdict:".');
    }

    const leakOutput = runNodeScript("duplicate-leak-summary.js", [resolvedRunDir]);
    if (!leakOutput.includes("Run:")) {
      throw new Error('Expected duplicate-leak-summary output to include "Run:".');
    }

    process.stdout.write(`Smoke test passed for ${resolvedRunDir}\n`);
  } finally {
    await rm(smokeRoot, { recursive: true, force: true });
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
  process.exitCode = 1;
});
