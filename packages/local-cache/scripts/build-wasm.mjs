import { cpSync, mkdirSync, rmSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";

const packageDir = resolve(process.cwd());
const crateDir = resolve(packageDir, "../../crates/nexrade-wasm");
const crateOutput = resolve(crateDir, "wasm");
const packageOutput = resolve(packageDir, "wasm");

const result = spawnSync(
  process.platform === "win32" ? "wasm-pack.exe" : "wasm-pack",
  ["build", crateDir, "--target", "web", "--out-dir", "wasm", "--release", "--features", "wasm"],
  { stdio: "inherit" },
);

if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);

rmSync(packageOutput, { recursive: true, force: true });
mkdirSync(dirname(packageOutput), { recursive: true });
cpSync(crateOutput, packageOutput, { recursive: true });
rmSync(resolve(packageOutput, ".gitignore"), { force: true });
