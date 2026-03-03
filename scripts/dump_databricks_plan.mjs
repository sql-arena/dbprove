#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { chromium } from "playwright";

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "-v") {
      args.verbose = "1";
      continue;
    }
    if (!a.startsWith("--")) continue;
    const key = a.slice(2);
    const val = argv[i + 1] && !argv[i + 1].startsWith("--") && !argv[i+1].startsWith("-") ? argv[++i] : "1";
    args[key] = val;
  }
  return args;
}

function usageAndExit(msg) {
  if (msg) console.error(msg);
  console.error(`Usage:
  node scripts/dump_databricks_plan.mjs \\
    --workspace <workspace URL> \\
    --o <warehouse_id> \\
    --queryId <queryId>> \\
    [--headless 0|1] \\
    [--profileDir ~/.databricks-playwright-profile] \\
    [--timeoutMs 120000] \\
    [-v]

Emits the captured plan payload JSON to stdout and exits 0.

Exit codes:
  0  success (JSON written to stdout)
  10 login likely required (landed on login page in headful run)
  20 timeout (did not observe plan payload)
  30 bad args
`);
  process.exit(msg ? 30 : 0);
}

function looksLikePlanPayload(obj) {
  if (!obj || typeof obj !== "object") return false;
  const s = JSON.stringify(obj);
  return (
    s.includes("sqlgatewayHistoryGetQueryPlan") ||
    s.includes("sqlgatewayHistoryGetQueryPlanMetadata") ||
    (s.includes("\"graphs\"") && s.includes("\"nodes\"") && s.includes("\"edges\"")) ||
    s.includes("planMetadatas")
  );
}

function looksLikeLogin(title, url) {
  return /sign in|login/i.test(title) || /login\.html|sso|oauth/i.test(url);
}

(async () => {
  const args = parseArgs(process.argv.slice(2));

  const workspace = args.workspace;
  const orgId = args.o;
  const queryId = args.queryId;

  if (!workspace) {
    usageAndExit("Missing required --workspace arg.");
  }

  const isAuthOnly = !orgId || !queryId;

  const headless = (args.headless ?? "1") === "1";
  const verbose = (args.verbose ?? "0") === "1";
  const timeoutMs = parseInt(args.timeoutMs ?? "120000", 10);

  function log(...msg) {
    if (verbose) {
      console.error(`[DEBUG ${new Date().toISOString()}]`, ...msg);
    }
  }

  const profileDir =
    args.profileDir ||
    path.join(os.homedir(), ".databricks-playwright-profile");

  log("Using profile directory:", profileDir);
  fs.mkdirSync(profileDir, { recursive: true });

  // Construct URL
  let url = workspace

  if (url.endsWith("/")) {
    throw new Error("Workspace URL must not end with a slash");
  }
  if (url.startsWith("http://")) {
    throw new Error("Workspace URL must be HTTPS");
  }

  if (!url.startsWith("https://")) {
    url = `https://${url}`;
  }


  if (!isAuthOnly) {
    url += `/sql/history?o=${encodeURIComponent(orgId)}` +
           `&queryId=${encodeURIComponent(queryId)}`;
  } else {
    // Just go to the base SQL URL or workspace root if we're only authenticating
    url += `/sql`;
  }

  log("Target URL:", url);

  const context = await chromium.launchPersistentContext(profileDir, {
    headless,
  });

  log("Chromium launched in", headless ? "headless" : "headful", "mode");

  let contextClosed = false;
  context.on("close", () => {
    contextClosed = true;
  });

  const page = await context.newPage();

  page.on("close", () => {
    log("Primary page closed.");
    contextClosed = true;
  });

  let captured = null;

  page.on("request", (req) => {
    const rurl = req.url();
    if (verbose && rurl.includes("/graphql/")) {
        log(`GraphQL Request: ${req.method()} ${rurl}`);
    }
  });

  page.on("response", async (resp) => {
    try {
      const rurl = resp.url();
      const status = resp.status();
      
      if (rurl.includes("/graphql/")) {
        log(`GraphQL Response: ${status} ${rurl}`);
      }

      if (!rurl.includes("/graphql/")) return;
      if (resp.request().method() !== "POST") return;

      const ct = (resp.headers()["content-type"] || "").toLowerCase();
      if (!ct.includes("application/json")) return;

      const body = await resp.json().catch(() => null);
      if (!body) return;

      // Prefer the specific op, but accept other plan-shaped payloads.
      if (
        rurl.includes("/graphql/HistoryStatementPlanById") ||
        rurl.includes("/graphql/HistoryStatementPlanMetadata")
      ) {
        log(`Response arrived from ${rurl}`)
        if (verbose) {
           log(`Body snippet: ${JSON.stringify(body).slice(0, 500)}`);
        }
        if (looksLikePlanPayload(body)) {
          log(`Plan payload CAPTURED from ${rurl}`);
          captured = body;
        }
        else {
          log(`Ignoring payload ${JSON.stringify(body)}`)
        }
      }
    } catch (e) {
      log(`Error in response handler: ${e.message}`);
    }
  });

  log("Navigating to target URL...");
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
  log("Initial navigation done. Current URL:", page.url());

  // If we accidentally landed on a login page, in headful mode let user login; in headless fail fast.
  const title = await page.title().catch(() => "");
  const curUrl = page.url();

  log("Page title:", title);

  if (looksLikeLogin(title, curUrl)) {
    log("Login page detected!");
    if (headless) {
      log("Exiting because login is required and we are in headless mode.");
      await context.close();
      process.exit(10);
    } else {
      console.error("Login required. Complete login in the opened browser window.");
      if (isAuthOnly) {
        console.error("The browser will stay open until you close it.");
        // Wait strictly for browser to close
        while (!contextClosed) {
          await new Promise((resolve) => setTimeout(resolve, 2000));
        }
        log("Browser closed.");
      } else {
        console.error("Press Enter in this terminal when you are back on the query page.");
        await new Promise((resolve) => process.stdin.once("data", resolve));
        log("Resuming after user login...");
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
      }
    }
  }

  if (isAuthOnly) {
    log("Authentication mode: exiting now.");
    await context.close();
    process.exit(0);
  }

  // Give the UI time to issue GraphQL calls
  const start = Date.now();
  log("Waiting for plan payload to be captured...");
  while (!captured && Date.now() - start < timeoutMs) {
    await page.waitForTimeout(1000);
    if (Math.floor((Date.now() - start) / 1000) % 5 === 0) {
        log(`Still waiting... (${Math.floor((Date.now() - start) / 1000)}s elapsed)`);
    }
  }

  if (!captured) {
    log("FAILED to capture plan payload within timeout.");
    await context.close();
    process.exit(20);
  }

  log("Success! Emitting captured JSON to stdout.");
  // Emit JSON to stdout for C++ to capture
  process.stdout.write(JSON.stringify(captured));

  await context.close();
  process.exit(0);
})().catch((e) => {
  console.error(e?.stack || String(e));
  process.exit(1);
});