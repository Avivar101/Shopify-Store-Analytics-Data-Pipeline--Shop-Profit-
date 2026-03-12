import { json } from "@remix-run/node";
import { exec } from "node:child_process";
import { promisify } from "node:util";

const execAsync = promisify(exec);

const PYTHON_PATH = "C:/Users/USER/Documents/GitHub/shop_profit/.venv/Scripts/python.exe";
const DBT_PATH = "C:/Users/USER/Documents/GitHub/shop_profit/.venv/Scripts/dbt.exe";
const INGESTION_DIR = "C:/Users/USER/Documents/GitHub/shop_profit/shopify_ingestion";
const DBT_DIR = "C:/Users/USER/Documents/GitHub/shop_profit/dbt_transform";

export async function action() {
  try {
    console.log("Starting ingestion...");
    const ingestion = await execAsync(
      `"${PYTHON_PATH}" run_ingestion.py`,
      { cwd: INGESTION_DIR }
    );
    console.log("Ingestion stdout:", ingestion.stdout);
    console.log("Ingestion stderr:", ingestion.stderr);

    console.log("Running dbt...");
    const dbt = await execAsync(
      `"${DBT_PATH}" build`,
      { cwd: DBT_DIR }
    );
    console.log("dbt stdout:", dbt.stdout);
    console.log("dbt stderr:", dbt.stderr);

    return json({
      ok: true,
      message: "Sync completed successfully",
      syncedAt: new Date().toISOString(),
    });
  } catch (error: any) {
    console.error("Sync failed:", error);
    console.error("stdout:", error?.stdout);
    console.error("stderr:", error?.stderr);

    return json(
      {
        ok: false,
        message: "Sync failed",
        error: String(error),
        stdout: error?.stdout ?? null,
        stderr: error?.stderr ?? null,
      },
      { status: 500 }
    );
  }
}