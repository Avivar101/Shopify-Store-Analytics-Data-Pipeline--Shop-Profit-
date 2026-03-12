import { useState } from "react";

export default function AppHome() {
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState("Not synced yet");
  const [syncedAt, setSyncedAt] = useState<string | null>(null);
  const [dashboardKey, setDashboardKey] = useState(0);

  async function runSync() {
    setLoading(true);
    setStatus("Sync running...");

    try {
      const res = await fetch("/api/sync", {
        method: "POST",
      });

      const data = await res.json();

      if (data.ok) {
        setStatus(data.message);
        setSyncedAt(data.syncedAt);
        setDashboardKey((prev) => prev + 1);
      } else {
        setStatus(data.message || "Sync failed");
      }
    } catch {
      setStatus("Sync failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ padding: "24px" }}>
      <h1>ShopProfit</h1>
      <p>Embedded Shopify analytics powered by dbt, DuckDB, and Metabase.</p>

      <div
        style={{
          marginTop: "16px",
          marginBottom: "24px",
          padding: "16px",
          border: "1px solid #ddd",
          borderRadius: "8px",
          background: "#fafafa",
        }}
      >
        <p><strong>Status:</strong> {status}</p>
        <p><strong>Last synced:</strong> {syncedAt ?? "Never"}</p>

        <button onClick={runSync} disabled={loading}>
          {loading ? "Syncing..." : "Sync now"}
        </button>
      </div>

      <div>
        <h2>Analytics Dashboard</h2>
        <p>If this is your first time, run a sync first.</p>

        <iframe
          key={dashboardKey}
          src="http://localhost:3000/public/dashboard/88265af6-94dd-4af7-9b2f-3d47b50f9e0e"
          title="ShopProfit Metabase Dashboard"
          width="100%"
          height="900"
          style={{
            border: "1px solid #ddd",
            borderRadius: "8px",
            background: "#fff",
          }}
        />
      </div>
    </div>
  );
}