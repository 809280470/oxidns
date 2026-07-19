import type { PluginInstance } from "./types";

export type ProviderReloadOutcome = "idle" | "success" | "error";

export interface ProviderReloadState {
  pending: boolean;
  outcome: ProviderReloadOutcome;
  error?: string;
}

export function reconcileProviderReloads(
  plugins: PluginInstance[],
  current: Record<string, ProviderReloadState>,
): Record<string, ProviderReloadState> {
  return Object.fromEntries(
    plugins
      .filter((plugin) => plugin.type === "provider")
      .map((plugin) => [
        plugin.name,
        current[plugin.name] ?? {
          pending: false,
          outcome: "idle" as const,
        },
      ]),
  );
}
