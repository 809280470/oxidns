import type { PluginInstance } from "./types";

export type MatcherControlAvailability = "loading" | "ready" | "unavailable";

export interface MatcherControlState {
  availability: MatcherControlAvailability;
  pending: boolean;
  enabled: boolean | null;
  error?: string;
}

export function reconcileMatcherControls(
  plugins: PluginInstance[],
  current: Record<string, MatcherControlState>,
): Record<string, MatcherControlState> {
  return Object.fromEntries(
    plugins
      .filter((plugin) => plugin.type === "matcher")
      .map((plugin) => [
        plugin.name,
        current[plugin.name] ?? {
          availability: "unavailable" as const,
          pending: false,
          enabled: null,
        },
      ]),
  );
}
