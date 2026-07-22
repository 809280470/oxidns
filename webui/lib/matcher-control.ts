import type { PluginInstance } from "./types";

export type MatcherControlAvailability = "loading" | "ready" | "unavailable";
export type MatcherRuntimeMode = "normal" | "force_miss" | "force_hit";
export type ForcedMatcherRuntimeMode = Exclude<MatcherRuntimeMode, "normal">;

export type MatcherModeChangePlan =
  | { kind: "apply"; mode: "normal" }
  | { kind: "confirm"; mode: ForcedMatcherRuntimeMode };

export interface MatcherControlState {
  availability: MatcherControlAvailability;
  pending: boolean;
  mode: MatcherRuntimeMode | null;
  error?: string;
}

export function planMatcherModeChange(
  mode: MatcherRuntimeMode,
): MatcherModeChangePlan {
  return mode === "normal"
    ? { kind: "apply", mode }
    : { kind: "confirm", mode };
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
          mode: null,
        },
      ]),
  );
}
