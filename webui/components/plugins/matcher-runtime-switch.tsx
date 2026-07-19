"use client";

import { Switch } from "@/components/ui/switch";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { WEBUI } from "@/lib/i18n";
import { useI18n } from "@/lib/i18n/provider";
import { useAppStore } from "@/lib/store";
import type { PluginInstance } from "@/lib/types";

export function MatcherRuntimeSwitch({
  plugin,
  showLabel = false,
}: {
  plugin: PluginInstance;
  showLabel?: boolean;
}) {
  const { t } = useI18n();
  const control = useAppStore((state) => state.matcherControls[plugin.name]);
  const setMatcherEnabled = useAppStore((state) => state.setMatcherEnabled);
  if (plugin.type !== "matcher") return null;

  const enabled = control?.enabled ?? false;
  const ready = control?.availability === "ready" && control.enabled !== null;
  const pending = Boolean(control?.pending);
  const disabled = !ready || pending;
  const actionLabel = enabled
    ? t(WEBUI.plugins.matcherDisable)
    : t(WEBUI.plugins.matcherEnable);
  let tooltip = actionLabel;
  if (control?.availability === "loading") {
    tooltip = t(WEBUI.plugins.matcherControlLoading);
  } else if (!control || control.availability === "unavailable") {
    tooltip = t(WEBUI.plugins.matcherControlUnavailable);
    if (control?.error) tooltip = `${tooltip}: ${control.error}`;
  } else if (control.error) {
    tooltip = `${t(WEBUI.plugins.matcherControlFailed)}: ${control.error}`;
  }
  const statusLabel = ready
    ? enabled
      ? t(WEBUI.common.enabled)
      : t(WEBUI.common.disabled)
    : control?.availability === "loading"
      ? t(WEBUI.plugins.matcherControlLoading)
      : t(WEBUI.plugins.matcherControlUnavailable);

  return (
    <div
      className={showLabel ? "flex items-center gap-2" : undefined}
      onClick={(event) => event.stopPropagation()}
      onPointerDown={(event) => event.stopPropagation()}
    >
      {showLabel && (
        <span className="text-sm text-muted-foreground">
          {t(WEBUI.plugins.matcherRuntimeControl)}:{" "}
          <span className="text-foreground">{statusLabel}</span>
        </span>
      )}
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="inline-flex">
            <Switch
              size="sm"
              checked={enabled}
              disabled={disabled}
              aria-label={actionLabel}
              onCheckedChange={(checked) => {
                void setMatcherEnabled(plugin.id, checked).catch(() => {
                  // The store keeps the backend error beside this matcher.
                });
              }}
            />
          </span>
        </TooltipTrigger>
        <TooltipContent side="bottom">{tooltip}</TooltipContent>
      </Tooltip>
    </div>
  );
}
