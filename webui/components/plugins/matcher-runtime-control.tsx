"use client";

import { useState, type MouseEvent } from "react";
import {
  CheckCircle2Icon,
  RotateCcwIcon,
  SlidersHorizontalIcon,
  XCircleIcon,
} from "lucide-react";

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogMedia,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Spinner } from "@/components/ui/spinner";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { WEBUI } from "@/lib/i18n";
import { useI18n } from "@/lib/i18n/provider";
import {
  planMatcherModeChange,
  type ForcedMatcherRuntimeMode,
  type MatcherRuntimeMode,
} from "@/lib/matcher-control";
import { useAppStore } from "@/lib/store";
import type { PluginInstance } from "@/lib/types";
import { cn } from "@/lib/utils";

type MatcherRuntimeControlProps = {
  plugin: PluginInstance;
  mode?: "compact" | "detail";
};

export function MatcherRuntimeControl({
  plugin,
  mode = "compact",
}: MatcherRuntimeControlProps) {
  const { t } = useI18n();
  const [confirmMode, setConfirmMode] =
    useState<ForcedMatcherRuntimeMode | null>(null);
  const control = useAppStore((state) => state.matcherControls[plugin.name]);
  const setMatcherMode = useAppStore((state) => state.setMatcherMode);

  if (plugin.type !== "matcher") return null;

  const ready = control?.availability === "ready" && control.mode !== null;
  const pending = Boolean(control?.pending);
  const currentMode = ready ? control.mode : null;
  const forcedMiss = currentMode === "force_miss";
  const forcedHit = currentMode === "force_hit";
  const forced = forcedMiss || forcedHit;
  const loading = control?.availability === "loading";
  const unavailable = !ready && !loading;
  const positiveReference = `$${plugin.name}`;
  const negativeReference = `!$${plugin.name}`;

  const statusLabel = ready
    ? forcedMiss
      ? t(WEBUI.plugins.matcherForceMiss)
      : forcedHit
        ? t(WEBUI.plugins.matcherForceHit)
        : t(WEBUI.plugins.matcherRuntimeNormal)
    : loading
      ? t(WEBUI.plugins.matcherControlLoading)
      : t(WEBUI.plugins.matcherControlUnavailable);

  const statusDescription = ready
    ? forcedMiss
      ? t(WEBUI.plugins.matcherForceMissDescription)
      : forcedHit
        ? t(WEBUI.plugins.matcherForceHitDescription)
        : t(WEBUI.plugins.matcherRuntimeNormalDescription)
    : loading
      ? t(WEBUI.plugins.matcherControlLoading)
      : t(WEBUI.plugins.matcherControlUnavailable);

  const applyForcedMode = async (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    if (!confirmMode) return;
    try {
      await setMatcherMode(plugin.id, confirmMode);
      setConfirmMode(null);
    } catch {
      // The store keeps the backend error beside this matcher.
    }
  };

  const requestModeChange = (nextMode: MatcherRuntimeMode) => {
    const plan = planMatcherModeChange(nextMode);
    if (plan.kind === "confirm") {
      setConfirmMode(plan.mode);
      return;
    }
    void setMatcherMode(plugin.id, plan.mode).catch(() => {
      // The store keeps the backend error beside this matcher.
    });
  };

  const modeIcon = loading ? (
    <Spinner data-icon="inline-start" />
  ) : forcedMiss ? (
    <XCircleIcon data-icon="inline-start" />
  ) : forcedHit ? (
    <CheckCircle2Icon data-icon="inline-start" />
  ) : (
    <SlidersHorizontalIcon data-icon="inline-start" />
  );

  const actionMenu = (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="ghost"
          size="icon-xs"
          className="text-muted-foreground hover:bg-muted hover:text-foreground"
          aria-label={t(WEBUI.plugins.matcherModeActions)}
          disabled={!ready || pending}
        >
          {modeIcon}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-44">
        <DropdownMenuLabel>{statusLabel}</DropdownMenuLabel>
        <DropdownMenuSeparator />
        {forced ? (
          <DropdownMenuItem onSelect={() => requestModeChange("normal")}>
            <RotateCcwIcon />
            {t(WEBUI.plugins.matcherRestoreAction)}
          </DropdownMenuItem>
        ) : null}
        {!forcedMiss ? (
          <DropdownMenuItem onSelect={() => requestModeChange("force_miss")}>
            <XCircleIcon />
            {t(WEBUI.plugins.matcherForceMissAction)}
          </DropdownMenuItem>
        ) : null}
        {!forcedHit ? (
          <DropdownMenuItem
            variant="destructive"
            onSelect={() => requestModeChange("force_hit")}
          >
            <CheckCircle2Icon />
            {t(WEBUI.plugins.matcherForceHitAction)}
          </DropdownMenuItem>
        ) : null}
      </DropdownMenuContent>
    </DropdownMenu>
  );

  const confirmation = (
    <AlertDialog
      open={confirmMode !== null}
      onOpenChange={(open) => {
        if (!open && !pending) setConfirmMode(null);
      }}
    >
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogMedia
            className={cn(
              confirmMode === "force_hit"
                ? "bg-destructive/10 text-destructive"
                : "bg-warning/15 text-warning-foreground",
            )}
          >
            {confirmMode === "force_hit" ? (
              <CheckCircle2Icon />
            ) : (
              <XCircleIcon />
            )}
          </AlertDialogMedia>
          <AlertDialogTitle>
            {t(
              confirmMode === "force_hit"
                ? WEBUI.plugins.matcherForceHitConfirmTitle
                : WEBUI.plugins.matcherForceMissConfirmTitle,
              { tag: plugin.name },
            )}
          </AlertDialogTitle>
          <AlertDialogDescription>
            {t(
              confirmMode === "force_hit"
                ? WEBUI.plugins.matcherForceHitConfirmDescription
                : WEBUI.plugins.matcherForceMissConfirmDescription,
            )}
          </AlertDialogDescription>
        </AlertDialogHeader>
        <div className="flex flex-col gap-2 text-sm text-muted-foreground">
          <p>
            {t(
              confirmMode === "force_hit"
                ? WEBUI.plugins.matcherForceHitImpact
                : WEBUI.plugins.matcherForceMissImpact,
              {
                positive: positiveReference,
                negative: negativeReference,
              },
            )}
          </p>
          <p>{t(WEBUI.plugins.matcherModeResetHint)}</p>
          {control?.error ? (
            <p className="text-destructive">
              {t(WEBUI.plugins.matcherControlFailed)}: {control.error}
            </p>
          ) : null}
        </div>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={pending}>
            {t(WEBUI.common.cancel)}
          </AlertDialogCancel>
          <AlertDialogAction
            variant={confirmMode === "force_hit" ? "destructive" : "warning"}
            disabled={pending || confirmMode === null}
            onClick={applyForcedMode}
          >
            {pending ? <Spinner data-icon="inline-start" /> : null}
            {t(
              confirmMode === "force_hit"
                ? WEBUI.plugins.matcherForceHitConfirmAction
                : WEBUI.plugins.matcherForceMissConfirmAction,
            )}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );

  const detailActions = (
    <div className="flex flex-wrap items-center gap-2">
      {forced ? (
        <Button
          variant="outline"
          size="sm"
          disabled={!ready || pending}
          onClick={() => requestModeChange("normal")}
        >
          {pending ? <Spinner data-icon="inline-start" /> : <RotateCcwIcon />}
          {t(WEBUI.plugins.matcherRestoreAction)}
        </Button>
      ) : null}
      {!forcedMiss ? (
        <Button
          variant="warning"
          size="sm"
          disabled={!ready || pending}
          onClick={() => requestModeChange("force_miss")}
        >
          <XCircleIcon />
          {t(WEBUI.plugins.matcherForceMissAction)}
        </Button>
      ) : null}
      {!forcedHit ? (
        <Button
          variant="destructive"
          size="sm"
          disabled={!ready || pending}
          onClick={() => requestModeChange("force_hit")}
        >
          <CheckCircle2Icon />
          {t(WEBUI.plugins.matcherForceHitAction)}
        </Button>
      ) : null}
    </div>
  );

  if (mode === "compact") {
    return (
      <div
        className="flex items-center"
        onClick={(event) => event.stopPropagation()}
        onPointerDown={(event) => event.stopPropagation()}
      >
        <Tooltip>
          <TooltipTrigger asChild>
            <span className="inline-flex">{actionMenu}</span>
          </TooltipTrigger>
          <TooltipContent side="bottom">
            {unavailable && control?.error
              ? `${statusLabel}: ${control.error}`
              : statusLabel}
          </TooltipContent>
        </Tooltip>
        {confirmation}
      </div>
    );
  }

  return (
    <Card
      className={cn(
        "mt-4",
        forcedMiss && "border-warning/40 bg-warning/5",
        forcedHit && "border-destructive/40 bg-destructive/5",
      )}
    >
      <CardHeader className="flex flex-row items-start justify-between gap-3">
        <div className="flex min-w-0 flex-col gap-1">
          <CardTitle className="text-sm">
            {t(WEBUI.plugins.matcherRuntimeControl)}
          </CardTitle>
          <CardDescription>{statusDescription}</CardDescription>
        </div>
        <Badge
          variant={
            forcedHit
              ? "destructive"
              : forcedMiss
                ? "warning"
                : ready
                  ? "secondary"
                  : "outline"
          }
        >
          {modeIcon}
          {statusLabel}
        </Badge>
      </CardHeader>
      {forced || control?.error ? (
        <CardContent className="flex flex-col gap-2 text-xs text-muted-foreground">
          {forced ? (
            <>
              <p>
                {t(
                  forcedHit
                    ? WEBUI.plugins.matcherForceHitImpact
                    : WEBUI.plugins.matcherForceMissImpact,
                  {
                    positive: positiveReference,
                    negative: negativeReference,
                  },
                )}
              </p>
              <p>{t(WEBUI.plugins.matcherModeResetHint)}</p>
            </>
          ) : null}
          {control?.error ? (
            <p className="text-destructive">
              {t(WEBUI.plugins.matcherControlFailed)}: {control.error}
            </p>
          ) : null}
        </CardContent>
      ) : null}
      <CardFooter>{detailActions}</CardFooter>
      {confirmation}
    </Card>
  );
}
