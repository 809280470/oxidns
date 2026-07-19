"use client";

import { useState, type MouseEvent } from "react";
import { RotateCcwIcon, ShieldCheckIcon, ShieldOffIcon } from "lucide-react";

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
  AlertDialogTrigger,
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
import { Spinner } from "@/components/ui/spinner";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { WEBUI } from "@/lib/i18n";
import { useI18n } from "@/lib/i18n/provider";
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
  const [confirmOpen, setConfirmOpen] = useState(false);
  const control = useAppStore((state) => state.matcherControls[plugin.name]);
  const setMatcherEnabled = useAppStore((state) => state.setMatcherEnabled);

  if (plugin.type !== "matcher") return null;

  const ready = control?.availability === "ready" && control.enabled !== null;
  const pending = Boolean(control?.pending);
  const bypassed = ready && control.enabled === false;
  const loading = control?.availability === "loading";
  const unavailable = !ready && !loading;
  const positiveReference = `$${plugin.name}`;
  const negativeReference = `!$${plugin.name}`;

  const statusLabel = ready
    ? bypassed
      ? t(WEBUI.plugins.matcherBypassed)
      : t(WEBUI.plugins.matcherRuntimeNormal)
    : loading
      ? t(WEBUI.plugins.matcherControlLoading)
      : t(WEBUI.plugins.matcherControlUnavailable);

  const statusDescription = ready
    ? bypassed
      ? t(WEBUI.plugins.matcherBypassedDescription)
      : t(WEBUI.plugins.matcherRuntimeNormalDescription)
    : loading
      ? t(WEBUI.plugins.matcherControlLoading)
      : t(WEBUI.plugins.matcherControlUnavailable);

  const handleBypass = async (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    try {
      await setMatcherEnabled(plugin.id, false);
      setConfirmOpen(false);
    } catch {
      // The store keeps the backend error beside this matcher.
    }
  };

  const handleRestore = () => {
    void setMatcherEnabled(plugin.id, true).catch(() => {
      // The store keeps the backend error beside this matcher.
    });
  };

  const bypassDialog = (
    <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
      <AlertDialogTrigger asChild>
        <Button
          variant="outline"
          size={mode === "compact" ? "icon-xs" : "sm"}
          aria-label={
            mode === "compact"
              ? t(WEBUI.plugins.matcherBypassAction)
              : undefined
          }
          disabled={!ready || pending || bypassed}
        >
          {loading ? (
            <Spinner data-icon="inline-start" />
          ) : (
            <ShieldOffIcon data-icon="inline-start" />
          )}
          {mode === "detail" ? t(WEBUI.plugins.matcherBypassAction) : null}
        </Button>
      </AlertDialogTrigger>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogMedia className="bg-warning/15 text-warning-foreground">
            <ShieldOffIcon />
          </AlertDialogMedia>
          <AlertDialogTitle>
            {t(WEBUI.plugins.matcherBypassConfirmTitle, {
              tag: plugin.name,
            })}
          </AlertDialogTitle>
          <AlertDialogDescription>
            {t(WEBUI.plugins.matcherBypassConfirmDescription)}
          </AlertDialogDescription>
        </AlertDialogHeader>
        <div className="flex flex-col gap-2 text-sm text-muted-foreground">
          <p>
            {t(WEBUI.plugins.matcherBypassPositiveImpact, {
              reference: positiveReference,
            })}
          </p>
          <p>
            {t(WEBUI.plugins.matcherBypassNegatedImpact, {
              reference: negativeReference,
            })}
          </p>
          <p>{t(WEBUI.plugins.matcherBypassResetHint)}</p>
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
            variant="warning"
            disabled={pending}
            onClick={handleBypass}
          >
            {pending ? <Spinner data-icon="inline-start" /> : null}
            {t(WEBUI.plugins.matcherBypassConfirmAction)}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );

  const restoreButton = (
    <Button
      variant="warning"
      size={mode === "compact" ? "icon-xs" : "sm"}
      aria-label={
        mode === "compact" ? t(WEBUI.plugins.matcherRestoreAction) : undefined
      }
      disabled={!ready || pending || !bypassed}
      onClick={handleRestore}
    >
      {pending ? (
        <Spinner data-icon="inline-start" />
      ) : (
        <RotateCcwIcon data-icon="inline-start" />
      )}
      {mode === "detail" ? t(WEBUI.plugins.matcherRestoreAction) : null}
    </Button>
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
            <span className="inline-flex">
              {bypassed ? restoreButton : bypassDialog}
            </span>
          </TooltipTrigger>
          <TooltipContent side="bottom">
            {unavailable
              ? control?.error
                ? `${statusLabel}: ${control.error}`
                : statusLabel
              : bypassed
                ? t(WEBUI.plugins.matcherRestoreAction)
                : t(WEBUI.plugins.matcherBypassAction)}
          </TooltipContent>
        </Tooltip>
      </div>
    );
  }

  return (
    <Card className={cn("mt-4", bypassed && "border-warning/40 bg-warning/5")}>
      <CardHeader className="flex flex-row items-start justify-between gap-3">
        <div className="flex min-w-0 flex-col gap-1">
          <CardTitle className="text-sm">
            {t(WEBUI.plugins.matcherRuntimeControl)}
          </CardTitle>
          <CardDescription>{statusDescription}</CardDescription>
        </div>
        <Badge variant={bypassed ? "warning" : ready ? "secondary" : "outline"}>
          {ready ? (
            bypassed ? (
              <ShieldOffIcon data-icon="inline-start" />
            ) : (
              <ShieldCheckIcon data-icon="inline-start" />
            )
          ) : loading ? (
            <Spinner data-icon="inline-start" />
          ) : null}
          {statusLabel}
        </Badge>
      </CardHeader>
      {bypassed || control?.error ? (
        <CardContent className="flex flex-col gap-2 text-xs text-muted-foreground">
          {bypassed ? (
            <>
              <p>
                {t(WEBUI.plugins.matcherBypassPositiveImpact, {
                  reference: positiveReference,
                })}
              </p>
              <p>
                {t(WEBUI.plugins.matcherBypassNegatedImpact, {
                  reference: negativeReference,
                })}
              </p>
              <p>{t(WEBUI.plugins.matcherBypassResetHint)}</p>
            </>
          ) : null}
          {control?.error ? (
            <p>
              <span className="text-destructive">
                {t(WEBUI.plugins.matcherControlFailed)}: {control.error}
              </span>
            </p>
          ) : null}
        </CardContent>
      ) : null}
      <CardFooter>{bypassed ? restoreButton : bypassDialog}</CardFooter>
    </Card>
  );
}
