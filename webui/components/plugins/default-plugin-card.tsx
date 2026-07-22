"use client";

import type { PluginCardComponentProps } from "./types";
import { PluginCardTemplate } from "./plugin-card-template";
import { getPluginCatalogItem } from "./catalog";
import { WEBUI } from "@/lib/i18n";
import { useI18n } from "@/lib/i18n/provider";
import { PluginCardItemGrid } from "./plugin-card-item-grid";

export function DefaultPluginCard(props: PluginCardComponentProps) {
  const { locale, t } = useI18n();
  const definition = getPluginCatalogItem(props.plugin.pluginKind, locale);
  const configFields = selectCardConfigFields(
    definition?.configSchema ?? [],
    props.plugin.config,
  );
  const configItems = configFields.map((field) => ({
    key: field.key,
    label: field.label,
    value: formatCardConfigValue(props.plugin.config[field.key], t),
  }));

  return (
    <PluginCardTemplate {...props}>
      {configItems.length > 0 ? (
        <PluginCardItemGrid items={configItems} />
      ) : null}
    </PluginCardTemplate>
  );
}

export function selectCardConfigFields<
  T extends { key: string; advanced?: boolean },
>(fields: T[], config: Record<string, unknown>): T[] {
  const visibleFields = fields.filter((field) => !field.advanced);
  const configuredAdvancedFields = fields.filter(
    (field) => field.advanced && hasCardConfigValue(config[field.key]),
  );

  const prioritizeConfigured = (candidates: T[]) =>
    candidates
      .map((field, index) => ({
        field,
        index,
        configured: hasCardConfigValue(config[field.key]),
      }))
      .sort(
        (left, right) =>
          Number(right.configured) - Number(left.configured) ||
          left.index - right.index,
      )
      .map(({ field }) => field);

  const primary = prioritizeConfigured(visibleFields).slice(0, 3);
  if (primary.length === 3) return primary;

  return [
    ...primary,
    ...prioritizeConfigured(configuredAdvancedFields).slice(
      0,
      3 - primary.length,
    ),
  ];
}

function hasCardConfigValue(value: unknown): boolean {
  if (value === undefined || value === null || value === "") return false;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "object") return Object.keys(value).length > 0;
  return true;
}

function formatCardConfigValue(
  value: unknown,
  t: ReturnType<typeof useI18n>["t"],
) {
  if (value === undefined || value === null || value === "") {
    return t(WEBUI.common.unconfigured);
  }
  if (typeof value === "boolean") {
    return value ? t(WEBUI.common.yes) : t(WEBUI.common.no);
  }
  if (typeof value === "number") return String(value);
  if (typeof value === "string") return value;
  if (Array.isArray(value))
    return value.length > 0
      ? t(WEBUI.common.itemCount, { count: value.length })
      : t(WEBUI.common.empty);
  if (typeof value === "object") {
    return Object.keys(value).length > 0
      ? t(WEBUI.common.configured)
      : t(WEBUI.common.empty);
  }
  return String(value);
}
