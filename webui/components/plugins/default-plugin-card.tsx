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
  const configFields = definition?.configSchema.slice(0, 3) ?? [];
  const configItems = configFields.map((field) => ({
    key: field.key,
    label: field.label,
    value: formatCardConfigValue(props.plugin.config[field.key], t),
  }));

  return (
    <PluginCardTemplate {...props}>
      <PluginCardItemGrid items={configItems} />
    </PluginCardTemplate>
  );
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
