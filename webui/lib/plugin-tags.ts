const PLUGIN_TAG_PATTERN = /^[A-Za-z0-9_.-]+$/;
const RESERVED_PLUGIN_TAG_PREFIXES = ["qs.exec.", "qs.match.", "qs.cron."];

export function isValidPluginTag(tag: string): boolean {
  return PLUGIN_TAG_PATTERN.test(tag);
}

export function isReservedPluginTag(tag: string): boolean {
  return RESERVED_PLUGIN_TAG_PREFIXES.some((prefix) => tag.startsWith(prefix));
}
