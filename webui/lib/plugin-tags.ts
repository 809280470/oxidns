const PLUGIN_TAG_PATTERN = /^[A-Za-z0-9_.-]+$/;

export function isValidPluginTag(tag: string): boolean {
  return PLUGIN_TAG_PATTERN.test(tag);
}
