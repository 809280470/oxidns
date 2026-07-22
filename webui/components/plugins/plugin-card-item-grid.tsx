import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export interface PluginCardItem {
  key: string;
  label: string;
  value: string;
}

export function pluginCardGridColumnClass(itemCount: number): string {
  return itemCount < 4 ? "grid-cols-1" : "grid-cols-2";
}

export function PluginCardItemSurface({ children }: { children: ReactNode }) {
  return (
    <div className="h-[5.25rem] overflow-hidden rounded-md bg-muted/25 px-2.5 py-2">
      {children}
    </div>
  );
}

export function PluginCardItemGrid({ items }: { items: PluginCardItem[] }) {
  return (
    <div
      className={cn(
        "grid gap-x-3 gap-y-1",
        pluginCardGridColumnClass(items.length),
      )}
    >
      {items.map((item) => (
        <div
          key={item.key}
          className="flex min-w-0 items-baseline justify-between gap-2 text-xs leading-5"
        >
          <span
            className="min-w-0 truncate text-muted-foreground"
            title={item.label}
          >
            {item.label}
          </span>
          <span
            className="min-w-0 truncate text-right font-mono font-medium text-foreground tabular-nums"
            title={item.value}
          >
            {item.value}
          </span>
        </div>
      ))}
    </div>
  );
}
