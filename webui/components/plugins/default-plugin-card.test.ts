import { describe, expect, it } from "vitest";

import { selectCardConfigFields } from "./default-plugin-card";

const fields = [
  { key: "first" },
  { key: "second" },
  { key: "third" },
  { key: "fourth" },
  { key: "advanced", advanced: true },
];

describe("default plugin card config summary", () => {
  it("prioritizes configured primary fields while preserving schema order", () => {
    expect(
      selectCardConfigFields(fields, { third: "configured" }).map(
        (field) => field.key,
      ),
    ).toEqual(["third", "first", "second"]);
  });

  it("does not let an advanced field displace primary configuration", () => {
    expect(
      selectCardConfigFields(fields, { advanced: true }).map(
        (field) => field.key,
      ),
    ).toEqual(["first", "second", "third"]);
  });

  it("uses configured advanced fields when no primary fields exist", () => {
    expect(
      selectCardConfigFields(
        [
          { key: "unused", advanced: true },
          { key: "enabled", advanced: true },
        ],
        { enabled: false },
      ).map((field) => field.key),
    ).toEqual(["enabled"]);
  });
});
