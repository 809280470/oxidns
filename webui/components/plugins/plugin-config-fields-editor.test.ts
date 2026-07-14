import { describe, expect, it } from "vitest";

import { matcherPluginDefinitions } from "@/lib/plugin-definitions/matcher";
import { executorPluginDefinitions } from "@/lib/plugin-definitions/executor";

import {
  createDefaultPluginConfigValues,
  createPluginConfigFormValues,
  isPluginConfigFormValid,
  serializePluginConfigValues,
} from "./plugin-config-fields-editor";

const timeDefinition = matcherPluginDefinitions.find(
  (definition) => definition.kind === "time",
);

if (!timeDefinition) {
  throw new Error("time matcher definition must exist");
}

const fields = timeDefinition.configSchema;
const periodsField = fields.find((field) => field.key === "periods");

if (!periodsField?.item || periodsField.item.type !== "object") {
  throw new Error("time matcher periods must use an object schema");
}
const periodFields = periodsField.item.fields;

const nftSetDefinition = executorPluginDefinitions.find(
  (definition) => definition.kind === "nftset",
);

if (!nftSetDefinition) {
  throw new Error("nftset executor definition must exist");
}

describe("time matcher config form", () => {
  it("normalizes legacy weekday aliases to ISO numbers while preserving monthdays", () => {
    const config = {
      timezone: "UTC",
      periods: [
        {
          start: "22:00",
          end: "02:00",
          weekdays: ["fri", "mon"],
          monthdays: [15, 1],
        },
      ],
    };

    const formValues = createPluginConfigFormValues(fields, config);
    const serialized = serializePluginConfigValues(fields, formValues);

    expect(serialized).toEqual({
      ...config,
      periods: [
        {
          ...config.periods[0],
          weekdays: [5, 1],
        },
      ],
    });
    const period = (serialized.periods as Record<string, unknown>[])[0];
    expect(period.weekdays).toEqual([5, 1]);
    expect(period.monthdays).toEqual([15, 1]);
    expect(
      (period.monthdays as unknown[]).every((day) => typeof day === "number"),
    ).toBe(true);
  });

  it("normalizes weekday aliases case-insensitively", () => {
    const formValues = createPluginConfigFormValues(fields, {
      periods: [{ weekdays: ["MON", "Fri"] }],
    });

    expect(serializePluginConfigValues(fields, formValues)).toEqual({
      periods: [{ weekdays: [1, 5] }],
    });
  });

  it("preserves omitted time bounds for existing all-day rules", () => {
    const formValues = createPluginConfigFormValues(fields, {
      periods: [{ weekdays: ["mon", "wed"] }],
    });
    const period = (formValues.periods as Record<string, unknown>[])[0];

    expect(isPluginConfigFormValid(fields, formValues)).toBe(true);
    expect(serializePluginConfigValues(fields, formValues)).toEqual({
      periods: [{ weekdays: [1, 3] }],
    });
    expect(period.start).toBeUndefined();
    expect(period.end).toBeUndefined();
  });

  it("accepts overnight ranges and rejects incomplete or equal ranges", () => {
    const valid = createPluginConfigFormValues(fields, {
      periods: [{ start: "22:00", end: "02:00" }],
    });
    const incomplete = createPluginConfigFormValues(fields, {
      periods: [{ start: "09:00" }],
    });
    const equal = createPluginConfigFormValues(fields, {
      periods: [{ start: "09:00", end: "09:00" }],
    });
    const malformed = createPluginConfigFormValues(fields, {
      periods: [{ start: "9:00", end: "18:00" }],
    });

    expect(isPluginConfigFormValid(fields, valid)).toBe(true);
    expect(isPluginConfigFormValid(fields, incomplete)).toBe(false);
    expect(isPluginConfigFormValid(fields, equal)).toBe(false);
    expect(isPluginConfigFormValid(fields, malformed)).toBe(false);
  });

  it("uses the default range only for newly added periods", () => {
    const newPeriod = createDefaultPluginConfigValues(periodFields);
    const newValues = { periods: [newPeriod] };

    expect(isPluginConfigFormValid(fields, newValues)).toBe(true);
    expect(serializePluginConfigValues(fields, newValues)).toEqual({
      periods: [{ start: "09:00", end: "18:00" }],
    });
  });

  it("rejects an existing empty period without time or calendar conditions", () => {
    const formValues = createPluginConfigFormValues(fields, { periods: [{}] });

    expect(isPluginConfigFormValid(fields, formValues)).toBe(false);
    expect(serializePluginConfigValues(fields, formValues)).toEqual({
      periods: [],
    });
  });
});

describe("optional object config fields", () => {
  it("does not validate required children when an optional object is omitted", () => {
    const formValues = createPluginConfigFormValues(
      nftSetDefinition.configSchema,
      {
        table_family4: "ip",
        table_name4: "mangle",
        set_name4: "dns_v4",
      },
    );

    expect(
      isPluginConfigFormValid(nftSetDefinition.configSchema, formValues),
    ).toBe(true);
    expect(
      serializePluginConfigValues(nftSetDefinition.configSchema, formValues),
    ).toEqual({
      table_family4: "ip",
      table_name4: "mangle",
      set_name4: "dns_v4",
    });
  });
});
