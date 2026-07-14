import { describe, expect, it } from "vitest";

import { matcherPluginDefinitions } from "@/lib/plugin-definitions/matcher";

import {
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

  it("initializes a period without time bounds to the default numeric range", () => {
    const formValues = createPluginConfigFormValues(fields, {
      periods: [{ weekdays: ["mon", "wed"] }],
    });
    const period = (formValues.periods as Record<string, unknown>[])[0];

    expect(isPluginConfigFormValid(fields, formValues)).toBe(true);
    expect(serializePluginConfigValues(fields, formValues)).toEqual({
      periods: [{ start: "09:00", end: "18:00", weekdays: [1, 3] }],
    });
    expect(period.start).toBe("09:00");
    expect(period.end).toBe("18:00");
  });

  it("accepts overnight ranges and rejects incomplete or equal ranges", () => {
    const valid = createPluginConfigFormValues(fields, {
      periods: [{ start: "22:00", end: "02:00" }],
    });
    const incomplete = createPluginConfigFormValues(fields, { periods: [{}] });
    (incomplete.periods as Record<string, unknown>[])[0].end = "";
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

  it("initializes an empty period with a valid default range", () => {
    const formValues = createPluginConfigFormValues(fields, { periods: [{}] });

    expect(isPluginConfigFormValid(fields, formValues)).toBe(true);
    expect(serializePluginConfigValues(fields, formValues)).toEqual({
      periods: [{ start: "09:00", end: "18:00" }],
    });
  });
});
