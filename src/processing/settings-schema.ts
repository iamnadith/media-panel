export const PROCESSING_SETTINGS_DEFAULTS = {
  orchestratorEnabled: true,
  registrationEnabled: true,
  videoProcessingEnabled: true,
  registerBatchSize: 1,
  maxRegisterPasses: 1,
  staleProcessingMinutes: 2,
  staleRegistrationMinutes: 5,
  registrationHistoryDays: 14,
  processorPollIntervalMs: 5_000,
  processorIdleIntervalMs: 5_000,
  processorHeartbeatIntervalMs: 5_000,
  processorClaimLimit: 1,
} as const;

export type ProcessingSettings = {
  -readonly [K in keyof typeof PROCESSING_SETTINGS_DEFAULTS]:
  (typeof PROCESSING_SETTINGS_DEFAULTS)[K] extends boolean ? boolean : number
};

export const PROCESSING_NUMBER_LIMITS = {
  registerBatchSize: [1, 1],
  maxRegisterPasses: [1, 20],
  staleProcessingMinutes: [1, 1_440],
  staleRegistrationMinutes: [1, 1_440],
  registrationHistoryDays: [1, 365],
  processorPollIntervalMs: [1_000, 300_000],
  processorIdleIntervalMs: [1_000, 300_000],
  processorHeartbeatIntervalMs: [1_000, 60_000],
  processorClaimLimit: [1, 3],
} as const satisfies Partial<
  Record<keyof ProcessingSettings, readonly [number, number]>
>;

export const parseProcessingSettings = (
  values: Partial<Record<keyof ProcessingSettings, unknown>>,
): ProcessingSettings => {
  const settings = { ...PROCESSING_SETTINGS_DEFAULTS } as ProcessingSettings;
  for (const key of Object.keys(settings) as (keyof ProcessingSettings)[]) {
    const fallback = PROCESSING_SETTINGS_DEFAULTS[key];
    const value = values[key];
    if (typeof fallback === 'boolean') {
      settings[key] = (value === undefined
        ? fallback
        : value === true ||
          value === 'true' ||
          value === '1' ||
          value === 'on') as never;
      continue;
    }
    const parsed = Number(value);
    const limits = PROCESSING_NUMBER_LIMITS[
      key as keyof typeof PROCESSING_NUMBER_LIMITS
    ];
    settings[key] = (Number.isFinite(parsed) && limits
      ? Math.min(Math.max(Math.round(parsed), limits[0]), limits[1])
      : fallback) as never;
  }
  return settings;
};
