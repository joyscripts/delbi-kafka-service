const logLevels = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3,
};

const currentLogLevel = process.env.LOG_LEVEL || "info";

const shouldLog = (level: keyof typeof logLevels): boolean => {
  return logLevels[level] <= logLevels[currentLogLevel as keyof typeof logLevels];
};

export const logger = {
  error: (...args: any[]) => {
    if (shouldLog("error")) {
      console.error("[ERROR]", ...args);
    }
  },
  warn: (...args: any[]) => {
    if (shouldLog("warn")) {
      console.warn("[WARN]", ...args);
    }
  },
  info: (...args: any[]) => {
    if (shouldLog("info")) {
      console.log("[INFO]", ...args);
    }
  },
  debug: (...args: any[]) => {
    if (shouldLog("debug")) {
      console.log("[DEBUG]", ...args);
    }
  },
};

