import { pino } from 'pino';
import { config } from './config.js';

export const log = pino({
  level: config.logLevel,
  base: { instance: config.instanceId },
  transport: process.env.NODE_ENV === 'development' ? { target: 'pino-pretty' } : undefined,
});
