import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export type Range = [number, number];
export type ProfileName = 'healthy' | 'network_degraded' | 'card_reader_fault' | 'battery_failing' | 'overheating';
export type ProfileSpec = {
  txnPerMin: Range;
  declineP: Range;
  errorP: Range;
  latencyMs: Range;
  hbDropP: Range;
  battery: Range;
  signalDbm: Range;
  temperatureC: Range;
  rebootsPerHour: Range;
  queueDepth: Range;
};
export type ProfilesFile = {
  heartbeatIntervalS: number;
  featureWindowS: number;
  faultFraction: number;
  faultWeights: Record<Exclude<ProfileName, 'healthy'>, number>;
  profiles: Record<ProfileName, ProfileSpec>;
  cities: { name: string; lat: number; lng: number }[];
  models: string[];
  firmware: string[];
};

/** shared/fault-profiles.json is the single source of truth, also read by the Python ML generator. */
export function loadProfiles(): ProfilesFile {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const candidates = [
    process.env.PROFILES_PATH,
    path.resolve(here, '../../../shared/fault-profiles.json'),
    path.resolve(process.cwd(), 'shared/fault-profiles.json'),
    path.resolve(process.cwd(), '../../shared/fault-profiles.json'),
  ].filter(Boolean) as string[];
  for (const p of candidates) {
    if (fs.existsSync(p)) return JSON.parse(fs.readFileSync(p, 'utf8'));
  }
  throw new Error(`fault-profiles.json not found (looked in: ${candidates.join(', ')})`);
}
