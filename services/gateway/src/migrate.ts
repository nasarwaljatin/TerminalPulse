import { migrate, pool } from './db.js';

await migrate();
await pool.end();
console.log('migrations up to date');
