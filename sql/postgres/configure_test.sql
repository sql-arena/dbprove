/* Configure a test system to not burn tons of memory */
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET work_mem = '4MB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET effective_cache_size = '512MB';
ALTER SYSTEM SET max_connections = '20';

/* NOTE: PostgreSQL service must be restarted after applying this */
