-- Drop TPCH dataset (SQL Server)
-- Drop in dependency-safe order (children before parents)

IF OBJECT_ID(N'tpch_sf1.lineitem', N'U') IS NOT NULL DROP TABLE tpch_sf1.lineitem;
IF OBJECT_ID(N'tpch_sf1.orders', N'U') IS NOT NULL DROP TABLE tpch_sf1.orders;
IF OBJECT_ID(N'tpch_sf1.partsupp', N'U') IS NOT NULL DROP TABLE tpch_sf1.partsupp;
IF OBJECT_ID(N'tpch_sf1.customer', N'U') IS NOT NULL DROP TABLE tpch_sf1.customer;
IF OBJECT_ID(N'tpch_sf1.supplier', N'U') IS NOT NULL DROP TABLE tpch_sf1.supplier;
IF OBJECT_ID(N'tpch_sf1.part', N'U') IS NOT NULL DROP TABLE tpch_sf1.part;
IF OBJECT_ID(N'tpch_sf1.nation', N'U') IS NOT NULL DROP TABLE tpch_sf1.nation;
IF OBJECT_ID(N'tpch_sf1.region', N'U') IS NOT NULL DROP TABLE tpch_sf1.region;

-- Drop schema when it no longer contains user objects
IF SCHEMA_ID(N'tpch') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID(N'tpch'))
BEGIN
    EXEC(N'DROP SCHEMA tpch');
END
