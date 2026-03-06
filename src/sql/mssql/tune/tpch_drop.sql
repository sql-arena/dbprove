-- Drop TPCH dataset (SQL Server)
-- Drop in dependency-safe order (children before parents)

IF OBJECT_ID(N'tpch.lineitem', N'U') IS NOT NULL DROP TABLE tpch.lineitem;
IF OBJECT_ID(N'tpch.orders', N'U') IS NOT NULL DROP TABLE tpch.orders;
IF OBJECT_ID(N'tpch.partsupp', N'U') IS NOT NULL DROP TABLE tpch.partsupp;
IF OBJECT_ID(N'tpch.customer', N'U') IS NOT NULL DROP TABLE tpch.customer;
IF OBJECT_ID(N'tpch.supplier', N'U') IS NOT NULL DROP TABLE tpch.supplier;
IF OBJECT_ID(N'tpch.part', N'U') IS NOT NULL DROP TABLE tpch.part;
IF OBJECT_ID(N'tpch.nation', N'U') IS NOT NULL DROP TABLE tpch.nation;
IF OBJECT_ID(N'tpch.region', N'U') IS NOT NULL DROP TABLE tpch.region;

-- Drop schema when it no longer contains user objects
IF SCHEMA_ID(N'tpch') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID(N'tpch'))
BEGIN
    EXEC(N'DROP SCHEMA tpch');
END
