-- SQL Server Tuning Script
-- TPCH tuning:
-- 1) Clustered columnstore index on each TPCH table
-- 2) Secondary row-store index via NONCLUSTERED PRIMARY KEY constraints

DECLARE @TableName NVARCHAR(255);
DECLARE @SQL NVARCHAR(MAX);

-- Primary keys
IF OBJECT_ID(N'tpch.part', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'pk_part' AND type = 'PK')
    ALTER TABLE tpch.part ADD CONSTRAINT pk_part PRIMARY KEY NONCLUSTERED (p_partkey);

IF OBJECT_ID(N'tpch.supplier', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'pk_supplier' AND type = 'PK')
    ALTER TABLE tpch.supplier ADD CONSTRAINT pk_supplier PRIMARY KEY NONCLUSTERED (s_suppkey);

IF OBJECT_ID(N'tpch.partsupp', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'pk_partsupp' AND type = 'PK')
    ALTER TABLE tpch.partsupp ADD CONSTRAINT pk_partsupp PRIMARY KEY NONCLUSTERED (ps_partkey, ps_suppkey);

IF OBJECT_ID(N'tpch.customer', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'pk_customer' AND type = 'PK')
    ALTER TABLE tpch.customer ADD CONSTRAINT pk_customer PRIMARY KEY NONCLUSTERED (c_custkey);

IF OBJECT_ID(N'tpch.orders', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'pk_orders' AND type = 'PK')
    ALTER TABLE tpch.orders ADD CONSTRAINT pk_orders PRIMARY KEY NONCLUSTERED (o_orderkey);

IF OBJECT_ID(N'tpch.lineitem', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'pk_lineitem' AND type = 'PK')
    ALTER TABLE tpch.lineitem ADD CONSTRAINT pk_lineitem PRIMARY KEY NONCLUSTERED (l_orderkey, l_linenumber);

IF OBJECT_ID(N'tpch.nation', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'pk_nation' AND type = 'PK')
    ALTER TABLE tpch.nation ADD CONSTRAINT pk_nation PRIMARY KEY NONCLUSTERED (n_nationkey);

IF OBJECT_ID(N'tpch.region', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'pk_region' AND type = 'PK')
    ALTER TABLE tpch.region ADD CONSTRAINT pk_region PRIMARY KEY NONCLUSTERED (r_regionkey);

-- Foreign keys
IF OBJECT_ID(N'tpch.orders', N'U') IS NOT NULL
   AND OBJECT_ID(N'tpch.customer', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_orders_customer')
    ALTER TABLE tpch.orders ADD CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey) REFERENCES tpch.customer (c_custkey);

IF OBJECT_ID(N'tpch.partsupp', N'U') IS NOT NULL
   AND OBJECT_ID(N'tpch.part', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_partsupp_part')
    ALTER TABLE tpch.partsupp ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (ps_partkey) REFERENCES tpch.part (p_partkey);

IF OBJECT_ID(N'tpch.partsupp', N'U') IS NOT NULL
   AND OBJECT_ID(N'tpch.supplier', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_partsupp_supplier')
    ALTER TABLE tpch.partsupp ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (ps_suppkey) REFERENCES tpch.supplier (s_suppkey);

IF OBJECT_ID(N'tpch.lineitem', N'U') IS NOT NULL
   AND OBJECT_ID(N'tpch.orders', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_lineitem_orders')
    ALTER TABLE tpch.lineitem ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (l_orderkey) REFERENCES tpch.orders (o_orderkey);

IF OBJECT_ID(N'tpch.lineitem', N'U') IS NOT NULL
   AND OBJECT_ID(N'tpch.part', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_lineitem_part')
    ALTER TABLE tpch.lineitem ADD CONSTRAINT fk_lineitem_part FOREIGN KEY (l_partkey) REFERENCES tpch.part (p_partkey);

IF OBJECT_ID(N'tpch.lineitem', N'U') IS NOT NULL
   AND OBJECT_ID(N'tpch.supplier', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_lineitem_supplier')
    ALTER TABLE tpch.lineitem ADD CONSTRAINT fk_lineitem_supplier FOREIGN KEY (l_suppkey) REFERENCES tpch.supplier (s_suppkey);

IF OBJECT_ID(N'tpch.lineitem', N'U') IS NOT NULL
   AND OBJECT_ID(N'tpch.partsupp', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_lineitem_partsupp')
    ALTER TABLE tpch.lineitem ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (l_partkey, l_suppkey)
        REFERENCES tpch.partsupp (ps_partkey, ps_suppkey);

IF OBJECT_ID(N'tpch.supplier', N'U') IS NOT NULL
   AND OBJECT_ID(N'tpch.nation', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_supplier_nation')
    ALTER TABLE tpch.supplier ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (s_nationkey) REFERENCES tpch.nation (n_nationkey);

IF OBJECT_ID(N'tpch.customer', N'U') IS NOT NULL
   AND OBJECT_ID(N'tpch.nation', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_customer_nation')
    ALTER TABLE tpch.customer ADD CONSTRAINT fk_customer_nation FOREIGN KEY (c_nationkey) REFERENCES tpch.nation (n_nationkey);

IF OBJECT_ID(N'tpch.nation', N'U') IS NOT NULL
   AND OBJECT_ID(N'tpch.region', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'fk_nation_region')
    ALTER TABLE tpch.nation ADD CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey) REFERENCES tpch.region (r_regionkey);

-- Supporting indexes
IF OBJECT_ID(N'tpch.lineitem', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'tpch.lineitem') AND name = N'lineitem_orderkey_idx')
    CREATE INDEX lineitem_orderkey_idx ON tpch.lineitem (l_orderkey);

IF OBJECT_ID(N'tpch.lineitem', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'tpch.lineitem') AND name = N'ix_q17')
    CREATE INDEX ix_q17 ON tpch.lineitem (l_partkey);

DECLARE table_cursor CURSOR FOR
SELECT t.name
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'tpch'
  AND t.name IN ('part', 'supplier', 'partsupp', 'customer', 'orders', 'lineitem', 'nation', 'region')
  AND NOT EXISTS (
      SELECT 1 FROM sys.indexes i
      WHERE i.object_id = t.object_id
        AND i.type = 5 -- Clustered Columnstore
  );

OPEN table_cursor;

FETCH NEXT FROM table_cursor INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = 'CREATE CLUSTERED COLUMNSTORE INDEX cci_' + @TableName + ' ON [tpch].' + QUOTENAME(@TableName);

    PRINT 'Executing: ' + @SQL;
    EXEC sp_executesql @SQL;

    FETCH NEXT FROM table_cursor INTO @TableName;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

-- Refresh statistics for TPCH tables when stats are missing or older than latest schema modifications.
DECLARE @StatsTableName NVARCHAR(255);
DECLARE @StatsSchemaName NVARCHAR(255);
DECLARE @QualifiedName NVARCHAR(512);

DECLARE stats_cursor CURSOR FOR
SELECT t.name, s.name
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'tpch'
  AND t.name IN ('part', 'supplier', 'partsupp', 'customer', 'orders', 'lineitem', 'nation', 'region');

OPEN stats_cursor;
FETCH NEXT FROM stats_cursor INTO @StatsTableName, @StatsSchemaName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @QualifiedName = QUOTENAME(@StatsSchemaName) + '.' + QUOTENAME(@StatsTableName);

    IF EXISTS (
        SELECT 1
        FROM sys.stats st
        JOIN sys.objects o ON o.object_id = st.object_id
        WHERE st.object_id = OBJECT_ID(@QualifiedName)
          AND (
              STATS_DATE(st.object_id, st.stats_id) IS NULL
              OR STATS_DATE(st.object_id, st.stats_id) < o.modify_date
          )
    )
    BEGIN
        SET @SQL = 'UPDATE STATISTICS ' + @QualifiedName;
        PRINT 'Executing: ' + @SQL;
        EXEC sp_executesql @SQL;
    END

    FETCH NEXT FROM stats_cursor INTO @StatsTableName, @StatsSchemaName;
END

CLOSE stats_cursor;
DEALLOCATE stats_cursor;
