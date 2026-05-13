SELECT COUNT(*) AS region_rows FROM tpch.region;
SELECT COUNT(*) AS nation_rows FROM tpch.nation;
SELECT COUNT(*) AS supplier_rows FROM tpch.supplier;
SELECT COUNT(*) AS customer_rows FROM tpch.customer;
SELECT COUNT(*) AS part_rows FROM tpch.part;
SELECT COUNT(*) AS partsupp_rows FROM tpch.partsupp;
SELECT COUNT(*) AS orders_rows FROM tpch.orders;
SELECT COUNT(*) AS lineitem_rows FROM tpch.lineitem;

SELECT path, num_rows, num_columns, table_size_bytes
FROM statistics_cache()
ORDER BY path;
