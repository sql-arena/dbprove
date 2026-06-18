SELECT COUNT(*) AS region_rows FROM tpch_sf1.region;
SELECT COUNT(*) AS nation_rows FROM tpch_sf1.nation;
SELECT COUNT(*) AS supplier_rows FROM tpch_sf1.supplier;
SELECT COUNT(*) AS customer_rows FROM tpch_sf1.customer;
SELECT COUNT(*) AS part_rows FROM tpch_sf1.part;
SELECT COUNT(*) AS partsupp_rows FROM tpch_sf1.partsupp;
SELECT COUNT(*) AS orders_rows FROM tpch_sf1.orders;
SELECT COUNT(*) AS lineitem_rows FROM tpch_sf1.lineitem;

SELECT path, num_rows, num_columns, table_size_bytes
FROM statistics_cache()
ORDER BY path;
