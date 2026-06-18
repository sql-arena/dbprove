CREATE TABLE scale.orders_scale_03
(
    join_key           BIGINT NOT NULL,
    o_orderkey         BIGINT NOT NULL,
    orders_replica_id  BIGINT NOT NULL,
    o_custkey          BIGINT NOT NULL,
    o_orderstatus      TEXT NOT NULL,
    o_totalprice       DECIMAL(15, 2) NOT NULL,
    o_orderdate        DATE NOT NULL,
    o_orderpriority    TEXT NOT NULL,
    o_clerk            TEXT NOT NULL,
    o_shippriority     BIGINT NOT NULL,
    o_comment          TEXT NOT NULL
);
