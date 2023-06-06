DROP TABLE IF EXISTS usertable;
CREATE TABLE usertable (
    ycsb_key int,
    field1   text,
    field2   text,
    field3   text,
    field4   text,
    field5   text,
    field6   text,
    field7   text,
    field8   text,
    field9   text,
    field10  text,
    shard    int
);

CREATE INDEX ON usertable (ycsb_key);
SELECT create_distributed_table('usertable', 'shard');
