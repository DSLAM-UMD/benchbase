DROP TABLE IF EXISTS usertable;
CREATE TABLE usertable (
    ycsb_key int PRIMARY KEY,
    field1   text,
    field2   text,
    field3   text,
    field4   text,
    field5   text,
    field6   text,
    field7   text,
    field8   text,
    field9   text,
    field10  text
) PARTITION BY RANGE (ycsb_key);

CREATE TABLE usertable_1 PARTITION OF usertable FOR VALUES FROM (0) TO (10000);
CREATE TABLE usertable_2 PARTITION OF usertable FOR VALUES FROM (10000) TO (20000);
CREATE TABLE usertable_3 PARTITION OF usertable FOR VALUES FROM (20000) TO (30000);