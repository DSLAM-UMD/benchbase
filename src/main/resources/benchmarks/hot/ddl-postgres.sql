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

CREATE TABLE usertable_1 PARTITION OF usertable FOR VALUES FROM (0) TO (3332);
CREATE TABLE usertable_2 PARTITION OF usertable FOR VALUES FROM (3333) TO (6665);
CREATE TABLE usertable_3 PARTITION OF usertable FOR VALUES FROM (6666) TO (9999);

UPDATE pg_class SET relregion = 1 WHERE relname = 'usertable_1';
UPDATE pg_class SET relregion = 1 WHERE relname = 'usertable_1_pkey';
UPDATE pg_class SET relregion = 2 WHERE relname = 'usertable_2';
UPDATE pg_class SET relregion = 2 WHERE relname = 'usertable_2_pkey';
UPDATE pg_class SET relregion = 3 WHERE relname = 'usertable_3';
UPDATE pg_class SET relregion = 3 WHERE relname = 'usertable_3_pkey';
