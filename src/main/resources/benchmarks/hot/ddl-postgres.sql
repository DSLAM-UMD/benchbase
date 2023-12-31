-- CREATE EXTENSION IF NOT EXISTS remotexact;

DROP TABLE IF EXISTS usertable_1;
CREATE TABLE usertable_1 (
    ycsb_key int primary key,
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
);
-- UPDATE pg_class SET relregion = 1 WHERE relname = 'usertable_1';
-- UPDATE pg_class SET relregion = 1 WHERE relname = 'usertable_1_pkey';

-- DROP TABLE IF EXISTS usertable_2;
-- CREATE TABLE usertable_2 (
--     ycsb_key int primary key,
--     field1   text,
--     field2   text,
--     field3   text,
--     field4   text,
--     field5   text,
--     field6   text,
--     field7   text,
--     field8   text,
--     field9   text,
--     field10  text
-- );
-- UPDATE pg_class SET relregion = 2 WHERE relname = 'usertable_2';
-- UPDATE pg_class SET relregion = 2 WHERE relname = 'usertable_2_pkey';

-- DROP TABLE IF EXISTS usertable_3;
-- CREATE TABLE usertable_3 (
--     ycsb_key int primary key,
--     field1   text,
--     field2   text,
--     field3   text,
--     field4   text,
--     field5   text,
--     field6   text,
--     field7   text,
--     field8   text,
--     field9   text,
--     field10  text
-- );
-- UPDATE pg_class SET relregion = 3 WHERE relname = 'usertable_3';
-- UPDATE pg_class SET relregion = 3 WHERE relname = 'usertable_3_pkey';
