DROP TABLE IF EXISTS usertable_1;
-- DROP TABLE IF EXISTS usertable_2;
-- DROP TABLE IF EXISTS usertable_3;

DROP TABLESPACE IF EXISTS region_1;
-- DROP TABLESPACE IF EXISTS region_2;
-- DROP TABLESPACE IF EXISTS region_3;

CREATE TABLESPACE region_1 WITH (
  replica_placement='{
    "num_replicas": 1,
    "placement_blocks": [{"cloud":"aws","region":"region-1","zone":"region-1","min_num_replicas":1}]
  }'
);

-- CREATE TABLESPACE region_2 WITH (
--    replica_placement='{
--     "num_replicas": 1,
--     "placement_blocks": [{"cloud":"aws","region":"region-2","zone":"region-2","min_num_replicas":1}]
--   }'
--  );

-- CREATE TABLESPACE region_3 WITH (
--   replica_placement='{
--     "num_replicas": 1,
--     "placement_blocks": [{"cloud":"aws","region":"region-3","zone":"region-3","min_num_replicas":1}]
--   }'
-- );

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
) TABLESPACE region_1;

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
-- ) TABLESPACE region_2;

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
-- ) TABLESPACE region_3;
