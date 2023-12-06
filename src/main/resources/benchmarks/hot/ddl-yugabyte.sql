DROP TABLE IF EXISTS usertable_1;
DROP TABLE IF EXISTS usertable_2;
DROP TABLE IF EXISTS usertable_3;

DROP TABLESPACE IF EXISTS us_east_1_tablespace;
DROP TABLESPACE IF EXISTS us_east_2_tablespace;
DROP TABLESPACE IF EXISTS us_west_1_tablespace;

CREATE TABLESPACE us_east_1_tablespace WITH (
  replica_placement='{
    "num_replicas": 1,
    "placement_blocks": [{"cloud":"aws","region":"us-east-1","zone":"us-east-1","min_num_replicas":1}]
  }'
);

CREATE TABLESPACE us_east_2_tablespace WITH (
   replica_placement='{
    "num_replicas": 1,
    "placement_blocks": [{"cloud":"aws","region":"us-east-2","zone":"us-east-2","min_num_replicas":1}]
  }'
 );

CREATE TABLESPACE us_west_1_tablespace WITH (
  replica_placement='{
    "num_replicas": 1,
    "placement_blocks": [{"cloud":"aws","region":"us-west-1","zone":"us-west-1","min_num_replicas":1}]
  }'
);

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
) TABLESPACE us_east_1_tablespace;

CREATE TABLE usertable_2 (
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
) TABLESPACE us_east_2_tablespace;

CREATE TABLE usertable_3 (
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
) TABLESPACE us_west_1_tablespace;
