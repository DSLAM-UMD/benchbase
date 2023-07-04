DROP INDEX IF EXISTS usertable_us_ycsb_key_idx;
DROP INDEX IF EXISTS usertable_eu_ycsb_key_idx;
DROP INDEX IF EXISTS usertable_ap_ycsb_key_idx;
DROP TABLE IF EXISTS usertable_us;
DROP TABLE IF EXISTS usertable_eu;
DROP TABLE IF EXISTS usertable_ap;
DROP TABLE IF EXISTS usertable;
DROP TABLESPACE IF EXISTS us_east_1_tablespace;
DROP TABLESPACE IF EXISTS eu_west_1_tablespace;
DROP TABLESPACE IF EXISTS ap_northeast_1_tablespace;

CREATE TABLESPACE us_east_1_tablespace WITH (
  replica_placement='{
    "num_replicas": 1,
    "placement_blocks": [{"cloud":"aws","region":"us-east-1","zone":"us-east-1a","min_num_replicas":1}]
  }'
);

CREATE TABLESPACE eu_west_1_tablespace WITH (
   replica_placement='{
    "num_replicas": 1,
    "placement_blocks": [{"cloud":"aws","region":"eu-west-1","zone":"eu-west-1a","min_num_replicas":1}]
  }'
 );

CREATE TABLESPACE ap_northeast_1_tablespace WITH (
  replica_placement='{
    "num_replicas": 1,
    "placement_blocks": [{"cloud":"aws","region":"ap-northeast-1","zone":"ap-northeast-1a","min_num_replicas":1}]
  }'
);

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
    geo_partition VARCHAR
) PARTITION BY LIST (geo_partition);


CREATE TABLE usertable_us
    PARTITION OF usertable
      (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, geo_partition,
      PRIMARY KEY (ycsb_key HASH, geo_partition))
    FOR VALUES IN ('US') TABLESPACE us_east_1_tablespace;

CREATE INDEX ON usertable_us(ycsb_key) TABLESPACE us_east_1_tablespace;

CREATE TABLE usertable_eu
    PARTITION OF usertable
      (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, geo_partition,
      PRIMARY KEY (ycsb_key HASH, geo_partition))
    FOR VALUES IN ('EU') TABLESPACE eu_west_1_tablespace;

CREATE INDEX ON usertable_eu(ycsb_key) TABLESPACE eu_west_1_tablespace;

CREATE TABLE usertable_ap
    PARTITION OF usertable
      (ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, geo_partition,
      PRIMARY KEY (ycsb_key HASH, geo_partition))
    FOR VALUES IN ('AP') TABLESPACE ap_northeast_1_tablespace;

CREATE INDEX ON usertable_ap(ycsb_key) TABLESPACE ap_northeast_1_tablespace;
