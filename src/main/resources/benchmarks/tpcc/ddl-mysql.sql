SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS history_1 CASCADE;
DROP TABLE IF EXISTS new_order_1 CASCADE;
DROP TABLE IF EXISTS order_line_1 CASCADE;
DROP TABLE IF EXISTS oorder_1 CASCADE;
DROP TABLE IF EXISTS customer_1 CASCADE;
DROP TABLE IF EXISTS district_1 CASCADE;
DROP TABLE IF EXISTS stock_1 CASCADE;
DROP TABLE IF EXISTS warehouse_1 CASCADE;

DROP TABLE IF EXISTS history_2 CASCADE;
DROP TABLE IF EXISTS new_order_2 CASCADE;
DROP TABLE IF EXISTS order_line_2 CASCADE;
DROP TABLE IF EXISTS oorder_2 CASCADE;
DROP TABLE IF EXISTS customer_2 CASCADE;
DROP TABLE IF EXISTS district_2 CASCADE;
DROP TABLE IF EXISTS stock_2 CASCADE;
DROP TABLE IF EXISTS warehouse_2 CASCADE;

DROP TABLE IF EXISTS history_3 CASCADE;
DROP TABLE IF EXISTS new_order_3 CASCADE;
DROP TABLE IF EXISTS order_line_3 CASCADE;
DROP TABLE IF EXISTS oorder_3 CASCADE;
DROP TABLE IF EXISTS customer_3 CASCADE;
DROP TABLE IF EXISTS district_3 CASCADE;
DROP TABLE IF EXISTS stock_3 CASCADE;
DROP TABLE IF EXISTS warehouse_3 CASCADE;

DROP TABLE IF EXISTS item CASCADE;

----------------------------------------------------------------------------------------------------------------

CREATE TABLE item (
    i_id    int           NOT NULL,
    i_name  varchar(24)   NOT NULL,
    i_price decimal(5, 2) NOT NULL,
    i_data  varchar(50)   NOT NULL,
    i_im_id int           NOT NULL,
    PRIMARY KEY (i_id)
);

----------------------------------------------------------------------------------------------------------------

CREATE TABLE warehouse_1 (
    w_id       int            NOT NULL,
    w_ytd      decimal(12, 2) NOT NULL,
    w_tax      decimal(4, 4)  NOT NULL,
    w_name     varchar(10)    NOT NULL,
    w_street_1 varchar(20)    NOT NULL,
    w_street_2 varchar(20)    NOT NULL,
    w_city     varchar(20)    NOT NULL,
    w_state    char(2)        NOT NULL,
    w_zip      char(9)        NOT NULL,
    PRIMARY KEY (w_id)
);

CREATE TABLE stock_1 (
    s_w_id       int           NOT NULL,
    s_i_id       int           NOT NULL,
    s_quantity   int           NOT NULL,
    s_ytd        decimal(8, 2) NOT NULL,
    s_order_cnt  int           NOT NULL,
    s_remote_cnt int           NOT NULL,
    s_data       varchar(50)   NOT NULL,
    s_dist_01    char(24)      NOT NULL,
    s_dist_02    char(24)      NOT NULL,
    s_dist_03    char(24)      NOT NULL,
    s_dist_04    char(24)      NOT NULL,
    s_dist_05    char(24)      NOT NULL,
    s_dist_06    char(24)      NOT NULL,
    s_dist_07    char(24)      NOT NULL,
    s_dist_08    char(24)      NOT NULL,
    s_dist_09    char(24)      NOT NULL,
    s_dist_10    char(24)      NOT NULL,
    FOREIGN KEY (s_w_id) REFERENCES warehouse_1 (w_id) ON DELETE CASCADE,
    FOREIGN KEY (s_i_id) REFERENCES item (i_id) ON DELETE CASCADE,
    PRIMARY KEY (s_w_id, s_i_id)
);

CREATE TABLE district_1 (
    d_w_id      int            NOT NULL,
    d_id        int            NOT NULL,
    d_ytd       decimal(12, 2) NOT NULL,
    d_tax       decimal(4, 4)  NOT NULL,
    d_next_o_id int            NOT NULL,
    d_name      varchar(10)    NOT NULL,
    d_street_1  varchar(20)    NOT NULL,
    d_street_2  varchar(20)    NOT NULL,
    d_city      varchar(20)    NOT NULL,
    d_state     char(2)        NOT NULL,
    d_zip       char(9)        NOT NULL,
    FOREIGN KEY (d_w_id) REFERENCES warehouse_1 (w_id) ON DELETE CASCADE,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE customer_1 (
    c_w_id         int            NOT NULL,
    c_d_id         int            NOT NULL,
    c_id           int            NOT NULL,
    c_discount     decimal(4, 4)  NOT NULL,
    c_credit       char(2)        NOT NULL,
    c_last         varchar(16)    NOT NULL,
    c_first        varchar(16)    NOT NULL,
    c_credit_lim   decimal(12, 2) NOT NULL,
    c_balance      decimal(12, 2) NOT NULL,
    c_ytd_payment  float          NOT NULL,
    c_payment_cnt  int            NOT NULL,
    c_delivery_cnt int            NOT NULL,
    c_street_1     varchar(20)    NOT NULL,
    c_street_2     varchar(20)    NOT NULL,
    c_city         varchar(20)    NOT NULL,
    c_state        char(2)        NOT NULL,
    c_zip          char(9)        NOT NULL,
    c_phone        char(16)       NOT NULL,
    c_since        timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    c_middle       char(2)        NOT NULL,
    c_data         varchar(500)   NOT NULL,
    FOREIGN KEY (c_w_id, c_d_id) REFERENCES district_1 (d_w_id, d_id) ON DELETE CASCADE,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE history_1 (
    h_c_id   int           NOT NULL,
    h_c_d_id int           NOT NULL,
    h_c_w_id int           NOT NULL,
    h_d_id   int           NOT NULL,
    h_w_id   int           NOT NULL,
    h_date   timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    h_amount decimal(6, 2) NOT NULL,
    h_data   varchar(24)   NOT NULL,
    FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id) REFERENCES customer_1 (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
    FOREIGN KEY (h_w_id, h_d_id) REFERENCES district_1 (d_w_id, d_id) ON DELETE CASCADE
);

CREATE TABLE oorder_1 (
    o_w_id       int       NOT NULL,
    o_d_id       int       NOT NULL,
    o_id         int       NOT NULL,
    o_c_id       int       NOT NULL,
    o_carrier_id int                DEFAULT NULL,
    o_ol_cnt     int       NOT NULL,
    o_all_local  int       NOT NULL,
    o_entry_d    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (o_w_id, o_d_id, o_id),
    FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer_1 (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
    UNIQUE (o_w_id, o_d_id, o_c_id, o_id)
);

CREATE TABLE new_order_1 (
    no_w_id int NOT NULL,
    no_d_id int NOT NULL,
    no_o_id int NOT NULL,
    FOREIGN KEY (no_w_id, no_d_id, no_o_id) REFERENCES oorder_1 (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE order_line_1 (
    ol_w_id        int           NOT NULL,
    ol_d_id        int           NOT NULL,
    ol_o_id        int           NOT NULL,
    ol_number      int           NOT NULL,
    ol_i_id        int           NOT NULL,
    ol_delivery_d  timestamp     NULL DEFAULT NULL,
    ol_amount      decimal(6, 2) NOT NULL,
    ol_supply_w_id int           NOT NULL,
    ol_quantity    decimal(6,2)  NOT NULL,
    ol_dist_info   char(24)      NOT NULL,
    FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES oorder_1 (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
    FOREIGN KEY (ol_supply_w_id, ol_i_id) REFERENCES stock_1 (s_w_id, s_i_id) ON DELETE CASCADE,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE INDEX idx_customer_name_1 ON customer_1 (c_w_id, c_d_id, c_last, c_first);

----------------------------------------------------------------------------------------------------------------

CREATE TABLE warehouse_2 (
    w_id       int            NOT NULL,
    w_ytd      decimal(12, 2) NOT NULL,
    w_tax      decimal(4, 4)  NOT NULL,
    w_name     varchar(10)    NOT NULL,
    w_street_1 varchar(20)    NOT NULL,
    w_street_2 varchar(20)    NOT NULL,
    w_city     varchar(20)    NOT NULL,
    w_state    char(2)        NOT NULL,
    w_zip      char(9)        NOT NULL,
    PRIMARY KEY (w_id)
);

CREATE TABLE stock_2 (
    s_w_id       int           NOT NULL,
    s_i_id       int           NOT NULL,
    s_quantity   int           NOT NULL,
    s_ytd        decimal(8, 2) NOT NULL,
    s_order_cnt  int           NOT NULL,
    s_remote_cnt int           NOT NULL,
    s_data       varchar(50)   NOT NULL,
    s_dist_01    char(24)      NOT NULL,
    s_dist_02    char(24)      NOT NULL,
    s_dist_03    char(24)      NOT NULL,
    s_dist_04    char(24)      NOT NULL,
    s_dist_05    char(24)      NOT NULL,
    s_dist_06    char(24)      NOT NULL,
    s_dist_07    char(24)      NOT NULL,
    s_dist_08    char(24)      NOT NULL,
    s_dist_09    char(24)      NOT NULL,
    s_dist_10    char(24)      NOT NULL,
    FOREIGN KEY (s_w_id) REFERENCES warehouse_2 (w_id) ON DELETE CASCADE,
    FOREIGN KEY (s_i_id) REFERENCES item (i_id) ON DELETE CASCADE,
    PRIMARY KEY (s_w_id, s_i_id)
);

CREATE TABLE district_2 (
    d_w_id      int            NOT NULL,
    d_id        int            NOT NULL,
    d_ytd       decimal(12, 2) NOT NULL,
    d_tax       decimal(4, 4)  NOT NULL,
    d_next_o_id int            NOT NULL,
    d_name      varchar(10)    NOT NULL,
    d_street_1  varchar(20)    NOT NULL,
    d_street_2  varchar(20)    NOT NULL,
    d_city      varchar(20)    NOT NULL,
    d_state     char(2)        NOT NULL,
    d_zip       char(9)        NOT NULL,
    FOREIGN KEY (d_w_id) REFERENCES warehouse_2 (w_id) ON DELETE CASCADE,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE customer_2 (
    c_w_id         int            NOT NULL,
    c_d_id         int            NOT NULL,
    c_id           int            NOT NULL,
    c_discount     decimal(4, 4)  NOT NULL,
    c_credit       char(2)        NOT NULL,
    c_last         varchar(16)    NOT NULL,
    c_first        varchar(16)    NOT NULL,
    c_credit_lim   decimal(12, 2) NOT NULL,
    c_balance      decimal(12, 2) NOT NULL,
    c_ytd_payment  float          NOT NULL,
    c_payment_cnt  int            NOT NULL,
    c_delivery_cnt int            NOT NULL,
    c_street_1     varchar(20)    NOT NULL,
    c_street_2     varchar(20)    NOT NULL,
    c_city         varchar(20)    NOT NULL,
    c_state        char(2)        NOT NULL,
    c_zip          char(9)        NOT NULL,
    c_phone        char(16)       NOT NULL,
    c_since        timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    c_middle       char(2)        NOT NULL,
    c_data         varchar(500)   NOT NULL,
    FOREIGN KEY (c_w_id, c_d_id) REFERENCES district_2 (d_w_id, d_id) ON DELETE CASCADE,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE history_2 (
    h_c_id   int           NOT NULL,
    h_c_d_id int           NOT NULL,
    h_c_w_id int           NOT NULL,
    h_d_id   int           NOT NULL,
    h_w_id   int           NOT NULL,
    h_date   timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    h_amount decimal(6, 2) NOT NULL,
    h_data   varchar(24)   NOT NULL,
    FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id) REFERENCES customer_2 (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
    FOREIGN KEY (h_w_id, h_d_id) REFERENCES district_2 (d_w_id, d_id) ON DELETE CASCADE
);

CREATE TABLE oorder_2 (
    o_w_id       int       NOT NULL,
    o_d_id       int       NOT NULL,
    o_id         int       NOT NULL,
    o_c_id       int       NOT NULL,
    o_carrier_id int                DEFAULT NULL,
    o_ol_cnt     int       NOT NULL,
    o_all_local  int       NOT NULL,
    o_entry_d    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (o_w_id, o_d_id, o_id),
    FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer_2 (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
    UNIQUE (o_w_id, o_d_id, o_c_id, o_id)
);

CREATE TABLE new_order_2 (
    no_w_id int NOT NULL,
    no_d_id int NOT NULL,
    no_o_id int NOT NULL,
    FOREIGN KEY (no_w_id, no_d_id, no_o_id) REFERENCES oorder_2 (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE order_line_2 (
    ol_w_id        int           NOT NULL,
    ol_d_id        int           NOT NULL,
    ol_o_id        int           NOT NULL,
    ol_number      int           NOT NULL,
    ol_i_id        int           NOT NULL,
    ol_delivery_d  timestamp     NULL DEFAULT NULL,
    ol_amount      decimal(6, 2) NOT NULL,
    ol_supply_w_id int           NOT NULL,
    ol_quantity    decimal(6,2)  NOT NULL,
    ol_dist_info   char(24)      NOT NULL,
    FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES oorder_2 (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
    FOREIGN KEY (ol_supply_w_id, ol_i_id) REFERENCES stock_2 (s_w_id, s_i_id) ON DELETE CASCADE,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE INDEX idx_customer_name_2 ON customer_2 (c_w_id, c_d_id, c_last, c_first);

----------------------------------------------------------------------------------------------------------------

CREATE TABLE warehouse_3 (
    w_id       int            NOT NULL,
    w_ytd      decimal(12, 2) NOT NULL,
    w_tax      decimal(4, 4)  NOT NULL,
    w_name     varchar(10)    NOT NULL,
    w_street_1 varchar(20)    NOT NULL,
    w_street_2 varchar(20)    NOT NULL,
    w_city     varchar(20)    NOT NULL,
    w_state    char(2)        NOT NULL,
    w_zip      char(9)        NOT NULL,
    PRIMARY KEY (w_id)
);

CREATE TABLE stock_3 (
    s_w_id       int           NOT NULL,
    s_i_id       int           NOT NULL,
    s_quantity   int           NOT NULL,
    s_ytd        decimal(8, 2) NOT NULL,
    s_order_cnt  int           NOT NULL,
    s_remote_cnt int           NOT NULL,
    s_data       varchar(50)   NOT NULL,
    s_dist_01    char(24)      NOT NULL,
    s_dist_02    char(24)      NOT NULL,
    s_dist_03    char(24)      NOT NULL,
    s_dist_04    char(24)      NOT NULL,
    s_dist_05    char(24)      NOT NULL,
    s_dist_06    char(24)      NOT NULL,
    s_dist_07    char(24)      NOT NULL,
    s_dist_08    char(24)      NOT NULL,
    s_dist_09    char(24)      NOT NULL,
    s_dist_10    char(24)      NOT NULL,
    FOREIGN KEY (s_w_id) REFERENCES warehouse_3 (w_id) ON DELETE CASCADE,
    FOREIGN KEY (s_i_id) REFERENCES item (i_id) ON DELETE CASCADE,
    PRIMARY KEY (s_w_id, s_i_id)
);

CREATE TABLE district_3 (
    d_w_id      int            NOT NULL,
    d_id        int            NOT NULL,
    d_ytd       decimal(12, 2) NOT NULL,
    d_tax       decimal(4, 4)  NOT NULL,
    d_next_o_id int            NOT NULL,
    d_name      varchar(10)    NOT NULL,
    d_street_1  varchar(20)    NOT NULL,
    d_street_2  varchar(20)    NOT NULL,
    d_city      varchar(20)    NOT NULL,
    d_state     char(2)        NOT NULL,
    d_zip       char(9)        NOT NULL,
    FOREIGN KEY (d_w_id) REFERENCES warehouse_3 (w_id) ON DELETE CASCADE,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE customer_3 (
    c_w_id         int            NOT NULL,
    c_d_id         int            NOT NULL,
    c_id           int            NOT NULL,
    c_discount     decimal(4, 4)  NOT NULL,
    c_credit       char(2)        NOT NULL,
    c_last         varchar(16)    NOT NULL,
    c_first        varchar(16)    NOT NULL,
    c_credit_lim   decimal(12, 2) NOT NULL,
    c_balance      decimal(12, 2) NOT NULL,
    c_ytd_payment  float          NOT NULL,
    c_payment_cnt  int            NOT NULL,
    c_delivery_cnt int            NOT NULL,
    c_street_1     varchar(20)    NOT NULL,
    c_street_2     varchar(20)    NOT NULL,
    c_city         varchar(20)    NOT NULL,
    c_state        char(2)        NOT NULL,
    c_zip          char(9)        NOT NULL,
    c_phone        char(16)       NOT NULL,
    c_since        timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    c_middle       char(2)        NOT NULL,
    c_data         varchar(500)   NOT NULL,
    FOREIGN KEY (c_w_id, c_d_id) REFERENCES district_3 (d_w_id, d_id) ON DELETE CASCADE,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE history_3 (
    h_c_id   int           NOT NULL,
    h_c_d_id int           NOT NULL,
    h_c_w_id int           NOT NULL,
    h_d_id   int           NOT NULL,
    h_w_id   int           NOT NULL,
    h_date   timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    h_amount decimal(6, 2) NOT NULL,
    h_data   varchar(24)   NOT NULL,
    FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id) REFERENCES customer_3 (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
    FOREIGN KEY (h_w_id, h_d_id) REFERENCES district_3 (d_w_id, d_id) ON DELETE CASCADE
);

CREATE TABLE oorder_3 (
    o_w_id       int       NOT NULL,
    o_d_id       int       NOT NULL,
    o_id         int       NOT NULL,
    o_c_id       int       NOT NULL,
    o_carrier_id int                DEFAULT NULL,
    o_ol_cnt     int       NOT NULL,
    o_all_local  int       NOT NULL,
    o_entry_d    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (o_w_id, o_d_id, o_id),
    FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer_3 (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
    UNIQUE (o_w_id, o_d_id, o_c_id, o_id)
);

CREATE TABLE new_order_3 (
    no_w_id int NOT NULL,
    no_d_id int NOT NULL,
    no_o_id int NOT NULL,
    FOREIGN KEY (no_w_id, no_d_id, no_o_id) REFERENCES oorder_3 (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE order_line_3 (
    ol_w_id        int           NOT NULL,
    ol_d_id        int           NOT NULL,
    ol_o_id        int           NOT NULL,
    ol_number      int           NOT NULL,
    ol_i_id        int           NOT NULL,
    ol_delivery_d  timestamp     NULL DEFAULT NULL,
    ol_amount      decimal(6, 2) NOT NULL,
    ol_supply_w_id int           NOT NULL,
    ol_quantity    decimal(6,2)  NOT NULL,
    ol_dist_info   char(24)      NOT NULL,
    FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES oorder_3 (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
    FOREIGN KEY (ol_supply_w_id, ol_i_id) REFERENCES stock_3 (s_w_id, s_i_id) ON DELETE CASCADE,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE INDEX idx_customer_name_3 ON customer_3 (c_w_id, c_d_id, c_last, c_first);

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;