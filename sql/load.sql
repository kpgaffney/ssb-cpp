DROP TABLE IF EXISTS lineorder;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS date;
DROP TABLE IF EXISTS part_encoded;
DROP TABLE IF EXISTS supplier_encoded;
DROP TABLE IF EXISTS customer_encoded;
DROP TABLE IF EXISTS date_encoded;
DROP TABLE IF EXISTS mfgr_codes;
DROP TABLE IF EXISTS category_codes;
DROP TABLE IF EXISTS brand1_codes;
DROP TABLE IF EXISTS city_codes;
DROP TABLE IF EXISTS nation_codes;
DROP TABLE IF EXISTS region_codes;
DROP TABLE IF EXISTS yearmonth_codes;

CREATE TABLE part
(
    partkey   INTEGER,
    p_name      TEXT,
    p_mfgr      TEXT,
    p_category  TEXT,
    p_brand1    TEXT,
    p_color     TEXT,
    p_type      TEXT,
    p_size      INTEGER,
    p_container TEXT,
    PRIMARY KEY (partkey)
);

CREATE TABLE supplier
(
    s_suppkey INTEGER,
    s_name    TEXT,
    s_address TEXT,
    s_city    TEXT,
    s_nation  TEXT,
    s_region  TEXT,
    s_phone   TEXT,
    PRIMARY KEY (s_suppkey)
);

CREATE TABLE customer
(
    c_custkey    INTEGER,
    c_name       TEXT,
    c_address    TEXT,
    c_city       TEXT,
    c_nation     TEXT,
    c_region     TEXT,
    c_phone      TEXT,
    c_mktsegment TEXT,
    PRIMARY KEY (c_custkey)
);

CREATE TABLE date
(
    d_datekey          INTEGER,
    d_date             TEXT,
    d_dayofweek        TEXT,
    d_month            TEXT,
    d_year             INTEGER,
    d_yearmonthnum     INTEGER,
    d_yearmonth        TEXT,
    d_daynuminweek     INTEGER,
    d_daynuminmonth    INTEGER,
    d_daynuminyear     INTEGER,
    d_monthnuminyear   INTEGER,
    d_weeknuminyear    INTEGER,
    d_sellingseason    TEXT,
    d_lastdayinweekfl  INTEGER,
    d_lastdayinmonthfl INTEGER,
    d_holidayfl        INTEGER,
    d_weekdayfl        INTEGER,
    PRIMARY KEY (d_datekey)
);

CREATE TABLE lineorder
(
    lo_orderkey      INTEGER,
    lo_linenumber    INTEGER,
    lo_custkey       INTEGER,
    lo_partkey       INTEGER,
    lo_suppkey       INTEGER,
    lo_orderdate     INTEGER,
    lo_orderpriority TEXT,
    lo_shippriority  TEXT,
    lo_quantity      INTEGER,
    lo_extendedprice INTEGER,
    lo_ordtotalprice INTEGER,
    lo_discount      INTEGER,
    lo_revenue       INTEGER,
    lo_supplycost    INTEGER,
    lo_tax           INTEGER,
    lo_commitdate    INTEGER,
    lo_shipmode      TEXT,
    PRIMARY KEY (lo_orderkey, lo_linenumber),
    FOREIGN KEY (lo_custkey) REFERENCES customer (c_custkey),
    FOREIGN KEY (lo_partkey) REFERENCES part (partkey),
    FOREIGN KEY (lo_suppkey) REFERENCES supplier (s_suppkey),
    FOREIGN KEY (lo_orderdate) REFERENCES date (d_datekey)
);

CREATE TABLE part_encoded
(
    partkey   INTEGER,
    p_name      TEXT,
    p_mfgr      INTEGER,
    p_category  INTEGER,
    p_brand1    INTEGER,
    p_color     TEXT,
    p_type      TEXT,
    p_size      INTEGER,
    p_container TEXT,
    PRIMARY KEY (partkey)
);

CREATE TABLE supplier_encoded
(
    s_suppkey INTEGER,
    s_name    TEXT,
    s_address TEXT,
    s_city    INTEGER,
    s_nation  INTEGER,
    s_region  INTEGER,
    s_phone   TEXT,
    PRIMARY KEY (s_suppkey)
);

CREATE TABLE customer_encoded
(
    c_custkey    INTEGER,
    c_name       TEXT,
    c_address    TEXT,
    c_city       INTEGER,
    c_nation     INTEGER,
    c_region     INTEGER,
    c_phone      TEXT,
    c_mktsegment TEXT,
    PRIMARY KEY (c_custkey)
);

CREATE TABLE date_encoded
(
    d_datekey          INTEGER,
    d_date             TEXT,
    d_dayofweek        TEXT,
    d_month            TEXT,
    d_year             INTEGER,
    d_yearmonthnum     INTEGER,
    d_yearmonth        INTEGER,
    d_daynuminweek     INTEGER,
    d_daynuminmonth    INTEGER,
    d_daynuminyear     INTEGER,
    d_monthnuminyear   INTEGER,
    d_weeknuminyear    INTEGER,
    d_sellingseason    TEXT,
    d_lastdayinweekfl  BOOLEAN,
    d_lastdayinmonthfl BOOLEAN,
    d_holidayfl        BOOLEAN,
    d_weekdayfl        BOOLEAN,
    PRIMARY KEY (d_datekey)
);

CREATE TABLE mfgr_codes
(
    mfgr_code INTEGER PRIMARY KEY AUTOINCREMENT,
    mfgr      TEXT UNIQUE
);

CREATE TABLE category_codes
(
    category_code INTEGER PRIMARY KEY AUTOINCREMENT,
    category      TEXT UNIQUE
);

CREATE TABLE brand1_codes
(
    brand1_code INTEGER PRIMARY KEY AUTOINCREMENT,
    brand1      TEXT UNIQUE
);

CREATE TABLE city_codes
(
    city_code INTEGER PRIMARY KEY AUTOINCREMENT,
    city      TEXT UNIQUE
);

CREATE TABLE nation_codes
(
    nation_code INTEGER PRIMARY KEY AUTOINCREMENT,
    nation      TEXT UNIQUE
);

CREATE TABLE region_codes
(
    region_code INTEGER PRIMARY KEY AUTOINCREMENT,
    region      TEXT UNIQUE
);

CREATE TABLE yearmonth_codes
(
    yearmonth_code INTEGER PRIMARY KEY AUTOINCREMENT,
    yearmonth      TEXT UNIQUE
);

.import part.tbl part
.import supplier.tbl supplier
.import customer.tbl customer
.import date.tbl date
.import lineorder.tbl lineorder

INSERT INTO mfgr_codes (mfgr)
SELECT DISTINCT p_mfgr
FROM part
ORDER BY p_mfgr;

INSERT INTO category_codes (category)
SELECT DISTINCT p_category
FROM part
ORDER BY p_category;

INSERT INTO brand1_codes (brand1)
SELECT DISTINCT p_brand1
FROM part
ORDER BY p_brand1;

INSERT INTO city_codes (city)
SELECT s_city AS city
FROM supplier
UNION
SELECT c_city AS city
FROM customer
ORDER BY city;

INSERT INTO nation_codes (nation)
SELECT s_nation AS nation
FROM supplier
UNION
SELECT c_nation AS nation
FROM customer
ORDER BY nation;

INSERT INTO region_codes (region)
SELECT s_region AS region
FROM supplier
UNION
SELECT c_region AS region
FROM customer
ORDER BY region;

INSERT INTO yearmonth_codes (yearmonth)
SELECT DISTINCT d_yearmonth
FROM date
ORDER BY d_yearmonth;

INSERT INTO part_encoded
SELECT partkey,
       p_name,
       mfgr_code,
       category_code,
       brand1_code,
       p_color,
       p_type,
       p_size,
       p_container
FROM part,
     mfgr_codes,
     category_codes,
     brand1_codes
WHERE p_mfgr = mfgr
  AND p_category = category
  AND p_brand1 = brand1;

INSERT INTO supplier_encoded
SELECT s_suppkey, s_name, s_address, city_code, nation_code, region_code, s_phone
FROM supplier,
     city_codes,
     nation_codes,
     region_codes
WHERE s_city = city
  AND s_nation = nation
  AND s_region = region;

INSERT INTO customer_encoded
SELECT c_custkey,
       c_name,
       c_address,
       city_code,
       nation_code,
       region_code,
       c_phone,
       c_mktsegment
FROM customer,
     city_codes,
     nation_codes,
     region_codes
WHERE c_city = city
  AND c_nation = nation
  AND c_region = region;

INSERT INTO date_encoded
SELECT d_datekey,
       d_date,
       d_dayofweek,
       d_month,
       d_year,
       d_yearmonthnum,
       yearmonth_code,
       d_daynuminweek,
       d_daynuminmonth,
       d_daynuminyear,
       d_monthnuminyear,
       d_weeknuminyear,
       d_sellingseason,
       d_lastdayinweekfl,
       d_lastdayinmonthfl,
       d_holidayfl,
       d_weekdayfl
FROM date,
     yearmonth_codes
WHERE d_yearmonth = yearmonth;
