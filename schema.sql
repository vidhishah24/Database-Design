CREATE TABLE PART (
	P_PARTKEY		INTEGER,
	P_NAME			VARCHAR(55),
	P_MFGR			VARCHAR(25),
	P_BRAND			VARCHAR(10),
	P_TYPE			VARCHAR(25),
	P_SIZE			INTEGER,
	P_CONTAINER		VARCHAR(10),
	P_RETAILPRICE	DOUBLE,
	P_COMMENT		VARCHAR(23)
);

CREATE TABLE SUPPLIER (
	S_SUPPKEY		INTEGER,
	S_NAME			VARCHAR(25),
	S_ADDRESS		VARCHAR(40),
	S_NATIONKEY		INTEGER NOT NULL, -- references N_NATIONKEY
	S_PHONE			VARCHAR(15),
	S_ACCTBAL		DOUBLE,
	S_COMMENT		VARCHAR(101)
);

CREATE TABLE PARTSUPP (
	PS_PARTKEY		INTEGER NOT NULL, -- references P_PARTKEY
	PS_SUPPKEY		INTEGER NOT NULL, -- references S_SUPPKEY
	PS_AVAILQTY		INTEGER,
	PS_SUPPLYCOST	DOUBLE,
	PS_COMMENT		VARCHAR(199)
);


CREATE TABLE CUSTOMER (
	C_CUSTKEY		INTEGER,
	C_NAME			VARCHAR(25),
	C_ADDRESS		VARCHAR(40),
	C_NATIONKEY		INTEGER NOT NULL, -- references N_NATIONKEY
	C_PHONE			VARCHAR(15),
	C_ACCTBAL		DOUBLE,
	C_MKTSEGMENT	VARCHAR(10),
	C_COMMENT		VARCHAR(117)
);


CREATE TABLE ORDERS (
	O_ORDERKEY		INTEGER,
	O_CUSTKEY		INTEGER NOT NULL, -- references C_CUSTKEY
	O_ORDERSTATUS	VARCHAR(1),
	O_TOTALPRICE	DOUBLE,
	O_ORDERDATE		DATE,
	O_ORDERPRIORITY	VARCHAR(15),
	O_CLERK			VARCHAR(15),
	O_SHIPPRIORITY	INTEGER,
	O_COMMENT		VARCHAR(79)
);


CREATE TABLE LINEITEM (
	L_ORDERKEY		INTEGER NOT NULL, -- references O_ORDERKEY
	L_PARTKEY		INTEGER NOT NULL, -- references P_PARTKEY (compound fk to PARTSUPP)
	L_SUPPKEY		INTEGER NOT NULL, -- references S_SUPPKEY (compound fk to PARTSUPP)
	L_LINENUMBER	INTEGER,
	L_QUANTITY		DOUBLE,
	L_EXTENDEDPRICE	DOUBLE,
	L_DISCOUNT		DOUBLE,
	L_TAX			DOUBLE,
	L_RETURNFLAG	VARCHAR(1),
	L_LINESTATUS	VARCHAR(1),
	L_SHIPDATE		DATE,
	L_COMMITDATE	DATE,
	L_RECEIPTDATE	DATE,
	L_SHIPINSTRUCT	VARCHAR(25),
	L_SHIPMODE		VARCHAR(10),
	L_COMMENT		VARCHAR(44)
);

CREATE TABLE NATION (
	N_NATIONKEY		INTEGER,
	N_NAME			VARCHAR(25),
	N_REGIONKEY		INTEGER NOT NULL,  -- references R_REGIONKEY
	N_COMMENT		VARCHAR(152)
);

CREATE TABLE REGION (
	R_REGIONKEY	INTEGER,
	R_NAME		VARCHAR(25),
	R_COMMENT	VARCHAR(152)
);
/* Query 1 */
SELECT
    l_extendedprice, l_discount, l_quantity
FROM
    lineitem
WHERE
    l_shipdate >= '1994-01-01'
    AND l_shipdate < '1994-01-03'
   AND l_discount > 0.05
    AND l_discount < 0.07
    AND l_quantity > 49;

/* Query 2 Note: NOT WRONG. TESTING CROSS PRODUCT */
SELECT C.*, N.n_name, R.r_name
FROM Customer C, Nation N, Region R
WHERE N.n_nationkey = C.c_nationkey
     AND N.n_nationkey < 3
     AND C.c_mktsegment = 'FURNITURE'
     AND C.c_acctbal > 9995;

/* Query 3 */
SELECT C.*, N.n_name, R.r_name
FROM Customer C, Nation N, Region R
WHERE R.r_regionkey = N.n_regionkey
     AND N.n_nationkey = C.c_nationkey
GROUP BY N.n_name
HAVING Count(*) > 2000;

/* Query 4 */
SELECT DISTINCT L.l_shipdate, L.l_commitdate, L.l_receiptdate, PU.ps_supplycost
FROM lineitem L, partsupp PU
WHERE PU.ps_partkey = L.l_partkey
      AND  PU.ps_suppkey = L.l_suppkey
	  AND PU.ps_supplycost < 2
      AND L.l_shipdate < '1992-02-01';




/* Query 6 */
SELECT
    C.c_custkey,
    C.c_name,
    count(L.l_extendedprice) as takeback,
    C.c_acctbal,
    N.n_name,
    C.c_address,
    C.c_phone,
    C.c_comment
FROM
    customer C,
    orders O,
    lineitem L,
    nation N
WHERE
    C.c_custkey = O.o_custkey
    AND L.l_orderkey = O.o_orderkey
    AND O.o_orderdate >= '1993-10-01'
    AND O.o_orderdate < '1994-10-01'
    AND L.l_returnflag = 'R'
    AND C.c_nationkey = N.n_nationkey
GROUP BY
    C.c_custkey
HAVING count(L.l_extendedprice) > 30
ORDER BY
    takeback desc;

/* Query 7 */
SELECT C_CUSTKEY, C_NAME, C_PHONE
FROM customer
WHERE C_CUSTKEY IN (SELECT C.c_custkey
					FROM customer C, orders O, lineitem L
					WHERE O.o_orderkey = L.l_orderkey
						AND C.c_custkey = O.o_custkey
						AND L.l_quantity >= 1 AND L.l_quantity <= 3)
				AND C_ACCTBAL > 9995;

/* Query 8 */
SELECT D.c_name, D.n_name
FROM
(SELECT C.*, N.n_name, R.r_name
FROM Customer C, Nation N, Region R
WHERE R.r_regionkey = N.n_regionkey
     AND N.n_nationkey = C.c_nationkey
     AND R.r_name LIKE 'AMERICA') D,
Orders O, Lineitem L
WHERE O.o_orderkey = L.l_orderkey
AND O.o_custkey = D.c_custkey
AND L.l_shipdate < '1992-01-05';



/* Query 9 */
SELECT
    L.l_extendedprice, L.l_discount
FROM
    lineitem L,
    part P
WHERE
P.p_partkey = L.l_partkey
AND L.l_discount > 0.09
AND    
   ((
        P.p_brand = 'Brand#12'
        AND P.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND L.l_quantity >= 1 AND L.l_quantity <= 11
        AND P.p_size >= 1 AND P.p_size <= 5
        AND L.l_shipmode in ('AIR', 'AIR REG')
        AND L.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        P.p_brand = 'Brand#23'
        AND P.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND L.l_quantity >= 10 AND L.l_quantity <= 20
        AND P.p_size >= 1 AND P.p_size <= 10
        AND L.l_shipmode in ('AIR', 'AIR REG')
        AND L.l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        P.p_brand = 'Brand#34'
        AND P.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND L.l_quantity >= 20 AND L.l_quantity <= 30
        AND P.p_size >= 1 AND P.p_size <= 15
        AND L.l_shipmode in ('AIR', 'AIR REG')
        AND L.l_shipinstruct = 'DELIVER IN PERSON'
    ));

/* Query 10 */
SELECT
    L.l_orderkey,
    L.l_extendedprice,
    L.l_discount,
    O.o_orderdate,
    O.o_shippriority
FROM
    customer C,
    orders O,
    lineitem L,
    nation N
WHERE
    C.c_mktsegment = 'BUILDING'
    AND C.c_custkey = O.o_custkey
    AND L.l_orderkey = O.o_orderkey
    AND C.c_nationkey = N.n_nationkey
    AND N.n_name = 'BRAZIL'
    AND O.o_orderdate < '1994-11-01'
    AND L.l_shipdate > '1994-11-01'
    AND L.l_discount > 0.09
GROUP BY
    O.o_orderdate
ORDER BY
    O.o_orderdate desc;

/* Query 11 */
SELECT
    C.c_custkey,
    C.c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    C.c_acctbal,
    N.n_name,
    C.c_address,
    C.c_phone,
    C.c_comment
FROM
    customer C,
    orders O,
    lineitem L,
    nation N
WHERE
    C.c_custkey = O.o_custkey
    AND L.l_orderkey = O.o_orderkey
    AND O.o_orderdate >= '1993-10-01'
    AND O.o_orderdate < '1994-10-01'
    AND L.l_returnflag = 'R'
    AND C.c_nationkey = N.n_nationkey
GROUP BY
    C.c_custkey
ORDER BY
    revenue desc
LIMIT 10;
