## Part 1

**Getting the file from S3**

- Needed to use aws-cli (via my own aws account credentials) to view files under the given public folder path: `s3://firebolt-publishing-public/assignments/solution_architect/`

```
aws s3 ls s3://firebolt-publishing-public/assignments/solution_architect/
```

Returned one file: `part.csv`

- Download [https://s3.amazonaws.com/firebolt-publishing-public/assignments/solution_architect](https://s3.amazonaws.com/firebolt-publishing-public/assignments/solution_architect)/part.csv into my local machine

**Ingesting the file into snowflake table**

- Create a table in Snowflake as instructed:

```
create or replace TABLE FIREBOLT.FIREBOLT_ASSIGNMENT.PART (
    P_PARTKEY INT,
    P_NAME VARCHAR(55),
    P_MFGR CHAR(25),
    P_BRAND CHAR(10),
    P_TYPE VARCHAR(25),
    P_SIZE INTEGER,
    P_CONTAINER CHAR(10),
    P_RETAILPRICE DECIMAL(1,1),
    P_COMMENT VARCHAR(23)
);
```

-  Upon trying to use Snowflake UI to load the file into the created table I've encountered an issue.
- I've used **SnowSQL** to examine the file using these commands:

```
put file:///Users/mrabi/Downloads/part.csv @~;
select $1, $2, $3, $4, $5, $6, $7, $8, $9 from @~/part.csv limit 20;
```

Upon looking at the data, looks like the **decimal**  **scale**  **and precision** we've set upon creating the table for the `P_RETAILPRICE` column (1, 1) **doesn't make sense**.

- I'm going to alter the table so that the precision of `P_RETAILPRICE`would be 6 and the scale would be 2.

```
ALTER TABLE FIREBOLT.FIREBOLT_ASSIGNMENT.PART ALTER 
P_RETAILPRICE SET DATA TYPE DECIMAL(6,2);
```

- Load the data onto the table using these commands:

```
copy into FIREBOLT.FIREBOLT_ASSIGNMENT.PART
(P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT)
from (
  select $1, $2, $3, $4, $5, $6, $7, $8, $9 
  from @~/part.csv
)
file_format = (type = csv);
```

Success!

```
select * from FIREBOLT.FIREBOLT_ASSIGNMENT.PART limit 10;
+-----------+------------------------------------------+----------------+----------+-------------------------+--------+-------------+---------------+----------------------+
| P_PARTKEY | P_NAME                                   | P_MFGR         | P_BRAND  | P_TYPE                  | P_SIZE | P_CONTAINER | P_RETAILPRICE | P_COMMENT            |
|-----------+------------------------------------------+----------------+----------+-------------------------+--------+-------------+---------------+----------------------|
|         1 | goldenrod lavender spring chocolate lace | Manufacturer#1 | Brand#13 | PROMO BURNISHED COPPER  |      7 | JUMBO PKG   |        901.00 | ly. slyly ironi      |
|         2 | blush thistle blue yellow saddle         | Manufacturer#1 | Brand#13 | LARGE BRUSHED BRASS     |      1 | LG CASE     |        902.00 | lar accounts amo     |
|         3 | spring green yellow purple cornsilk      | Manufacturer#4 | Brand#42 | STANDARD POLISHED BRASS |     21 | WRAP CASE   |        903.00 | egular deposits hag  |
|         4 | cornflower chocolate smoke green pink    | Manufacturer#3 | Brand#34 | SMALL PLATED BRASS      |     14 | MED DRUM    |        904.00 | p furiously r        |
|         5 | forest brown coral puff cream            | Manufacturer#3 | Brand#32 | STANDARD POLISHED TIN   |     15 | SM PKG      |        905.00 |  wake carefully      |
|         6 | bisque cornflower lawn forest magenta    | Manufacturer#2 | Brand#24 | PROMO PLATED STEEL      |      4 | MED BAG     |        906.00 | sual a               |
|         7 | moccasin green thistle khaki floral      | Manufacturer#1 | Brand#11 | SMALL PLATED COPPER     |     45 | SM BAG      |        907.00 | lyly. ex             |
|         8 | misty lace thistle snow royal            | Manufacturer#4 | Brand#44 | PROMO BURNISHED TIN     |     41 | LG DRUM     |        908.00 | eposi                |
|         9 | thistle dim navajo dark gainsboro        | Manufacturer#4 | Brand#43 | SMALL BURNISHED STEEL   |     12 | WRAP CASE   |        909.00 | ironic foxe          |
|        10 | linen pink saddle puff powder            | Manufacturer#5 | Brand#54 | LARGE BURNISHED STEEL   |     44 | LG CAN      |        910.01 | ithely final deposit |
+-----------+------------------------------------------+----------------+----------+-------------------------+--------+-------------+---------------+----------------------+
10 Row(s) produced. Time Elapsed: 0.462s

```

## Part 2

So, I've started with this: considering a row has an l_extendedprice **** equals to **x,**   **how can I get the previous l_extendedprice?** (mimicking the **lag** window function). This query does it:

```
select max(l_extendedprice)
from (select *
      from
      "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM"
      where l_extendedprice < x and l_orderkey = 1466369)
```

Since I need the **l_partkey** and not the **l_extendedprice**, I can't simply put this query in a subselect in my select statement.

Instead, I've done a self-join on LINEITEM to **join each row with her previous l_extendedprice row** while utilizing the query above in my where statement:

```
select
  origin.l_orderkey,
  origin.l_partkey,
  origin.l_extendedprice,
  window.l_partkey as next_l_partkey
from
  ("SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM") as origin,
  ("SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM") as window
where 
  origin.l_orderkey = 1466369
  and window.l_orderkey = origin.l_orderkey 
  and window.l_extendedprice = (select max(l_extendedprice) from
                                "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" 
                                where l_orderkey = origin.l_orderkey and 
                                l_extendedprice < origin.l_extendedprice);
```

The problem with this statement is that it doesn't return the first row (which the next_l_partkey should be null).

To solve this, I've added another statement to handle this case and unioned them together for the **final query** to achieve the same result without using window functions:

```
select
  origin.l_orderkey,
  origin.l_partkey,
  origin.l_extendedprice,
  window.l_partkey as next_l_partkey
from
  ("SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM") as origin,
  ("SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM") as window
where 
  origin.l_orderkey = 1466369
  and window.l_orderkey = origin.l_orderkey 
  and window.l_extendedprice = (select max(l_extendedprice) from
                                "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" 
                                where l_orderkey = origin.l_orderkey and 
                                l_extendedprice < origin.l_extendedprice)
union
  select l_orderkey, l_partkey, l_extendedprice, null as next_l_partkey
  from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" only_min_row
  where l_orderkey = 1466369 and 
  l_extendedprice = (select min(l_extendedprice) 
                     from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM" 
                     where l_orderkey = only_min_row.l_orderkey);
```

This might not be the most elegant way to solve this.

There might be a way to achieve this goal without the union (maybe going down the path of nested sub-selects), but since this query will not change no matter the size of the table, I'm happy with this.
