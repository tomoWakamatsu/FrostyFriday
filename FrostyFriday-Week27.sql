
/*  https://frostyfriday.org/2022/12/16/week-27-beginner/

Frosty Friday Week27
SELECT EXCLUDE & RENAME
*/

-- SYSADMINを使います
use role sysadmin;
use warehouse compute_wh;

-- まずはDBを作る 
create database if not exists ffdb;


-- テーブル作ります
create or replace table ffweek27 
(
    icecream_id int,
    icecream_flavour varchar(15),
    icecream_manufacturer varchar(50),
    icecream_brand varchar(50),
    icecreambrandowner varchar(50),
    milktype varchar(15),
    region_of_origin varchar(50),
    recomendad_price number,
    wholesale_price number
);

insert into ffweek27 values
    (1, 'strawberry', 'Jimmy Ice', 'Ice Co.', 'Food Brand Inc.', 'normal', 'Midwest', 7.99, 5),
    (2, 'vanilla', 'Kelly Cream Company', 'Ice Co.', 'Food Brand Inc.', 'dna-modified', 'Northeast', 3.99, 2.5),
    (3, 'chocolate', 'ChoccyCream', 'Ice Co.', 'Food Brand Inc.', 'normal', 'Midwest', 8.99, 5.5);

select *
    exclude milktype
    rename icecreambrandowner as ice_cream_brand_owner
     from ffweek27;

-- でも、excludeできたら何が嬉しいのかな？-----

-- 例えばめちゃくちゃ列のあるものに対して、最後の一つだけをのぞいてGroup BYしたい場合
-- サンプルテーブル(100万行)
-- Reference:https://zenn.dev/indigo13love/articles/142571ac201e53
create or replace table very_wide_table(
  c1 int,
  c2 int,
  c3 int,
  c4 int,
  c5 int,
  c6 int,
  c7 int,
  c8 int,
  c9 int,
  c10 int,
  c11 int,
  c12 int,
  c13 int,
  c14 int,
  c15 int,
  c16 int,
  c17 int,
  c18 int,
  c19 int,
  c20 int,
  c21 int,
  c22 int,
  c23 int,
  c24 int,
  c25 int,
  c26 int,
  c27 int,
  c28 int,
  c29 int,
  c30 int,
  c31 int,
  c32 int,
  c33 int,
  c34 int,
  c35 int,
  c36 int,
  c37 int,
  c38 int,
  c39 int,
  c40 int,
  c41 int,
  c42 int,
  c43 int,
  c44 int,
  c45 int,
  c46 int,
  c47 int,
  c48 int,
  c49 int,
  c50 int,
  c51 int,
  c52 int,
  c53 int,
  c54 int,
  c55 int,
  c56 int,
  c57 int,
  c58 int,
  c59 int,
  c60 int,
  c61 int,
  c62 int,
  c63 int,
  c64 int,
  c65 int,
  c66 int,
  c67 int,
  c68 int,
  c69 int,
  c70 int,
  c71 int,
  c72 int,
  c73 int,
  c74 int,
  c75 int,
  c76 int,
  c77 int,
  c78 int,
  c79 int,
  c80 int,
  c81 int,
  c82 int,
  c83 int,
  c84 int,
  c85 int,
  c86 int,
  c87 int,
  c88 int,
  c89 int,
  c90 int,
  c91 int,
  c92 int,
  c93 int,
  c94 int,
  c95 int,
  c96 int,
  c97 int,
  c98 int,
  c99 int,
  c100 int
) as
select
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random(),
  random()
from table(generator(rowcount => 100000));

--これくらいならまあまあかける
SELECT
  c1
 ,c2
 ,c3
 ,SUM(c100)
FROM very_wide_table
GROUP BY 
  c1
 ,c2
 ,c3;

-- けどこれがc100まであって、さらにc1-c98まででGROUP BYしたいってなったら。。。

SELECT
  c1,
  c2,
  c3,
  c4,
  c5,
  c6,
  c7,
  c8,
  c9,
  c10,
  c11,
  c12,
  c13,
  c14,
  c15,
  c16,
  c17,
  c18,
  c19,
  c20,
  c21,
  c22,
  c23,
  c24,
  c25,
  c26,
  c27,
  c28,
  c29,
  c30,
  c31,
  c32,
  c33,
  c34,
  c35,
  c36,
  c37,
  c38,
  c39,
  c40,
  c41,
  c42,
  c43,
  c44,
  c45,
  c46,
  c47,
  c48,
  c49,
  c50,
  c51,
  c52,
  c53,
  c54,
  c55,
  c56,
  c57,
  c58,
  c59,
  c60,
  c61,
  c62,
  c63,
  c64,
  c65,
  c66,
  c67,
  c68,
  c69,
  c70,
  c71,
  c72,
  c73,
  c74,
  c75,
  c76,
  c77,
  c78,
  c79,
  c80,
  c81,
  c82,
  c83,
  c84,
  c85,
  c86,
  c87,
  c88,
  c89,
  c90,
  c91,
  c92,
  c93,
  c94,
  c95,
  c96,
  c97,
  c98,
  SUM(c100)
FROM
  very_wide_table
GROUP BY
  c1,
  c2,
  c3,
  c4,
  c5,
  c6,
  c7,
  c8,
  c9,
  c10,
  c11,
  c12,
  c13,
  c14,
  c15,
  c16,
  c17,
  c18,
  c19,
  c20,
  c21,
  c22,
  c23,
  c24,
  c25,
  c26,
  c27,
  c28,
  c29,
  c30,
  c31,
  c32,
  c33,
  c34,
  c35,
  c36,
  c37,
  c38,
  c39,
  c40,
  c41,
  c42,
  c43,
  c44,
  c45,
  c46,
  c47,
  c48,
  c49,
  c50,
  c51,
  c52,
  c53,
  c54,
  c55,
  c56,
  c57,
  c58,
  c59,
  c60,
  c61,
  c62,
  c63,
  c64,
  c65,
  c66,
  c67,
  c68,
  c69,
  c70,
  c71,
  c72,
  c73,
  c74,
  c75,
  c76,
  c77,
  c78,
  c79,
  c80,
  c81,
  c82,
  c83,
  c84,
  c85,
  c86,
  c87,
  c88,
  c89,
  c90,
  c91,
  c92,
  c93,
  c94,
  c95,
  c96,
  c97,
  c98
  ;
  
--こんな簡単なことなのに、長っ！！（200行)
-- こんなときExcludeを使うと
SELECT * exclude(c99, c100)
  ,SUM(c100)
FROM
  very_wide_table
GROUP BY
  c1,
  c2,
  c3,
  c4,
  c5,
  c6,
  c7,
  c8,
  c9,
  c10,
  c11,
  c12,
  c13,
  c14,
  c15,
  c16,
  c17,
  c18,
  c19,
  c20,
  c21,
  c22,
  c23,
  c24,
  c25,
  c26,
  c27,
  c28,
  c29,
  c30,
  c31,
  c32,
  c33,
  c34,
  c35,
  c36,
  c37,
  c38,
  c39,
  c40,
  c41,
  c42,
  c43,
  c44,
  c45,
  c46,
  c47,
  c48,
  c49,
  c50,
  c51,
  c52,
  c53,
  c54,
  c55,
  c56,
  c57,
  c58,
  c59,
  c60,
  c61,
  c62,
  c63,
  c64,
  c65,
  c66,
  c67,
  c68,
  c69,
  c70,
  c71,
  c72,
  c73,
  c74,
  c75,
  c76,
  c77,
  c78,
  c79,
  c80,
  c81,
  c82,
  c83,
  c84,
  c85,
  c86,
  c87,
  c88,
  c89,
  c90,
  c91,
  c92,
  c93,
  c94,
  c95,
  c96,
  c97,
  c98;

-- スッキリ！(102行）
--さらにスッキリさせたいぞ
-- GROUP BY ALLを使おう
SELECT * exclude(c99, c100)
  ,SUM(c100)
FROM
  very_wide_table
GROUP BY ALL;
--なんとスッキリ(5行)

-- 同じようにrenameも大量にカラムがあるとき、数個だけ違う名前にASしたいときに便利
select c1,
    * rename c1 as rename_c1
     from very_wide_table
     limit 100;

-- 複数もできるよ
select c1,c2,
    * rename (c1 as rename_c1, c2 as rename_c2)
     from very_wide_table
     limit 100;
-- 

-- お掃除
drop database ffdb;
