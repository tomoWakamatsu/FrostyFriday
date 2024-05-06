/*  https://frostyfriday.org/blog/2022/07/15/week-5-basic/

今週は、この記事の執筆時点で報道でかなり注目されている機能、つまり
Snowflake の Python を使用します。

シナリオ：

まず、数値を含む 1 つの列を含む単純なテーブルを作成します。サイズと量は自由に設定できます。 

その後、これらの数値を 3 で乗算するという非常に基本的な関数から始めます。

ここでの課題は、「非常に難しい Python 関数を構築する」ことではなく、Snowflake で関数を構築して使用することです。

単純な select ステートメントを使用してコードをテストできます。

Ex)*******************************

SELECT timesthree(start_int)
FROM FF_week_5
**********************************

This week, we’re using a feature that, at the time of writing, is pretty hot off the press :
Python in Snowflake.

To start out  create a simple table with a single column with a number, the size and amount are up to you, 

After that we’ll start with a very basic function: multiply those numbers by 3.

The challenge here is not ‘build a very difficult python function’ but to build and use the function in Snowflake.
*/




-- 環境の構築

create database if not exists frosty_friday;
create schema if not exists frosty;
drop schema if exists public;



-- https://github.com/marioveld/frosty_friday/blob/main/ffw5/ffw5.sql
-- Created by Mario van der Velden
-- GENERATORを使ってテスト用データを作る
-- https://docs.snowflake.com/ja/sql-reference/functions/generator
CREATE OR REPLACE TEMPORARY TABLE
    ff_week_5
    AS SELECT
        ROW_NUMBER() OVER (ORDER BY TRUE) AS start_int   -- 1から始まるデータにしたいのでWINDOW関数のROW_NUMBERを使う
    FROM TABLE(GENERATOR(ROWCOUNT => 500))
    ;


-- データが正しく入っているかチェック
select * from FF_WEEK_5;


-- ***solution 01　SQL UDF*---*---*---*-
-- SQL UDFを作ってみる 
create or replace function timesthree(i integer)
  returns integer
  as
  $$
    i*3
  $$
  ;

-- 関数のテスト。値に対して３をかけた値が返ってきていればOK
SELECT start_int, 
       timesthree(start_int) as timesthreeVal
FROM FF_week_5;


-- ***solution 02　python UDF*---*---*---*-
-- python UDFを作ってみる 
create or replace function timesthree_python(i int)
returns int
language python
runtime_version = '3.8'
handler = 'timesthree_py'
as
$$
def timesthree_py(i):
  return i*3
$$;

-- 関数のテスト。値に対して３をかけた値が返ってきていればOK
SELECT start_int,
       timesthree_python(start_int) as timesthreeValPy
FROM FF_week_5;

-- お掃除
drop database frosty_friday;