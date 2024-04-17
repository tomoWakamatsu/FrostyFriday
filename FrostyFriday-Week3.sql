/*  https://frostyfriday.org/blog/2022/07/15/week-3-basic/

シナリオ：

あなたの雇用主である Frosty Friday Inc. は、.csv データ ダンプで満たされた S3 バケットを持っています。これらのダンプはそれほど複雑ではなく、すべて同じスタイルと内容を持っています。これらのファイルはすべて 1 つのテーブルに配置する必要があります。

ただし、重要なデータもアップロードされる場合があり、これらのファイルには異なる命名スキームがあり、追跡する必要があります。
参照用にメタデータを別のテーブルに保存する必要があります。 S3 バケット内にファイルがあるため、これらのファイルを認識できます。
このファイル、keywords.csv には、ファイルを重要としてマークするすべてのキーワードが含まれています。
The basics aren’t earth-shattering but might cause you to scratch your head a bit once you start building the solution.

Frosty Friday Inc., your benevolent employer, has an S3 bucket that was filled with .csv data dumps. These dumps aren’t very complicated and all have the same style and contents. All of these files should be placed into a single table.

However, it might occur that some important data is uploaded as well, these files have a different naming scheme and need to be tracked. We need to have the metadata stored for reference in a separate table. You can recognize these files because of a file inside of the S3 bucket. This file, keywords.csv, contains all of the keywords that mark a file as important.

Objective:

Create a table that lists all the files in our stage that contain any of the keywords in the keywords.csv file.

The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_3/
*/


-- ***solution 01　ベーシックな回答*---*---*---*-
-- Created by Marian Eerens 
-- https://github.com/meerens/frosty-friday/blob/main/week_3_basic_metadata_queries.sql

create database frosty_friday;

USE DATABASE frosty_friday;
USE SCHEMA public;

-- ステージ作成
CREATE STAGE week3_basic
URL = 's3://frostyfridaychallenges/challenge_3/';

-- ステージの中身確認
LIST @week3_basic;

-- keywords.csv のなかのキーワード確認
-- ステージングされたファイルのメタデータのクエリ
-- https://docs.snowflake.com/ja/user-guide/querying-metadata
SELECT 
metadata$filename AS file_name,
metadata$file_row_number AS file_row_numer,
$1,$2, $3, $4
FROM @week3_basic/keywords.csv;

-- 'data'ファイルの中身の一部を見てましょう

SELECT 
metadata$filename AS file_name,
metadata$file_row_number AS number_of_rows,
$1 AS id,
$2 AS first_name,
$3 AS last_name,
$4 AS catch_phrase,
$5 AS timestamp
FROM @week3_basic/week3_data4_extra.csv;


-- 新しいファイルフォーマット作成
CREATE FILE FORMAT csv_frosty_skip_header
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

-- RAWデータを格納するテーブル作成（data fileと同じ構造にします）

CREATE OR REPLACE TABLE w3_basic_raw (
 file_name VARCHAR,
 number_of_rows VARCHAR,
 id VARCHAR,
 first_name VARCHAR,
 last_name VARCHAR,
 catch_phrase VARCHAR,
 time_stamp VARCHAR  
);


-- keyword.csvに入っているーワードを格納するテーブルを作成
CREATE OR REPLACE TABLE w3_basic_keywords (
file_name VARCHAR,
file_row_number VARCHAR,
keyword VARCHAR,
added_by VARCHAR,
nonsense VARCHAR
);

-- キーワードTableからロード
COPY INTO w3_basic_keywords
FROM
(
  SELECT 
  metadata$filename AS file_name,
  metadata$file_row_number AS file_row_numer,
  t.$1 AS keyword,
  t.$2 AS added_by, 
  t.$3 AS nonsense
  FROM @week3_basic/keywords.csv AS t
)
FILE_FORMAT = 'csv_frosty_skip_header'
PATTERN = 'challenge_3/keywords.csv';   -- パターンを固定で書く

-- 中身確認
select * from w3_basic_keywords;

-- data fileロード
COPY INTO w3_basic_raw
FROM 
(
  SELECT
  metadata$filename AS file_name,
  metadata$file_row_number AS number_of_rows,
  t.$1 AS id,
  t.$2 AS first_name,
  t.$3 AS last_name,
  t.$4 AS catch_phrase,
  t.$5 AS timestamp
  FROM @week3_basic AS t
)
FILE_FORMAT = 'csv_frosty_skip_header';


-- 中身確認
select * from w3_basic_raw;

-- create a view for the keyword files 
-- キーワードに引っかかるファイルの特定
-- CONTAINSを使うパターン
SELECT
file_name,
COUNT(*) AS number_of_rows
FROM w3_basic_raw
WHERE EXISTS 
(
  SELECT keyword
  FROM w3_basic_keywords
  WHERE CONTAINS (w3_basic_raw.file_name,w3_basic_keywords.keyword)
)
GROUP BY file_name;

-- CONTAINの代わりにLIKE ANYを使ってみる
-- クエリはこちらの方が速い（クエリプロファイルもチェック）
SELECT
file_name,
COUNT(*) AS number_of_rows
FROM w3_basic_raw
WHERE file_name like any (select '%' || $3 || '%' from w3_basic_keywords)
GROUP BY file_name;

-- VIEW作成
CREATE OR REPLACE VIEW w3_keywordfiles
AS
SELECT
file_name,
COUNT(*) AS number_of_rows
FROM w3_basic_raw
WHERE file_name like any (select '%' || $3 || '%' from w3_basic_keywords)
GROUP BY file_name;

-- 確認
SELECT * FROM w3_keywordfiles;


-- ***solution 02 正規表現使うパターン---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*
-- commited by arjansnowflake
--https://github.com/arjansnowflake/Frosty_Friday/blob/main/Week_3/week_3.sql

//データベースをCURRENT_DATE作成します
create database frostyfriday;

use database frostyfriday;
use schema public;

//CSVファイルフォーマットを作ります
create or replace file format frosty_3
    type = 'CSV'
    comment = 'file_format associated with Frosty Friday challenge #3'
    skip_header = 1;  -- スキップするファイルの先頭の行数を指定します


//外部ステージを作ります
create stage frosty_3
    URL = 's3://frostyfridaychallenges/challenge_3/'
    FILE_FORMAT = (FORMAT_NAME = 'frosty_3')
    COMMENT = 'stage for loading FrostyFriday files';


//中身の確認
list @frosty_3;

// キーワードファイルの中身をみてみましょう
// このキーワードに引っかかるファイル名を特定する
select $1 from @frosty_3/keywords.csv;


//結果テーブルを作成します
CREATE OR REPLACE TABLE CHALLENGE_3_RESULT
(FILENAME varchar,
NUMBER_OF_ROWS number);

//確認用
//LIKE ANY 
//1つ以上のパターンとの比較に基づいて、大文字と小文字を区別して文字列を照合します。
//LIKEだと１つだけだけど、LIKE ANYで複数の文字について照合することができます
--https://docs.snowflake.com/ja/sql-reference/functions/like_any
select METADATA$FILENAME FILENAME,
       count(*) NUMBER_OF_ROWS 
from @frosty_3
where FILENAME like any (select '%' || $1 || '%' from @frosty_3/keywords.csv) -- LIKE ANY
group by FILENAME
order by NUMBER_OF_ROWS;

//結果テーブルにInsertします
INSERT INTO challenge_3_result 
(select METADATA$FILENAME FILENAME, 
        count(*) NUMBER_OF_ROWS 
 from @frosty_3
 where FILENAME like any (select '%' || $1 || '%' from @frosty_3/keywords.csv)
 group by FILENAME
 ORDER BY NUMBER_OF_ROWS); 

//結果を確認
select * from challenge_3_result;

-- 
//ファイルの内容と列名をチェックするためのファイル形式
create or replace file format frosty_3_check
    type = 'CSV'
    comment = 'file_format associated with Frosty Friday challenge #3'
    skip_header = 0;

--列名GET
select $1, $2, $3, $4, $5, $6, $7 from @frosty_3/week3_data2_stacy_forgot_to_upload.csv (file_format => 'frosty_3_check');

//Creating the data table
CREATE OR REPLACE TABLE CHALLENGE_3_DATA
    (
    file_name VARCHAR,
    file_row_number VARCHAR,
    id number,
    first_name varchar,
    last_name varchar,
    catch_phrase varchar,
    timestamp date);

//再度、S3の中身確認
list @frosty_3;

//正規表現で照合しながら、正しいファイルをテーブルにコピーする。
//COPY INTOはパターンで指定するのもあり
//遅いときはコードを見直すものあり
//https://qiita.com/hoto17296/items/92e8d4bdb9d363420b62
//公式
//https://docs.snowflake.com/ja/sql-reference/sql/copy-into-table
COPY INTO challenge_3_data
FROM
(SELECT
  metadata$filename AS file_name,
  metadata$file_row_number AS number_of_rows,
  $1 AS id,
  $2 AS first_name,
  $3 AS last_name,
  $4 AS catch_phrase,
  $5 AS timestamp
FROM @frosty_3)
PATTERN= '.*?week3_data.*?.csv';  -- パターンは正規表現でもできるよ
--PATTERN= '.*?week3_data.?.csv';  -- これだとdataXのみ

//チェックする
select file_name from challenge_3_data group by all;

//結果を確認
select * from challenge_3_result;

---- お掃除
drop database frostyfriday;
drop database frosty_friday;
