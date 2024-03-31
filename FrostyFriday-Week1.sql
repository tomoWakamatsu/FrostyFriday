/*  https://frostyfriday.org/blog/2022/07/14/week-1/

    FrostyFriday Inc., your benevolent employer, has an S3 bucket that is filled with .csv data dumps. 
    This data is needed for analysis. Your task is to create an external stage, and load the csv files 
    directly from that stage into a table.
    
    The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_1/
*/

--まずはDBやスキーマを作るよ
--ここはなんでもOK
create database if not exists frosty_friday;
create schema if not exists frosty;
drop schema if exists public;

--S3の外部ステージを作るよ
--GUIから作ってもOK
create or replace stage data_dumps 
	URL = 's3://frostyfridaychallenges/challenge_1/' 
	directory = ( enable = true );  -- これをtrueにしておくとディレクトリが有効になって更新しなくてもよくなる

-- 外部ステージの中身を確認
list @data_dumps;


-- ***solution 01 まずはベーシックな回答
-- commited by darylkit
-- https://github.com/darylkit/Frosty_Friday/blob/main/Week%201%20-%20External%20Stages/external_stages.sql
--テーブル作成
create table if not exists data_dump as 
select $1 as val from @data_dumps;

-- どんなデータができたかみてみよう
select * from data_dump;

---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*


-- ***solution 0２　実務であるあるパターン
-- commited by tshoji
-- https://github.com/taksho/Frosty_Friday/blob/main/Week1/external_stages.sql

-- Set the SYSADMIN role
use role sysadmin;

-- Create warehouse, Database, schema
create or replace warehouse ff_wh AUTO_SUSPEND=1;
create or replace database ff_db;
create or replace schema ff_db.ff_schema;

-- Create S3 External Stage
create or replace stage ff_db.ff_schema.ff_s3
    url = 's3://frostyfridaychallenges/challenge_1/'
;

-- List files in S3 bucket
list @ff_db.ff_schema.ff_s3;

-- Create a file format for csv
create or replace file format csv_ff
    type = csv
;

-- Check the files using the file format
select $1, metadata$filename, metadata$file_row_number from @ff_s3 (file_format=>'csv_ff');

-- Replace the file format
create or replace file format csv_ff
    type = csv
    skip_header = 1
    null_if = ('NULL', 'totally_empty') 
    skip_blank_lines = true
    comment = '"null_if" is used to eliminate useless values'
;

-- Create a table
create or replace table week1csv(
    result varchar,
    filename varchar,
    file_row_number int,
    loaded_at timestamp_ltz
)
;

-- Copy the files using file format and metadatas
COPY into week1csv from (
    select 
        $1,
        metadata$filename,
        metadata$file_row_number,
        metadata$start_scan_time
    from '@ff_s3')
    file_format = (format_name = 'csv_ff')
;

-- Delete the NULL rows
delete from week1csv where result is null;

-- Select the result by the right order
select * from week1csv order by filename, file_row_number;

---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*

-- ***solution 03　複数行の文字列データを指定したカラムの値ごとに1行に集約するパターン
-- https://github.com/JavaCaste/snow/blob/main/Week1_Basic_ExternalStages.sql
-- file format
CREATE OR REPLACE FILE FORMAT csv_ff
    TYPE = 'csv' 
    SKIP_HEADER=1
    NULL_IF=('NULL','totally_empty');


--select $1 from @Week1_Basic_ExternalStage (file_format => CSV_FF);
-- LISTAGG https://docs.snowflake.com/ja/sql-reference/functions/listagg
-- LISTAGGとは・・・複数行の文字列データを指定したカラムの値ごとに1行に集約できる
-- https://dev.classmethod.jp/articles/snowflake-listagg/
SELECT LISTAGG($1,' ') WITHIN GROUP 
(ORDER BY METADATA$FILENAME, METADATA$FILE_ROW_NUMBER) AS COL1  --WITHIN GROUPでリスト内の各グループの値の順序を決定します。ファイル名とファイルNoの順番出ないと変な言葉になるよ
FROM @data_dumps (file_format=>'csv_ff');


-- テストテーブルだし、TEMPテーブルで作るのも良いよね
CREATE TEMPORARY TABLE FROSTY.Week1_Basic_ExternalStage AS 
(SELECT LISTAGG($1,' ') WITHIN GROUP (ORDER BY METADATA$FILENAME, METADATA$FILE_ROW_NUMBER) AS COL1
FROM @data_dumps  (file_format=>'csv_ff') );

select * from FROSTY.Week1_Basic_ExternalStage;


--- お掃除
drop table data_dump;
drop table Week1_Basic_ExternalStage;