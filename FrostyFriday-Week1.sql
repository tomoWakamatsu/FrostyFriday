/*  https://frostyfriday.org/blog/2022/07/14/week-1/

　　　　　　　シナリオ：

　　　　　　　　あなたの雇用主であるFrostyFriday Inc.は、.csvデータダンプで満たされたS3バケットを持っている。
    このデータは分析に必要です。あなたのタスクは、外部ステージを作成し、そのステージから直接csvファイルをテーブルにロードすることです。
    ファイルを直接テーブルにロードしてください
    
    The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_1/
*/


use role sysadmin;
use warehouse compute_wh;

-- ***solution 01 まずはベーシックな回答*---*---*---*---*---*---*---*---*---*---**---*---*---*---*---*
-- commited by darylkit
-- https://github.com/darylkit/Frosty_Friday/blob/main/Week%201%20-%20External%20Stages/external_stages.sql


--まずはDBやスキーマを作るよ
--ここはなんでもOK
create database if not exists frosty_friday;
create schema if not exists frosty;
USE SCHEMA frosty_friday.frosty ;

--S3の外部ステージを作るよ
--GUIから作ってもOK
create or replace stage data_dumps 
	URL = 's3://frostyfridaychallenges/challenge_1/' 
	directory = ( enable = true );  -- これをtrueにしておくとディレクトリが有効になって更新しなくてもよくなる

-- 外部ステージの中身を確認
list @data_dumps;


--テーブル作成
create table if not exists data_dump as 
select $1 as val from @data_dumps;

-- どんなデータができたかみてみよう
select * from data_dump;

--- お掃除
drop database frosty_friday;

---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*


-- ***solution 0２　実務であるあるパターン*---*---*---*---*---*---*---*---*---*---**---*---*---*---*---*
-- commited by tshoji
-- https://github.com/taksho/Frosty_Friday/blob/main/Week1/external_stages.sql

-- Set the SYSADMIN role
use role sysadmin;

-- Database, schema
create or replace database ff_db;
create or replace schema ff_db.ff_schema;
USE SCHEMA ff_db.ff_schema ;

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

-- Select the result by the right order
select * from week1csv order by filename, file_row_number;

-- Delete the NULL rows
delete from week1csv where result is null;

-- Select the result by the right order
select * from week1csv order by filename, file_row_number;


--- お掃除
drop database ff_db;

---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*---*

-- ***solution 03　複数行の文字列データを指定したカラムの値ごとに1行に集約するパターン*---*---*---*---*---*---*---*
-- https://github.com/JavaCaste/snow/blob/main/Week1_Basic_ExternalStages.sql
-- file format

CREATE DATABASE SAMPLES;
CREATE SCHEMA SAMPLES.FROSTY;
USE SCHEMA SAMPLES.FROSTY ;

-- create stage
CREATE STAGE Week1_Basic_ExternalStage url='s3://frostyfridaychallenges/challenge_1/';

list @Week1_Basic_ExternalStage;


CREATE OR REPLACE FILE FORMAT csv_ff
    TYPE = 'csv' 
    SKIP_HEADER=1
    NULL_IF=('NULL','totally_empty');


--select $1 from @Week1_Basic_ExternalStage (file_format => CSV_FF);
-- LISTAGG https://docs.snowflake.com/ja/sql-reference/functions/listagg
-- LISTAGGとは・・・複数行の文字列データを指定したカラムの値ごとに1行に集約できる
-- https://dev.classmethod.jp/articles/snowflake-listagg/
SELECT LISTAGG($1,' ') WITHIN GROUP (ORDER BY METADATA$FILENAME, METADATA$FILE_ROW_NUMBER) AS COL1
FROM @Week1_Basic_ExternalStage (file_format=>'csv_ff');


-- テストテーブルだし、TEMPテーブルで作るのも良いよね
CREATE TEMPORARY TABLE FROSTY.Week1_Basic_ExternalStage AS 
(SELECT LISTAGG($1,' ') WITHIN GROUP (ORDER BY METADATA$FILENAME, METADATA$FILE_ROW_NUMBER) AS COL1
FROM @FROSTY.Week1_Basic_ExternalStage  (file_format=>'csv_ff') );


SELECT * FROM FROSTY.Week1_Basic_ExternalStage;

-- お掃除
drop database SAMPLES;

*---*---*---*---*---*---*---*---*---*---**---*---*---*---*---*---*---*---*---*---**---*---*---*---*

