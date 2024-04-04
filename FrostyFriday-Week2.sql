/*  https://frostyfriday.org/blog/2022/07/15/week-2-intermediate/

　　　　　　　シナリオ：

　　　　　　　　人事部門の関係者は、変更追跡を行いたいと考えていますが、自分たちのために作成されたストリームが、関心のない情報を提供しすぎていることを懸念しています
　　　　　　　　parquetのデータをロードしてテーブルに変換し、DEPT 列と JOB_TITLE 列への変更のみを表示するストリームを作成してください
    
    You can find the parquet data: https://frostyfridaychallenges.s3.eu-west-1.amazonaws.com/challenge_2/employees.parquet
*/

use role sysadmin;
use warehouse compute_wh;

--まずはDBやスキーマを作るよ
--ここはなんでもOK
create database if not exists frosty_friday;
create schema if not exists frosty;
USE SCHEMA frosty_friday.frosty ;


-- ***solution 01 ベーシックな回答*---*---*---*---*---**---*---*---*---*---**---*---*---*---*---**---*---*---*---*---*
-- commited by datajamesfox
-- https://github.com/datajamesfox/frosty-friday-snowflake-challenges/blob/main/Week_02.sql
-- ファイルフォーマットの作成
create or replace file format w2_parquet
    type = 'parquet';

-- 格納する場所が欲しいので内部ステージを作る（GUIでもOK）
create or replace stage week_2_stage
    file_format = w2_parquet;

-- 内部ステージの中にparquetファイルを入れる（ここはGUIでもCLIでもOK）
-- In SnowlflakeSQL CLI:
 -- connect to account: snowsql -a <accountname>.<server> -u <email> --authenticator externalbrowser
 -- use <database>
 -- use <schema>
 -- use <warehouse>
 -- put file://<filepath>\employees.parquet @week_2_stage

-- 入っている確認
list @week_2_stage;

--　中身のチェック
select $1 from @week_2_stage;

-- テーブルを作成
create or replace table week_2_table
    (city varchar,
     country varchar,
     country_code varchar,
     dept varchar,
     education varchar,
     email varchar,
     employee_id int,
     first_name varchar,
     job_title varchar,
     last_name varchar,
     payroll_iban varchar,
     postcode varchar,
     street_name varchar,
     street_num int,
     time_zone varchar,
     title varchar
    );

-- ステージからテーブルへCOPY INTO

-- match_by_column_nameってなんだっけ？⭐️
-- 公式ドキュメントをチェック→https://docs.snowflake.com/ja/sql-reference/sql/copy-into-table

-- データで表される対応する列と一致するターゲットテーブルの列に半構造化データをロードするかどうかを指定する文字列

-- 大文字と小文字が区別される（CASE_SENSITIVE）
-- 大文字と小文字が区別されない（CASE_INSENSITIVE)
-- NONE:COPY 操作は、半構造化データをバリアント列にロードするか、クエリが COPY ステートメントに含まれている場合にデータを変換します。
-- デフォルトはNONE

copy into week_2_table from @week_2_stage
    file_format = w2_parquet
    match_by_column_name = case_insensitive;
    
-- select table
select * from week_2_table;

-- DEPT と JOB_TITLEのみのViewを作ります
create or replace view week_2_view
    as (select employee_id, dept, job_title from week_2_table);
    
-- select view
select * from week_2_view;

-- テーブルとビューの構造の違いを見てみましょう
desc table week_2_table;
desc view week_2_view;

-- Viewに対してのストリームを作成する。
create or replace stream week_2_stream on view week_2_view;

-- execute following commands
UPDATE week_2_table SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
UPDATE week_2_table SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
UPDATE week_2_table SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
UPDATE week_2_table SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
UPDATE week_2_table SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;

-- select stream
-- 変更があったDEPT(25)とJOB_TITLE(68)のもののみ表示される
-- 
select * from week_2_stream;

-- UPDATEしたのに、METADATA$ACTION　がINSERT/DELETEなのどうして？？
-- 公式ドキュメント
-- [データフロー]がわかりやすいです
-- https://docs.snowflake.com/ja/user-guide/streams-intro

-- お掃除
　drop database frosty_friday;

--*---*---*---*---*---**---*---*---*---*---**---*---*---*---*---**---*---*---*---*---**---*---*---*---*

-- ***solution 0２　Schema Detectionでより楽に*---*---*---*---*---*---*---*---*---*---**---*---*---*---*---*
-- commited by taksho
-- https://github.com/taksho/Frosty_Friday/blob/main/Week2/stream.sql
-- Set the SYSADMIN role  実務の時はSYSADMINがおすすめ
use role sysadmin;

-- Create database, schema
create database ff_db;
create schema ff_db.ff_schema;
use schema ff_db.ff_schema;


-- Create internal stage
create or replace stage week_2_stage;

-- GUIでparquetをおく

-- Upload .parquet by UI and list a file in internal stage
list @week_2_stage;

-- Create a file format for parquet
create or replace file format parquet_ff
    type = parquet
;

-- Schema Detectionでどのようなカラムになるかチェック
-- Schema Detectionって？
-- 公式ドキュメント
-- https://docs.snowflake.com/ja/sql-reference/functions/infer_schema

select * from table(
    infer_schema(
        location=>'@week_2_stage',
        file_format=>'parquet_ff'
    )
)
;

-- Schema Detectionを使ってテーブルを作成
-- ARRAY_AGG って何？
-- 公式ドキュメント
-- https://docs.snowflake.com/ja/sql-reference/functions/array_agg?utm_source=snowscope&utm_medium=serp&utm_term=ARRAY_AGG
-- 配列にピボットされた入力値を返します。入力が空の場合、関数は空の配列を返します。
create table week2parquet using template (
    select array_agg(object_construct(*))
    from table (
        infer_schema(
            location=>'@week_2_stage',
            file_format=>'parquet_ff'
            )
    )
);

-- parquetのデータをロードします
copy into week2parquet from '@week_2_stage'
file_format = (format_name = 'parquet_ff') MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;

-- Check the table
select * from week2parquet;

--  DEPTおよびJOB_TITLEカラムの変更のみを表示するストリームを作成する。
create view week2view as
select "employee_id", "dept", "job_title" from week2parquet;

-- Viewに対してのストリームを作成する。
create or replace stream week2view_stream on view week2view;

-- Execute the following commands
UPDATE week2parquet SET "country" = 'Japan' WHERE "employee_id" = 8;
UPDATE week2parquet SET "last_name" = 'Forester' WHERE "employee_id" = 22;
UPDATE week2parquet SET "dept" = 'Marketing' WHERE "employee_id" = 25;
UPDATE week2parquet SET "title" = 'Ms' WHERE "employee_id" = 32;
UPDATE week2parquet SET "job_title" = 'Senior Financial Analyst' WHERE "employee_id" = 68;

-- Check the result of stream
select * from week2view_stream;

-- お掃除
drop database ff_db;

*---*---*---*---*---**---*---*---*---*---**---*---*---*---*---**---*---*---*---*---**---*---*---*---*---*

