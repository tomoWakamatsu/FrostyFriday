-- frostyfriday week38 Basic Stream
-- https://frostyfriday.org/blog/2023/03/17/week-38-basic/

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;



CREATE DATABASE Frosty_DB;

-- 最初のテーブルを作成
CREATE TABLE employees (
id INT,
name VARCHAR(50),
department VARCHAR(50)
);


-- サンプルデータをInsert

INSERT INTO employees (id, name, department)
VALUES
(1, 'Alice' 'Sales'),
(2, 'Bob', 'Marketing');

-- エラーになる！なぜ？そうだ。Copilotに聞いてみよう



-- CAUTION!
-- Copilotを使う場合、Createした直後ではまだ利用することができないので、MAX３〜４時間待つ必要があります
-- https://docs.snowflake.com/en/user-guide/snowflake-copilot [Limitations]参照














-- なるほど。カンマが抜けてたのですね
INSERT INTO employees (id, name, department)
VALUES
(1, 'Alice', 'Sales'),
(2, 'Bob', 'Marketing');

INSERT INTO employees (id, name, department)
VALUES
(3, 'tomo', 'Sales Engineering'),
(4, 'Gaku', 'Developer'),
(5, 'Are', 'Developer');

-- ２つ目のテーブルを作ります
CREATE TABLE sales (
id INT,
employee_id INT,
sale_amount DECIMAL(10, 2)
);

-- データをInsertします
INSERT INTO sales (id, employee_id, sale_amount)
VALUES
(1, 1, 100.00),
(2, 1, 200.00),
(3, 2, 150.00),
(4, 4, 400.00),
(5, 5, 500.00)
;

-- 1と2を結合したViewを作ります
CREATE VIEW employee_sales AS
SELECT e.id, e.name, e.department, s.sale_amount
FROM employees e
JOIN sales s ON e.id = s.employee_id;

-- データを確認
select * from employee_sales;

-- 変更を監視するストリームを作ります
create
or replace stream ff38_stream on view employee_sales;

SHOW STREAMS LIKE 'ff38_stream';

-- Delete 1 sales from the underlaying table for the view
-- 1のSalesテーブルからid=3のレコードを削除します
delete from
    sales
where
    id = 1;

table sales;

select * from ff38_stream;

delete from
    sales
where
    id = 2;

table sales;

-- ビューを確認、1行が削除されている
-- ストリームを確認、1行が追加されている
-- https://docs.snowflake.com/ja/user-guide/streams-intro
-- streamのデータはずっと保持されるわけではないので退避させる
create or replace table ff38_deleted_sales (
    ID int,
    NAME string,
    DEPARTMENT string,
    SALE_AMOUNT number,
    METADATA$ROW_ID string,
    METADATA$ACTION string,
    METADATA$ISUPDATE boolean,
    deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- streamのデータをテーブルに追加
insert into ff38_deleted_sales(
    ID ,
    NAME ,
    DEPARTMENT ,
    SALE_AMOUNT ,
    METADATA$ROW_ID ,
    METADATA$ACTION ,
    METADATA$ISUPDATE 
)   
select
    ID ,
    NAME ,
    DEPARTMENT ,
    SALE_AMOUNT ,
    METADATA$ROW_ID ,
    METADATA$ACTION ,
    METADATA$ISUPDATE 
from
    ff38_stream;

-- -- 削除が発生した際に、異なるステージ間を移動する行数を比較する
select
    count(*) as row_count,
    'base table' as source
from
    sales
union
select
    count(*) as row_count,
    'view' as source
from
    employee_sales
union
select
    count(*) as row_count,
    'stream' as source
from
    ff38_stream
union
select
    count(*) as row_count,
    'deleted table' as source
from
    ff38_deleted_sales;

-- これをStreamにデータが発生した場合に動くタスクにして、効率よくInsertしよう！

-- SYSADMINでやる場合はTASK作成の権限をあらかじめ付与しておく
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

-- TASKの作成
CREATE OR REPLACE TASK process_deleted_sales
  TARGET_COMPLETION_INTERVAL='1 MINUTE'
  WHEN system$stream_has_data('ff38_stream')    -- ここでStreamを指定
AS
-- streamのデータをテーブルに追加
insert into ff38_deleted_sales(
    ID ,
    NAME ,
    DEPARTMENT ,
    SALE_AMOUNT ,
    METADATA$ROW_ID ,
    METADATA$ACTION ,
    METADATA$ISUPDATE 
)   
select
    ID ,
    NAME ,
    DEPARTMENT ,
    SALE_AMOUNT ,
    METADATA$ROW_ID ,
    METADATA$ACTION ,
    METADATA$ISUPDATE 
from
    ff38_stream
WHERE METADATA$ACTION = 'DELETE';   -- DELETEのみ

  
ALTER TASK process_deleted_sales RESUME;   -- 始動させる

SHOW TASKS;



-- salesのDELETEは試したので、employeeでも同じことができるかテスト
select * from employees;

-- Deleteしてみる
delete from employees where id = 2;

table employees;
table ff38_deleted_sales;

-- -- 削除が発生した際に、異なるステージ間を移動する行数を比較する
select
    count(*) as row_count,
    'base table' as source
from
    employees
union
select
    count(*) as row_count,
    'view' as source
from
    employee_sales
union
select
    count(*) as row_count,
    'stream' as source
from
    ff38_stream
union
select
    count(*) as row_count,
    'deleted table' as source
from
    ff38_deleted_sales;

table ff38_deleted_sales;


-- one more
truncate table employees;
truncate table sales;
truncate table ff38_deleted_sales;


-- cleaning
drop database Frosty_DB;



