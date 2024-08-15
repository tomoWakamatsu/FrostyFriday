
/*  https://frostyfriday.org/blog/2022/10/21/week-19-basic/

　　　　　　　シナリオ：
今週は、風変わりですが便利なもの、つまり、2 つの日付間の営業日数を計算する UDF と組み合わせた日付ディメンションについて見ていきます
(簡単な課題なので、休日は除外しません)。

私たちはスーパーヒーロー組合と話し合い、どのヒーローのスケジュールも「通常の勤務スケジュール」を考慮しておらず、
「スーパーヒーローでも週末は休みが必要だ」という苦情を受けました。

このプロジェクトを開始するには、次の内容を含む日付ディメンション テーブルを構築する必要があります。 

2000年1月1日から始まる日付
年を 4 桁で表す (1-1-2000 の場合は 2000)
月の最初の 3 文字 (2000 年 1 月 1 日の場合は Jan)
月の名前 (2000 年 1 月 1 日の場合は 1 月になります)       
その月内の日番号（2000 年 1 月 1 日の場合は 1）
曜日の番号（月曜日は 1、火曜日は 2 など、または他の意味のある順序）
その年の週番号（2000 年 1 月 1 日の場合は 1）       
その年の日番号（2000 年 1 月 1 日の場合は 1）
日付ディメンションの他に、2 つの日付間の営業日数を計算する関数も作成する必要があります。この関数では、計算に 2 番目の日付を含めるか除外するかを簡単に切り替えることができます (つまり、2020 年 11 月 2 日月曜日から 2020 年 11 月 6 日金曜日までは 4 日間ですが
、2020 年 11 月 2 日月曜日から 2020 年 11 月 6 日金曜日までは 5 日間です)。

*/


--使うROLEはSYSADMIN
use role sysadmin;
-- 使うWHはCOMPUTE_WH
use warehouse compute_wh;

-- DB作成
create database ff19db;

-- テストデータ作成
create table testing_data (
id INT,
start_date DATE,
end_date DATE
);

-- データInsert
insert into testing_data (id, start_date, end_date) values (1, '11/11/2020', '9/3/2022');
insert into testing_data (id, start_date, end_date) values (2, '12/8/2020', '1/19/2022');
insert into testing_data (id, start_date, end_date) values (3, '12/24/2020', '1/15/2022');
insert into testing_data (id, start_date, end_date) values (4, '12/5/2020', '3/3/2022');
insert into testing_data (id, start_date, end_date) values (5, '12/24/2020', '6/20/2022');
insert into testing_data (id, start_date, end_date) values (6, '12/24/2020', '5/19/2022');
insert into testing_data (id, start_date, end_date) values (7, '12/31/2020', '5/6/2022');
insert into testing_data (id, start_date, end_date) values (8, '12/4/2020', '9/16/2022');
insert into testing_data (id, start_date, end_date) values (9, '11/27/2020', '4/14/2022');
insert into testing_data (id, start_date, end_date) values (10, '11/20/2020', '1/18/2022');
insert into testing_data (id, start_date, end_date) values (11, '12/1/2020', '3/31/2022');
insert into testing_data (id, start_date, end_date) values (12, '11/30/2020', '7/5/2022');
insert into testing_data (id, start_date, end_date) values (13, '11/28/2020', '6/19/2022');
insert into testing_data (id, start_date, end_date) values (14, '12/21/2020', '9/7/2022');
insert into testing_data (id, start_date, end_date) values (15, '12/13/2020', '8/15/2022');
insert into testing_data (id, start_date, end_date) values (16, '11/4/2020', '3/22/2022');
insert into testing_data (id, start_date, end_date) values (17, '12/24/2020', '8/29/2022');
insert into testing_data (id, start_date, end_date) values (18, '11/29/2020', '10/13/2022');
insert into testing_data (id, start_date, end_date) values (19, '12/10/2020', '7/31/2022');
insert into testing_data (id, start_date, end_date) values (20, '11/1/2020', '10/23/2021');

SET last_insert_query_id = LAST_QUERY_ID();

table testing_data;   -- SELECT * FROMの代わりになりますよ。Week3のあれさんのテクニックですね

---Pt1.
-- DateDiff使うUDF
-- Number型にキャストする場合はincluding::intを使う。Booleanの場合、Trueが1,Falseが0にキャストされて返ってきます
create or replace function calculate_businss_days(
    start_date date,
    end_date date,
    inc boolean
) returns int 
as
$$
datediff('day', start_date, end_date) + inc :: number   -- Datadiffはそのままだと「含まない」ため、+してあげる必要がある
$$;



-- 確認
-- 例になっていたデータと直近のデータで見てましょう
-- Include
truncate table testing_data;
insert into testing_data (id, start_date, end_date) values (21, '11/02/2020', '11/06/2020');
insert into testing_data (id, start_date, end_date) values (22, '08/01/2024', '08/08/2024');

-- datediffの挙動確認
select id,datediff('day', start_date, end_date),start_date,end_date from testing_data;

select id,
       start_date, 
       end_date,
       calculate_businss_days(start_date, end_date, true) as including,   --含めるとき
       calculate_businss_days(start_date, end_date, false) as excluding   --含めない時
  from testing_data
   order by id;


-- Pt2.ディメンションテーブルを作成するパターン(土日考慮するバターンのUDF)
-- 2020-01-01 から始まる1500日間の日付を生成するという処理をしています
-- row_number() over(order by seq4()) の役割:
-- この部分は、各行に対して行番号を振るために使われています。ORDER BY seq4() によって、生成されたシーケンスに基づいて行番号が振られます。
-- 結果的に、このクエリでは、1から始まる連番が生成されます。
-- TO_CHAR https://docs.snowflake.com/en/sql-reference/date-time-input-output
-- YEAR* / DAY* / WEEK* / MONTH / QUARTER https://docs.snowflake.com/ja/sql-reference/functions/year
-- GENERATOR https://docs.snowflake.com/ja/sql-reference/functions/generator

create or replace table date_dimension as 
select dateadd(day, (row_number() over(order by seq4())) - 1, date('2020-01-01')) as date,  -- 2020/1/1から開始して1個づつ日付を加える
       year(date) as year,                 -- dateの年部分
       monthname(date) as month,           -- dateの月の名前（デフォルトでは3文字の月名 Ex:Jan）
       to_char(date,'MMMM') as full_month_name,  -- フルネームをとってきたいときはこれでできる 
       dayofmonth(date) day_of_month,    -- dateの月の部分（数値）
       dayofweek(date) day_of_week,      -- dateの週の部分(数値)　2020/1/1は水曜日　0は日曜日
       weekofyear(date) week_of_year,    -- dateの年の週（数値） -- 2020/1/1を１週とした時のカウント(日曜日までを1週とかぞえる)
       dayofyear(date) date_of_year      -- dateの年の日付（数値）  -- 2020/1/1を1としてカウントアップ（365日まで)
  from table(generator(rowcount => 366*100)); 　-- 便利なGenerator 100年後までの日付作っておくよ


-- ROW_NUMBERはORDERの挙動を確かめてみましょう
-- https://docs.snowflake.com/en/sql-reference/functions/row_number
-- ROW_NUMBERはORDER BYとセットじゃないと動かないです
-- シーケンスは、同時実行ステートメントを含むセッションおよびステートメント全体で一意の番号を生成するために使用されます。シーケンスは、主キーまたは一意の値を必要とする任意の列の値を生成するために使用できます。でもSnowflake では、ギャップのないシーケンス番号の生成は保証されません。生成された番号は必ずしも連続しているわけではありませんので注意
-- https://docs.snowflake.com/en/user-guide/querying-sequences

select row_number() over(order by seq4()),
       row_number() over(order by seq4())-1,
       dateadd(day, (row_number() over(order by seq4())) - 1, date('2020-01-01')) as date
from table(generator(rowcount => 10));

-- 何件できたか確認
select count(1) from date_dimension;


-- including(T/F)を引数にして、含めるか含めないか制御
-- もし土日も考慮する場合は(6,0)は除く

create or replace function calculate_businss_days2 (start_date date, end_date date, including boolean)
  returns int
  as
  $$
    select count(*) + including::int - 1    -- ここはFunction1と違って、COUNTの時点で含んでいるため、-1しないといけない
      from date_dimension
     where date between start_date and end_date 
       and day_of_week not in (6,0)
  $$
  ;

-- 元々の要件も満たせているか確認
select id,
       start_date, 
       end_date,
       calculate_businss_days2(start_date, end_date, true) as including,   --含めるとき
       calculate_businss_days2(start_date, end_date, false) as excluding   --含めない時
  from testing_data 
  where id=21;

-- 土日を差し引いているか確認
select id,
       start_date, 
       end_date,
       calculate_businss_days(start_date, end_date, true) as including1,   --含めるとき
       calculate_businss_days(start_date, end_date, false) as excluding1,   --含めない時
       calculate_businss_days2(start_date, end_date, true) as including2,   --含めるとき
       calculate_businss_days2(start_date, end_date, false) as excluding2   --含めない時
  from testing_data 
  where id=22;

  


 -- タイムトラベルで元に戻す(Insertが流れた時点)
create or replace table testing_data as
select *
from testing_data at(statement => $last_insert_query_id)
;

table testing_data;

-- 改めて全件
select id,
       start_date, 
       end_date,
       calculate_businss_days(start_date, end_date, true) as including1,   --含めるとき
       calculate_businss_days(start_date, end_date, false) as excluding1,   --含めない時
       calculate_businss_days2(start_date, end_date, true) as including2,   --含めるとき
       calculate_businss_days2(start_date, end_date, false) as excluding2   --含めない時
  from testing_data 
  order by id;
 
--*----------------------------------------------------------------
-- お掃除------------------------------------------------------------
drop database ff19db;
