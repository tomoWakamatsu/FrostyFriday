-- https://frostyfriday.org/2022/11/18/week-23-basic/


-- in CMD on MacOS

https://developers.snowflake.com/snowsql/

--接続情報Tips--------------------------------------------------------------
--.snowsql/configに以下のように書いておくと切り替えできる
--複数環境ある方は便利

--Configはここにありますよ
--Windows: C:\Users\<Your-Username>\.snowsql\config
--Mac/Linux: /Users/<Your-Username>/.snowsql/config または ~/.snowsql/config

-- configファイルの中身にこれを書く-----------------------------------------------
[connections.prod01]
accountname = test3.southeast-asia.azure
username = testuser
password = xxxxxx


[connections.dev01]
accountname = test1.ap-northeast-1.aws
username = testuser
password = xxxxxx


-- ここからsnowSQL -----------------------------------------------

$ snowsql -c prod01
Select current_region();
$ snowsql --connection dev01
Select current_region();



--　条件1: '1' で終わるファイルを一括ロード (data_batch_1-1.csv、data_batch_1-11.csv など)
--　今回はデフォルトのユーザーステージにPUT
/*************************************************************************************
snowsql
--put file://C:\splitcsv-c18c2b43-ca57-4e6e-8d95-f2a689335892-results\*1.csv @~/w23;
--put file:///Users/username/FF23/*1.csv @~/w23;
put file:///Users/twakamatsu/FF23/*1.csv @~/w23;
************************************************************************************/


-- ユーザーステージにファイルがステージングされたかチェック
ls @~;

create database ff23db;

-- ファイルフォーマットを作成
create or replace temporary file format ff_csv
type = csv
skip_header = 1
field_optionally_enclosed_by='"'
;

-- 確認
select $1 as id, $2 as first_name, $3 as last_name, $4 as email, $5 as gender, $6 as ip_address 
from @~/w23 (file_format => ff_csv)
order by 1;


-- Temporaryテーブル作成
-- Temporaryなのでセッションの間のみ有効
create or replace temporary table frosty_w23 (
  id number not null
, first_name varchar
, last_name varchar
, email2 varchar
, gender varchar
, email varchar
);


-- データロード
copy into frosty_w23 
from (
    select $1 as id, $2 as first_name, $3 as last_name, $4 as email2, $5 as gender, $4 as email
    from '@~/w23'
 ) 
file_format = (format_name = ff_csv)
on_error = skip_file  -- 条件2:エラーのあるファイルをスキップする
;

-- test
select * from frosty_w23 order by id limit 10;


-- cleanup REMOVEコマンドでCSVを削除します
-- https://docs.snowflake.com/ja/sql-reference/sql/remove
rm @~/w23;

-- ユーザーステージが消えたかチェック
ls @~;

-- clean up
drop database ff23db;

