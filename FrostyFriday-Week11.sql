
--タスクを起動するには「グローバル権限」のEXECUTE TASKが必要となるので、ACCOUNTADMINでSYSADMINに権限を付与
use role accountadmin;
grant execute task on account to role SYSADMIN;
grant execute managed task on account to role SYSADMIN;

--使うROLEはSYSADMIN
use role sysadmin;
-- 使うWHはCOMPUTE_WH
use warehouse compute_wh;

-- DB作成
create database ff11db;
create schema CHALLENGE_11;

--ファイルフォーマット作成
create or replace file format MY_CSV_FORMAT
type = csv
skip_header = 1;

-- S3外部ステージ作成
create stage week_11_frosty_stage
    url = 's3://frostyfridaychallenges/challenge_11/'
    file_format = MY_CSV_FORMAT;

    
--外部ステージ確認
list @week_11_frosty_stage;
    
-- CTASでテーブル作成
create or replace table week11 as
select 
    m.$1 as milking_datetime,
    m.$2 as cow_number,
    m.$3 as fat_percentage,
    m.$4 as farm_code,
    m.$5 as centrifuge_start_time,
    m.$6 as centrifuge_end_time,
    m.$7 as centrifuge_kwph,
    m.$8 as centrifuge_electricity_used,
    m.$9 as centrifuge_processing_time,
    m.$10 as task_used 
from @week_11_frosty_stage (file_format => 'MY_CSV_FORMAT', pattern => '.*milk_data.*[.]csv') m;

--ロードしたデータの確認
select * from week11 limit 10;

-- 乳脂肪率見てみましょう(3%,2%,1%)が存在する
select fat_percentage from week11 group by all;


-- TASK 1: Remove all the centrifuge dates and centrifuge kwph and replace them with NULLs WHERE fat = 3.
--         乳脂肪率が3%の行はすべての遠心日付と遠心kwphを削除し、NULLに置き換える　→脱脂乳にしない？
--         centrifuge:遠心分離

/*
CRONの設定方法
https://docs.snowflake.com/ja/sql-reference/sql/create-task
# __________ minute (0-59)
# | ________ hour (0-23)
# | | ______ day of month (1-31, or L)
# | | | ____ month (1-12, JAN-DEC)
# | | | | _ day of week (0-6, SUN-SAT, or L)
# | | | | |
# | | | | |
  * * * * *
* ・・・ワイルドカード。フィールドのオカレンスを指定します。
L・・・最後」の略。曜日フィールドで使用すると、特定の月の「最後の金曜日」（「5L」）などの構造を指定できます。月の日フィールドでは、月の最後の日を指定します。
SCHEDULE = 'USING CRON 0 0 10-20 * TUE,THU UTC' は、月の10日から20日、およびそれらの日付以外の火曜日または木曜日に、 0AM でタスクをスケジュールします。
*/

create or replace task whole_milk_updates_3
    warehouse = 'COMPUTE_WH'
    schedule = 'USING CRON 0 */1 * * * Asia/Tokyo'   -- 1時間に1回実行、毎時0分
    timezone = 'Asia/Tokyo'
    user_task_timeout_ms = 5400000    -- タスクがタイムアウトするまでの1回の実行の制限時間（ミリ秒単位）デフォルト: 3600000 （1時間）
    task_auto_retry_attempts = 5  --タスクグラフの自動再試行回数を指定します。タスクグラフが FAILED の状態で完了した場合
    comment = '3%の場合のタスク'
 
as
    update week11
set 
    centrifuge_start_time = NULL,
    centrifuge_end_time = NULL,
    centrifuge_kwph = NULL,
    centrifuge_electricity_used = NULL,
    centrifuge_processing_time = NULL,
    task_used = system$current_user_task_name()|| 'at' ||TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')
where fat_percentage = 3;

-- taskを見てみましょう
show tasks;


-- TASK 2: Calculate centrifuge processing time (difference between start and end time) WHERE fat != 3. 
　--         3%ではない場合、脱脂乳になる。遠心分離機の処理時間（開始時間と終了時間の差）を計算する

create or replace task skim_milk_updates_1or2
    warehouse = 'COMPUTE_WH'
    -- schedule = 'USING CRON 0 */1 * * * Asia/Tokyo'   -- こちらの設定は子タスクは設定できない。開始時間は親タスクに依存する
     timezone = 'Asia/Tokyo'
    user_task_timeout_ms = 5400000    -- タスクがタイムアウトするまでの1回の実行の制限時間（ミリ秒単位）デフォルト: 3600000 （1時間）
    --task_auto_retry_attempts = 5  --こちらの設定は「タスクグラフ」についての設定であるため、親タスクに依存する
    comment = '3%ではない場合のタスク'
    after whole_milk_updates_3   -- TASK1の後に実行するように設定。これで前後関係ができる
as
    update week11
    set
    centrifuge_processing_time= datediff('MINUTE', centrifuge_start_time, centrifuge_end_time),
    centrifuge_electricity_used = round(((datediff('minute',CENTRIFUGE_START_TIME,CENTRIFUGE_END_TIME)/60) * CENTRIFUGE_KWPH),2),
    task_used = system$current_user_task_name()|| 'at' ||to_char(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS') --タスクによって定義されたステートメントまたはストアドプロシージャから呼び出されたときに、現在実行されているタスクの名前を返します。
    where    
    fat_percentage != 3;

-- taskを見てみましょう
show tasks;

-- check
-- Task名がUpdateされていればOK
select task_used, count(*) as row_count from week11 group by task_used;

--SnowsightでGUIでも確認しましょう
--GUIで一部編集もできます
--FF11DB>Challenge11>Task

-- タスクを再開します（子タスクからresumeしないとなので一度親タスクを停止します）
alter task whole_milk_updates_3  suspend;
alter task skim_milk_updates_1or2 resume;
alter task whole_milk_updates_3  resume;

-- taskを見てみましょう
SHOW TASKS;

-- タスク１を実行します
execute task whole_milk_updates_3;

-- check
-- Task名がUpdateされていればOK
select task_used, count(*) as row_count from week11 group by task_used;

-- check
-- 3%とそれ以外のデータが更新されているか確認
select FAT_PERCENTAGE,
       centrifuge_processing_time,
       centrifuge_electricity_used,
       task_used
       from week11 where FAT_PERCENTAGE = 3;

select FAT_PERCENTAGE,
       centrifuge_processing_time,
       centrifuge_electricity_used,
       task_used
       from week11 where FAT_PERCENTAGE != 3;


-- Check task history
select *
from table(information_schema.task_history())
order by scheduled_time desc;

--　タスクを停止
alter task whole_milk_updates_3 suspend;
alter task skim_milk_updates_1or2 suspend;

--view task
show tasks; 





------------------------------------------------------
----パターン２：サーバレスタスクを使う
------------------------------------------------------


-- CTASでテーブルデータを洗い替え

create or replace table week11 as
select 
    m.$1 as milking_datetime,
    m.$2 as cow_number,
    m.$3 as fat_percentage,
    m.$4 as farm_code,
    m.$5 as centrifuge_start_time,
    m.$6 as centrifuge_end_time,
    m.$7 as centrifuge_kwph,
    m.$8 as centrifuge_electricity_used,
    m.$9 as centrifuge_processing_time,
    m.$10 as task_used 
from @week_11_frosty_stage (file_format => 'MY_CSV_FORMAT', pattern => '.*milk_data.*[.]csv') m;

-- check
-- Task名がUpdateされていればOK
select task_used, count(*) as row_count from week11 group by task_used;


-- TASK1:Warehouseをサーバレスに
create or replace task whole_milk_updates_3
    --warehouse = 'COMPUTE_WH'   --ここを書かなければサーバレスになる
     user_task_managed_initial_warehouse_size = 'X-Small'   -- 初期サイズ。基本よしなにやってくれるけどどうしても指定したい場合
     schedule = 'USING CRON 0 */1 * * * Asia/Tokyo'   -- 1時間に1回実行、毎時0分
     timezone = 'Asia/Tokyo'
     user_task_timeout_ms = 5400000    -- タスクがタイムアウトするまでの1回の実行の制限時間（ミリ秒単位）デフォルト: 3600000 （1時間）
     task_auto_retry_attempts = 5  --タスクグラフの自動再試行回数を指定します。タスクグラフが FAILED の状態で完了した場合
     comment = '3%の場合のタスク'
 
as
    update week11
set 
    centrifuge_start_time = NULL,
    centrifuge_end_time = NULL,
    centrifuge_kwph = NULL,
    centrifuge_electricity_used = NULL,
    centrifuge_processing_time = NULL,
    task_used = system$current_user_task_name()|| 'at' ||TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS')
where fat_percentage = 3;

-- taskを見てみましょう
SHOW TASKS;


-- TASK 2: Calculate centrifuge processing time (difference between start and end time) WHERE fat != 3. 
　--         3%ではない場合、脱脂乳になる。遠心分離機の処理時間（開始時間と終了時間の差）を計算する

create or replace task skim_milk_updates_1or2
    --warehouse = 'COMPUTE_WH'   --ここを書かなければサーバレスになる
     user_task_managed_initial_warehouse_size = 'X-Small'   -- 初期サイズ。基本よしなにやってくれるけどどうしても指定したい場合
     timezone = 'Asia/Tokyo'
    user_task_timeout_ms = 5400000    -- タスクがタイムアウトするまでの1回の実行の制限時間（ミリ秒単位）デフォルト: 3600000 （1時間）
    comment = '3%ではない場合のタスク'
    after whole_milk_updates_3   -- TASK1の後に実行するように設定。これで前後関係ができる
as
    update week11
    set
    centrifuge_processing_time= datediff('MINUTE', centrifuge_start_time, centrifuge_end_time),
    centrifuge_electricity_used = round(((datediff('minute',CENTRIFUGE_START_TIME,CENTRIFUGE_END_TIME)/60) * CENTRIFUGE_KWPH),2),
    task_used = system$current_user_task_name()|| 'at' ||to_char(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS') --タスクによって定義されたステートメントまたはストアドプロシージャから呼び出されたときに、現在実行されているタスクの名前を返します。
    where    
    fat_percentage != 3;

-- taskを見てみましょう
SHOW TASKS;

--SnowsightでGUIでも確認しましょう
--GUIで一部編集もできます
--FF11DB>Challenge11>Task

-- タスクを再開します（子タスクからresumeしないとなので一度親タスクを停止します）
alter task whole_milk_updates_3  suspend;
alter task skim_milk_updates_1or2 resume;
alter task whole_milk_updates_3  resume;

-- taskを見てみましょう
SHOW TASKS;

-- タスク１を実行します
execute task whole_milk_updates_3;

-- check
-- Task名がUpdateされていればOK
select task_used, count(*) as row_count from week11 group by task_used;

-- check
-- 3%とそれ以外のデータが更新されているか確認
select FAT_PERCENTAGE,
       centrifuge_processing_time,
       centrifuge_electricity_used,
       task_used
       from week11 where FAT_PERCENTAGE = 3;

select FAT_PERCENTAGE,
       centrifuge_processing_time,
       centrifuge_electricity_used,
       task_used
       from week11 where FAT_PERCENTAGE != 3;


-- Check task history
select *
from table(information_schema.task_history())
order by scheduled_time desc;

--　タスクを停止
alter task whole_milk_updates_3 suspend;
alter task skim_milk_updates_1or2 suspend;



-- お掃除
drop database ff11db;
