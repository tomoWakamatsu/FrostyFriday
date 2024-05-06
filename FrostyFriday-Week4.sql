/*  https://frostyfriday.org/blog/2022/07/15/week-4-hard/

シナリオ：
Frosty Friday Consultantsはフロスト大学の歴史学部に雇われました。彼らは分析のためにデータウェアハウスに君主に関するデータを求めています。あなたの仕事は、ここにあるJSONファイルをデータウェアハウスに取り込み、それを解析して次のようなテーブルを作成することです：

上記が読めない場合は、右クリックして別のタブで画像を表示してください。(https://frostyfriday.org/blog/2022/07/15/week-4-hard/の画像参照)

ニックネームとconsorts1～3の列は別々で入れる。多くはNULLになる。
年代順のID（誕生日）。
Inter-HouseのIDはファイルに表示されている順番で。
最後は26行になるはずです。

Frosty Friday Consultants has been hired by the University of Frost’s history department; they want data on monarchs in their data warehouse for analysis. Your job is to take the JSON file located here, ingest it into the data warehouse, and parse it into a table that looks like this:

If you can’t read the above right-click and view image in another tab.

Separate columns for nicknames and consorts 1 – 3, many will be null.
An ID in chronological order (birth).
An Inter-House ID in order as they appear in the file.
There should be 26 rows at the end.

JSONはダウンロードしておきましょう
https://frostyfridaychallenges.s3.eu-west-1.amazonaws.com/challenge_4/Spanish_Monarchs.json
*/


-- ***solution 01　ベーシックな回答*---*---*---*-
-- Created by darylkit
--https://github.com/darylkit/Frosty_Friday/blob/main/Week%204%20-%20JSON/json.sql

--環境を作る
create database if not exists frosty_friday;
create schema if not exists frosty;
drop schema if exists public;

-- RAWデータを入れるテーブルを作る（VARIANT型）
create or replace table raw_source (
  src variant);

-- 内部ステージ
CREATE STAGE Week4Stage 
	DIRECTORY = ( ENABLE = true );

-- JSONをアップロードする（GUIで実施した）
  
--S3からJSONをロードする
copy into raw_source
from '@frosty_friday.frosty.Week4Stage/spanish_monarchs.json'
file_format = (type = json);


--データの確認
select * from raw_source;

-- 要件にあったSQLを作成し、テーブルを作る
create or replace table spanish_monarchs as
 select row_number() over (order by monarchs.value:Birth::date) as ID,   --誕生日順で並べる
        row_number() over (partition by houses.value:House ORDER by monarchs.index) as INTER_HOUSE_ID,  -- monarchsのINDEX順
        src.value:Era::varchar as ERA,
        houses.value:House::varchar as HOUSE,
        monarchs.value:Name::varchar as NAME,
        monarchs.value:Nickname[0]::varchar as NICKNAME_1,  -- 3つに分割
        monarchs.value:Nickname[1]::varchar as NICKNAME_2,  -- 3つに分割
        monarchs.value:Nickname[2]::varchar as NICKNAME_3,  -- 3つに分割
        monarchs.value:Birth::date as BIRTH,
        monarchs.value:"Place of Birth"::varchar as PLACE_OF_BIRTH,
        monarchs.value:"Start of Reign"::date as START_OF_REIGN,
        coalesce( monarchs.value:"Consort\/Queen Consort"[0],monarchs.value:"Consort\/Queen Consort" )::varchar as QUEEN_OR_QUEEN_CONSORT_1,  -- (coalesceしないと１つ目が綺麗に入らない)
        --monarchs.value:"Consort\/Queen Consort"[0]::varchar as QUEEN_OR_QUEEN_CONSORT_1,    -- 👆のソースをこちらに変えて比較して確認してみよう
        monarchs.value:"Consort\/Queen Consort"[1]::varchar as QUEEN_OR_QUEEN_CONSORT_2,  -- 3つに分割
        monarchs.value:"Consort\/Queen Consort"[2]::varchar as QUEEN_OR_QUEEN_CONSORT_3,  -- 3つに分割
        monarchs.value:"End of Reign"::date as END_OF_REIGN,
        monarchs.value:Duration::varchar as DURATION,
        monarchs.value:Death::date as DEATH,
        trim(replace(lower(monarchs.value:"Age at Time of Death"),'years',''))::int as AGE_AT_THE_TIME_OF_DEATH_YEARS,
        monarchs.value:"Place of Death"::varchar as PLACE_OF_DEATH,
        monarchs.value:"Burial Place"::varchar as BURIAL_PLACE
   from raw_source,
lateral flatten( input => src ) src,  -- LATERAL 親
lateral flatten( input => src.value:Houses) houses,  -- LATERAL 子
lateral flatten( input => houses.value:Monarchs) monarchs -- LATERAL 孫
  order by id;  -- IDは誕生日順に並び替えられているのでID順にすると要件を満たす


-- 中身の確認
select * from spanish_monarchs;


-- お掃除
drop database frosty_friday;
