import clickhouse_driver
from clickhouse_driver import Client
import json
from datetime import datetime

TARGET_URL = "localhost"

DDL = """
DROP DATABASE IF EXISTS dev_beluga;
DROP DATABASE IF EXISTS dev_mysql_service_dm;
DROP TABLE IF EXISTS dev_mysql_service_dm.brand_ad_conversion;
DROP TABLE IF EXISTS dev_beluga.ad_action;
DROP TABLE IF EXISTS dev_beluga.ad_payment;
DROP TABLE IF EXISTS dev_beluga.ad_payment_by_creative;
DROP TABLE IF EXISTS dev_beluga.brand_filter_log_raw;
DROP TABLE IF EXISTS dev_beluga.open_listing_filter_log_raw;
DROP TABLE IF EXISTS dev_beluga.search_listing_filter_log_raw;

CREATE DATABASE IF NOT EXISTS dev_beluga;
CREATE DATABASE IF NOT EXISTS dev_mysql_service_dm;

-- auto-generated definition
create table dev_mysql_service_dm.brand_ad_conversion
(
    idx          Int64,
    ws_idx       Int32,
    creative_idx Int32,
    like_count Nullable(Int32),
    ws_visit_count Nullable(Int32),
    trade_request_count Nullable(Int32),
    viewer_count Nullable(Int32),
    dt Nullable(DateTime),
    updated_at   DateTime,
    created_at   DateTime
)
    engine = MergeTree() ORDER BY idx;

-- auto-generated definition
create table IF NOT EXISTS dev_beluga.ad_action
(
    datetime Nullable(String),
    userid Nullable(String),
    platform Nullable(String),
    osversion Nullable(String),
    appversion Nullable(String),
    screenname Nullable(String),
    screenlabel Nullable(String),
    referrer Nullable(String),
    referrerlabel Nullable(String),
    accountid Nullable(String),
    bidprice Nullable(String),
    campaignidx UInt32,
    chargingtype Nullable(String),
    event       String,
    groupidx    UInt32,
    productidx  UInt32,
    selectiongroupid Nullable(String),
    selectionid Nullable(String),
    selectiontime Nullable(String),
    wsidx       UInt32,
    timestamp   UInt64,
    unitidx     UInt32,
    creativeidx UInt32,
    pageidx     UInt32,
    query Nullable(String),
    keywordidx Nullable(String),
    usertype Nullable(String),
    rsidx Nullable(String),
    exposuretime Nullable(String),
    uuid Nullable(String),
    dt          Date,
    guid        UUID                   default generateUUIDv4(),
    consumed_at DateTime('Asia/Seoul') default toDateTime(now(), 'Asia/Seoul')
    ) ENGINE = MergeTree() ORDER BY wsidx;

-- auto-generated definition
create table dev_beluga.ad_payment
(
    idx Nullable(UInt64),
    id Nullable(String),
    account_id Nullable(String),
    campaign_idx Nullable(UInt64),
    group_idx Nullable(UInt64),
    product_idx      UInt64,
    ws_idx           UInt64,
    total_amount     Decimal(18, 2),
    total_amount_vat Decimal(18, 2),
    total_payment Nullable(Int64),
    is_payment Nullable(UInt8),
    is_complete Nullable(UInt8),
    method Nullable(String),
    s3_bucket_key Nullable(String),
    window_start     DateTime('Asia/Seoul'),
    window_end Nullable(DateTime('Asia/Seoul')),
    created_at Nullable(DateTime('Asia/Seoul')),
    updated_at Nullable(DateTime('Asia/Seoul')),
    pay_balance Nullable(Int64),
    pay_date Nullable(DateTime('Asia/Seoul')),
    pay_message Nullable(String),
    pay_paid_amount Nullable(UInt64),
    pay_free_amount Nullable(UInt64),
    guid             UUID                   default generateUUIDv4(),
    consumed_at      DateTime('Asia/Seoul') default toDateTime(now(), 'Asia/Seoul')
) ENGINE = MergeTree() ORDER BY ws_idx;

-- auto-generated definition
create table dev_beluga.ad_payment_by_creative
(
    idx Nullable(Int32),
    ws_idx       UInt32,
    account_id Nullable(String),
    campaign_idx Nullable(UInt32),
    product_idx  UInt32,
    group_idx Nullable(UInt32),
    creative_idx Nullable(UInt32),
    window_start DateTime('Asia/Seoul'),
    window_end Nullable(DateTime('Asia/Seoul')),
    charging_type Nullable(String),
    amount       Decimal(10, 2)         default 0,
    vat          Decimal(10, 2)         default 0,
    vat_amount   Decimal(10, 2)         default 0,
    created_at Nullable(DateTime('Asia/Seoul')),
    keyword_idx Nullable(UInt32),
    query Nullable(String),
    guid         UUID                   default generateUUIDv4(),
    consumed_at  DateTime('Asia/Seoul') default toDateTime(now(), 'Asia/Seoul')
) ENGINE = MergeTree() ORDER BY ws_idx;

-- auto-generated definition
create table dev_beluga.brand_filter_log_raw
(
    idx Nullable(UInt32),
    ad_payment_raw_idx Nullable(UInt32),
    account_id Nullable(String),
    campaign_idx Nullable(UInt32),
    group_idx Nullable(UInt32),
    ws_idx       UInt32,
    product_idx Nullable(UInt32),
    unit_idx Nullable(UInt32),
    page_idx Nullable(UInt32),
    platform Nullable(String),
    os_version Nullable(String),
    app_version Nullable(String),
    creative_idx Nullable(UInt32),
    bid_price Nullable(Decimal(9, 2)),
    count Nullable(UInt32),
    created_at Nullable(DateTime('Asia/Seoul')),
    updated_at Nullable(DateTime('Asia/Seoul')),
    selection_id Nullable(String),
    event        String,
    rs_idx Nullable(UInt32),
    user_id Nullable(String),
    window_start DateTime('Asia/Seoul'),
    window_end Nullable(DateTime('Asia/Seoul')),
    query Nullable(String),
    charging_type Nullable(String),
    filter_code Nullable(String),
    filtered_at Nullable(DateTime('Asia/Seoul')),
    keyword_idx Nullable(UInt32),
    cd_idx Nullable(UInt32),
    guid         UUID                   default generateUUIDv4(),
    consumed_at  DateTime('Asia/Seoul') default toDateTime(now(), 'Asia/Seoul')
) ENGINE = MergeTree() ORDER BY ws_idx;

-- auto-generated definition
create table dev_beluga.open_listing_filter_log_raw
(
    idx Nullable(UInt32),
    ad_payment_raw_idx Nullable(UInt32),
    account_id Nullable(String),
    campaign_idx Nullable(UInt32),
    group_idx Nullable(UInt32),
    ws_idx       UInt32,
    product_idx Nullable(UInt32),
    unit_idx Nullable(UInt32),
    page_idx Nullable(UInt32),
    platform Nullable(String),
    os_version Nullable(String),
    app_version Nullable(String),
    creative_idx Nullable(UInt32),
    bid_price Nullable(Decimal(9, 2)),
    count Nullable(UInt32),
    created_at Nullable(DateTime('Asia/Seoul')),
    updated_at Nullable(DateTime('Asia/Seoul')),
    selection_id Nullable(String),
    event        String,
    rs_idx Nullable(UInt32),
    user_id Nullable(String),
    window_start DateTime('Asia/Seoul'),
    window_end Nullable(DateTime('Asia/Seoul')),
    charging_type Nullable(String),
    filter_code Nullable(String),
    filtered_at Nullable(DateTime('Asia/Seoul')),
    cd_idx Nullable(UInt32),
    guid         UUID                   default generateUUIDv4(),
    consumed_at  DateTime('Asia/Seoul') default toDateTime(now(), 'Asia/Seoul')
) ENGINE = MergeTree() ORDER BY ws_idx;

-- auto-generated definition
create table dev_beluga.search_listing_filter_log_raw
(
    idx Nullable(UInt32),
    ad_payment_raw_idx Nullable(UInt32),
    account_id Nullable(String),
    campaign_idx Nullable(UInt32),
    group_idx Nullable(UInt32),
    ws_idx       UInt32,
    product_idx Nullable(UInt32),
    unit_idx Nullable(UInt32),
    page_idx Nullable(UInt32),
    platform Nullable(String),
    os_version Nullable(String),
    app_version Nullable(String),
    creative_idx Nullable(UInt32),
    bid_price Nullable(Decimal(9, 2)),
    count Nullable(UInt32),
    created_at Nullable(DateTime('Asia/Seoul')),
    updated_at Nullable(DateTime('Asia/Seoul')),
    selection_id Nullable(String),
    event        String,
    rs_idx Nullable(UInt32),
    user_id Nullable(String),
    window_start DateTime('Asia/Seoul'),
    window_end Nullable(DateTime('Asia/Seoul')),
    query Nullable(String),
    charging_type Nullable(String),
    filter_code Nullable(String),
    filtered_at Nullable(DateTime('Asia/Seoul')),
    keyword_idx Nullable(UInt32),
    cd_idx Nullable(UInt32),
    guid         UUID                   default generateUUIDv4(),
    consumed_at  DateTime('Asia/Seoul') default toDateTime(now(), 'Asia/Seoul')
) ENGINE = MergeTree() ORDER BY ws_idx;

DROP DATABASE IF EXISTS total_summation_raw_mapper_test_beluga;
DROP TABLE IF EXISTS total_summation_raw_mapper_test_beluga.ad_payment;

CREATE DATABASE IF NOT EXISTS total_summation_raw_mapper_test_beluga;

-- FOR_TotalSummationRawMapperTest_START
    -- auto-generated definition
create table total_summation_raw_mapper_test_beluga.ad_payment
(
    idx Nullable(UInt64),
    id Nullable(String),
    account_id Nullable(String),
    campaign_idx Nullable(UInt64),
    group_idx Nullable(UInt64),
    product_idx      UInt64,
    ws_idx           UInt64,
    total_amount     Decimal(18, 2),
    total_amount_vat Decimal(18, 2),
    total_payment Nullable(Int64),
    is_payment Nullable(UInt8),
    is_complete Nullable(UInt8),
    method Nullable(String),
    s3_bucket_key Nullable(String),
    window_start     DateTime('Asia/Seoul'),
    window_end Nullable(DateTime('Asia/Seoul')),
    created_at Nullable(DateTime('Asia/Seoul')),
    updated_at Nullable(DateTime('Asia/Seoul')),
    pay_balance Nullable(Int64),
    pay_date Nullable(DateTime('Asia/Seoul')),
    pay_message Nullable(String),
    pay_paid_amount Nullable(UInt64),
    pay_free_amount Nullable(UInt64),
    guid             UUID                   default generateUUIDv4(),
    consumed_at      DateTime('Asia/Seoul') default toDateTime(now(), 'Asia/Seoul')
) ENGINE = MergeTree() ORDER BY ws_idx;
    """

IMP = "IMP"
GCK = "GCK"
LTA = "LTA"
LBA = "LBA"
WCK = "WCK"

CPC = "CPC"
CPM = "CPM"


def upsert_click_house_query(query):
    Client(TARGET_URL).execute(query)


OPEN_LISTING_FILTER_LOG_RAW_INSERT = """INSERT INTO dev_beluga.open_listing_filter_log_raw (
idx, 
ad_payment_raw_idx, 
account_id, 
campaign_idx, 
group_idx, 
ws_idx, 
product_idx, 
unit_idx, 
page_idx, 
platform, 
os_version, 
app_version, 
creative_idx, 
bid_price, 
count, 
created_at, 
updated_at, 
selection_id, 
event, 
rs_idx, 
user_id, 
window_start, 
window_end, 
charging_type, 
filter_code, 
filtered_at, 
cd_idx, 
guid, 
consumed_at
) VALUES (
{}, 
148840, 
'5e09f986-67d8-49c4-a219-beea5b5d9865', 
262, 
262, 
{}, 
1, 
81, 
4, 
'ANDROID', 
'9', 
'4.29.1', 
{}, 
{}, 
{}, 
'{}', 
'{}', 
'ae5cd882-5ded-4fd6-b92b-b8dd073350b6', 
'{}', 
174324, 
'sooki', 
'{}',  
'{}',  
'CPM', 
'none', 
'{}',  
49, 
'fc177402-0f2d-44b7-8dc7-2f957762725e', 
'{}'
);"""

SEARCH_LISTING_FILTER_LOG_RAW_INSERT = """INSERT INTO dev_beluga.search_listing_filter_log_raw (
idx, 
ad_payment_raw_idx, 
account_id, 
campaign_idx, 
group_idx, 
ws_idx, 
product_idx, 
unit_idx, 
page_idx, 
platform, 
os_version, 
app_version, 
creative_idx, 
bid_price, 
count, 
created_at, 
updated_at, 
selection_id, 
event, 
rs_idx, 
user_id, 
window_start, 
window_end, 
query, 
charging_type, 
filter_code, 
filtered_at, 
keyword_idx, 
cd_idx, 
guid, 
consumed_at
) VALUES (
{}, 
0, 
'eb60ff53-c46b-452d-9061-44e2df00912b', 
87, 
87, 
{}, 
2, 
102, 
8, 
'IOS', 
'15.3.1', 
'4.30.0', 
{}, 
{}, 
{}, 
'{}', 
'{}', 
'f08b0bc8-d759-4289-a5be-cba190ba60b2', 
'{}', 
277048, 
'yoonj_22', 
'{}', 
'{}', 
'', 
'CPC', 
'notPayment', 
'{}', 
5756, 
0, 
'd6fad3ec-50fb-4f83-abfe-9bb308cf7f96', 
'{}'
);
"""

BRAND_FILTER_LOG_RAW_INSERT = """INSERT INTO dev_beluga.brand_filter_log_raw (
idx, 
ad_payment_raw_idx, 
account_id, 
campaign_idx, 
group_idx, 
ws_idx, 
product_idx, 
unit_idx, 
page_idx, 
platform, 
os_version, 
app_version, 
creative_idx, 
bid_price, 
count, 
created_at, 
updated_at, 
selection_id, 
event, 
rs_idx, 
user_id, 
window_start, 
window_end, 
query, 
charging_type, 
filter_code, 
filtered_at, 
keyword_idx, 
cd_idx, 
guid, 
consumed_at
) VALUES (
{}, 
0, 
'91c61b91-7034-47b7-9b3c-c994a4c01a26', 
221, 
221, 
{}, 
4, 
157, 
12, 
'IOS', 
'15.3.1', 
'4.30.0', 
{}, 
{}, 
{},  
'{}', 
'{}', 
'9d7db646-3b40-4d98-8346-c7a2d4018e35', 
'{}', 
174324, 
'somenamjun', 
'{}', 
'{}', 
'', 
'CPC', 
'notPayment', 
'2022-03-15 16:17:16', 
0, 
0, 
'1fe85260-90d3-4661-a4c3-0e8015529d9d', 
'{}'
);
"""

AD_ACTION_RAW_INSERT = """INSERT INTO dev_beluga.ad_action (
datetime, 
userid, 
platform, 
osversion, 
appversion, 
screenname, 
screenlabel, 
referrer, 
referrerlabel, 
accountid, 
bidprice, 
campaignidx, 
chargingtype, 
event, 
groupidx, 
productidx, 
selectiongroupid, 
selectionid, 
selectiontime, 
wsidx, 
timestamp, 
unitidx, 
creativeidx, 
pageidx, 
query, 
keywordidx, 
usertype, 
rsidx, 
exposuretime, 
uuid, 
dt, 
guid, 
consumed_at
) VALUES (
'{}', 
'woongretailceo', 
'ANDROID', 
'10', 
'4.32.0', 
'소매_거래처목록_추천거래처', 
'CLR', 
'소매_상품목록_전체신상', 
'TTN', 
'eb60ff53-c46b-452d-9061-44e2df00912b', 
'0445ebf0ed8dc87b3f6eb3119ab90a6568d6e80295676632f7eb8024ffc136ca', 
132, 
'{}', 
'{}', 
132, 
4, 
'f2d3fc36-c71b-424b-82f2-a42c1e4b2e69', 
'660321f7-fbb7-4c65-a94e-9526762cd466', 
'{}', 
{}, 
1649640767882, 
125, 
{}, 
11, 
null, 
null, 
'R', 
'277343', 
'1649640753049', 
'0fcee104-fddc-3520-8bb9-b01dca5ebe35', 
'2022-04-11', 
'652881da-7599-4e9c-b5c0-9e494c67a68f', 
'{}'
);
"""

AD_PAYMENT_RAW_INSERT = """INSERT INTO dev_beluga.ad_payment (
idx, 
id, 
account_id, 
campaign_idx, 
group_idx, 
product_idx, 
ws_idx, 
total_amount, 
total_amount_vat, 
total_payment, 
is_payment, 
is_complete, 
method, 
s3_bucket_key, 
window_start, 
window_end, 
created_at, 
updated_at, 
pay_balance, 
pay_date, 
pay_message, 
pay_paid_amount, 
pay_free_amount, 
guid, 
consumed_at
) VALUES (
{}, 
'882a4199-8faf-4239-a816-517d913a314b', 
'087c9def-5039-43bb-a5b5-0212e3b55f16', 
67, 
67, 
{}, 
{}, 
2.00, 
0.00, 
{}, 
1, 
1, 
'AUTO', 
'', 
'{}', 
'{}', 
'{}', 
'{}', 
99798, 
'{}',  
'[오픈리스팅] 광고 사용', 
{}, 
{}, 
'28499328-3d57-4cd3-a350-66492f724006', 
'{}'
);
"""

AD_PAYMENT_CREATIVE_RAW_INSERT = """INSERT INTO dev_beluga.ad_payment_by_creative (
idx, 
ws_idx, 
account_id, 
campaign_idx, 
product_idx, 
group_idx, 
creative_idx, 
window_start, 
window_end, 
charging_type, 
amount, 
vat, 
vat_amount, 
created_at, 
keyword_idx, 
query, 
guid, 
consumed_at
) VALUES (
{}, 
{}, 
'087c9def-5039-43bb-a5b5-0212e3b55f16', 
67, 
{}, 
67, 
{}, 
'{}',
'{}', 
'CPM', 
2.00, 
0.20, 
{}, 
'{}', 
0, 
'null', 
'2336fdeb-cd5e-436e-aa18-3df05cd28a3d', 
'{}'
);
"""


def read_json_and_execute_query(query, path):
    def load_with_datetime(pairs, format='%Y-%m-%d %H:%M:%S'):
        """Load with dates"""
        d = {}
        for k, v in pairs:
            if isinstance(v, str):
                try:
                    d[k] = datetime.strptime(v, format)
                except ValueError:
                    d[k] = v
            else:
                d[k] = v
        return d

    Client(TARGET_URL).execute(
        query,
        json.load(
            fp=open(path),
            object_pairs_hook=load_with_datetime
        )
    )


def init_ad_payment_average_data():
    query = """insert into dev_beluga.ad_payment (idx, id, account_id, campaign_idx, group_idx, product_idx, ws_idx, total_amount, total_amount_vat, total_payment, is_payment, is_complete, method, s3_bucket_key, window_start, window_end, created_at, updated_at, pay_balance, pay_date, pay_message, pay_paid_amount, pay_free_amount, guid, consumed_at) values"""

    read_json_and_execute_query(query, "./QUERY/ad_payment1.json")  # 오픈리스팅
    read_json_and_execute_query(query, "./QUERY/ad_payment2.json")  # 서치리스팅
    read_json_and_execute_query(query, "./QUERY/ad_payment4.json")  # 브랜드


def init_openlisting_average_data():
    query = """insert into dev_beluga.open_listing_filter_log_raw (idx, ad_payment_raw_idx, account_id, campaign_idx, group_idx, ws_idx, product_idx, unit_idx, page_idx, platform, os_version, app_version, creative_idx, bid_price, count, created_at, updated_at, selection_id, event, rs_idx, user_id, window_start, window_end, charging_type, filter_code, filtered_at, cd_idx, guid, consumed_at) values"""

    read_json_and_execute_query(query, "./QUERY/openlisting_gck.json")
    read_json_and_execute_query(query, "./QUERY/openlisting_imp.json")


def init_searchlisting_average_data():
    query = """INSERT INTO dev_beluga.search_listing_filter_log_raw (idx, ad_payment_raw_idx, account_id, campaign_idx, group_idx, ws_idx, product_idx, unit_idx, page_idx, platform, os_version, app_version, creative_idx, bid_price, count, created_at, updated_at, selection_id, event, rs_idx, user_id, window_start, window_end, query, charging_type, filter_code, filtered_at, keyword_idx, cd_idx, guid, consumed_at) VALUES"""

    read_json_and_execute_query(query, "./QUERY/searchlisting_gck.json")
    read_json_and_execute_query(query, "./QUERY/searchlisting_imp.json")


def init_brand_average_data():
    query = """INSERT INTO dev_beluga.brand_filter_log_raw (idx, ad_payment_raw_idx, account_id, campaign_idx, group_idx, ws_idx, product_idx, unit_idx, page_idx, platform, os_version, app_version, creative_idx, bid_price, count, created_at, updated_at, selection_id, event, rs_idx, user_id, window_start, window_end, query, charging_type, filter_code, filtered_at, keyword_idx, cd_idx, guid, consumed_at) VALUES"""

    read_json_and_execute_query(query, "./QUERY/brand_gck.json")
    read_json_and_execute_query(query, "./QUERY/brand_imp.json")


def correct_ad_payment_average_data(
        start_datetime_str,
        end_datetime_str
):
    correct_ad_payment_average_data_open_listing(start_datetime_str, end_datetime_str)
    correct_ad_payment_average_data_search_listing(start_datetime_str, end_datetime_str)
    correct_ad_payment_average_data_brand_listing(start_datetime_str, end_datetime_str)


def correct_ad_payment_average_data_open_listing(
        start_datetime_str,
        end_datetime_str
):
    correct_ad_payment_average_data_internal(
        product_idx=1,
        start_datetime_str=start_datetime_str,
        end_datetime_str=end_datetime_str
    )


def correct_ad_payment_average_data_search_listing(
        start_datetime_str,
        end_datetime_str
):
    correct_ad_payment_average_data_internal(
        product_idx=2,
        start_datetime_str=start_datetime_str,
        end_datetime_str=end_datetime_str
    )


def correct_ad_payment_average_data_brand_listing(
        start_datetime_str,
        end_datetime_str
):
    correct_ad_payment_average_data_internal(4, start_datetime_str, end_datetime_str)


def correct_ad_payment_average_data_internal(
        product_idx,
        start_datetime_str,
        end_datetime_str
):
    target_average_amount = 30000
    insert_query = """insert into dev_beluga.ad_payment (idx, id, account_id, campaign_idx, group_idx, product_idx, ws_idx, total_amount, total_amount_vat, total_payment, is_payment, is_complete, method, s3_bucket_key, window_start, window_end, created_at, updated_at, pay_balance, pay_date, pay_message, pay_paid_amount, pay_free_amount, guid, consumed_at) values"""

    payment_data = Client(TARGET_URL).execute(
        f"""SELECT SUM(pay_paid_amount + pay_free_amount) total_payment, COUNT(*) count_of_payment, MAX(idx) max_idx FROM dev_beluga.ad_payment WHERE product_idx = {product_idx} AND created_at >= '{start_datetime_str}' and created_at <= '{end_datetime_str}'"""
    )
    total_payment = payment_data[0][0]
    count_of_payment = payment_data[0][1]
    max_idx = payment_data[0][2]
    parsed_datetime = datetime.fromisoformat(start_datetime_str)

    amount_to_add = target_average_amount * (count_of_payment + 1) - total_payment
    if amount_to_add < 0:
        return
    Client(TARGET_URL).execute(
        insert_query,
        [{"idx": max_idx + 10,
          "id": "882a4199-8faf-4239-a816-517d913a314b",
          "account_id": "087c9def-5039-43bb-a5b5-0212e3b55f16",
          "campaign_idx": 67,
          "group_idx": 67,
          "product_idx": product_idx,
          "ws_idx": 1,
          "total_amount": 2.0,
          "total_amount_vat": 0.0,
          "total_payment": amount_to_add,
          "is_payment": True,
          "is_complete": True,
          "method": "AUTO", "s3_bucket_key": "",
          "window_start": parsed_datetime,
          "window_end": parsed_datetime,
          "created_at": parsed_datetime,
          "updated_at": parsed_datetime,
          "pay_balance": 99798,
          "pay_date": parsed_datetime,
          "pay_message": "[오픈리스팅] 광고 사용",
          "pay_paid_amount": amount_to_add,
          "pay_free_amount": 0,
          "guid": "28499328-3d57-4cd3-a350-66492f724006",
          "consumed_at": parsed_datetime
          }],
            types_check=True
    )

