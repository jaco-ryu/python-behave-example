import itertools

from behave import *

from QUERY.clickhouse import upsert_click_house_query, IMP, GCK, \
    OPEN_LISTING_FILTER_LOG_RAW_INSERT, SEARCH_LISTING_FILTER_LOG_RAW_INSERT, BRAND_FILTER_LOG_RAW_INSERT, \
    AD_ACTION_RAW_INSERT, LTA, LBA, WCK

counter = itertools.count()
next(counter)


@given('광고테스트를 위한 스키마를 생성한다.')
def create_schema(context):
    pass  # upsert_click_house_query(DDL)


@when(
    '오픈리스팅광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 노출이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 노출 row가 {row_count}회 쌓인다.')
def insert_open_listing_filter_log_raw_imp(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count
):
    for i in range(row_count):
        query = OPEN_LISTING_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, bid_price, count, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, IMP, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '오픈리스팅광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 클릭이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 클릭 row가 {row_count}회 쌓인다.')
def insert_open_listing_filter_log_raw_gck(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count
):
    for i in range(row_count):
        query = OPEN_LISTING_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, bid_price, count, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, GCK, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '서치리스팅광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 노출이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 노출 row가 {row_count}회 쌓인다.')
def insert_search_listing_filter_log_raw_imp(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count
):
    for i in range(row_count):
        query = SEARCH_LISTING_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, bid_price, count, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, IMP, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '서치리스팅광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 클릭이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 클릭 row가 {row_count}회 쌓인다.')
def insert_search_listing_filter_log_raw_gck(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count
):
    for i in range(row_count):
        query = SEARCH_LISTING_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, bid_price, count, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, GCK, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '브랜드광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 노출이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 노출 row가 {row_count}회 쌓인다.')
def insert_brand_filter_log_raw_imp(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count
):
    for i in range(row_count):
        query = BRAND_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, bid_price, count, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, IMP, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '브랜드광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 클릭이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 클릭 row가 {row_count}회 쌓인다.')
def insert_brand_filter_log_raw_gck(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count
):
    for i in range(row_count):
        query = BRAND_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, bid_price, count, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, GCK, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 상단영역 찜이 {yyyy_mm_dd_hh_mm_ss}의 시점에 {row_count}회 발생했다.')
def insert_ad_action_lta(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, row_count
):
    for i in range(row_count):
        query = AD_ACTION_RAW_INSERT.format(
            yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, LTA, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 하단영역 찜이 {yyyy_mm_dd_hh_mm_ss}의 시점에 {row_count}회 발생했다.')
def insert_ad_action_lba(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, row_count
):
    for i in range(row_count):
        query = AD_ACTION_RAW_INSERT.format(
            yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, LBA, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 매장방문이 {yyyy_mm_dd_hh_mm_ss}의 시점에 {row_count}회 발생했다.')
def insert_ad_action_visit(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, row_count
):
    for i in range(row_count):
        query = AD_ACTION_RAW_INSERT.format(
            yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, WCK, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when('과금이 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 매장방문이 {yyyy_mm_dd_hh_mm_ss}의 시점에 {row_count}회 발생했다.')
def insert_ad_payment(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, price, row_count
):
    for i in range(row_count):

    pass


@when('소재단위리포트 데이터가 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 매장방문이 {yyyy_mm_dd_hh_mm_ss}의 시점에 {row_count}회 발생했다.')
def insert_ad_payment_by_creative(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, price, row_count
):
    for i in range(row_count):

    pass


@then('테스트 데이터 생성이 완료되었다.')
def step_impl(context):
    # for item in Client('localhost').execute('SHOW DATABASES'):
    #    sys.stdout.write("stdout:%s;\n" % item)
    assert context.failed is False
