import itertools

from behave import *

from MODEL.AdAmount import AdAmount

from REPOSITORY.clickhouse import execute_click_house_multi_line_ddl, upsert_click_house_query, \
    select_one_click_house_query

from QUERY.clickhouse import IMP, GCK, DDL, \
    OPEN_LISTING_FILTER_LOG_RAW_INSERT, SEARCH_LISTING_FILTER_LOG_RAW_INSERT, BRAND_FILTER_LOG_RAW_INSERT, \
    AD_ACTION_RAW_INSERT, AD_PAYMENT_CREATIVE_RAW_INSERT, AD_PAYMENT_RAW_INSERT, \
    SELECT_CLICK_EXPOSE_COUNT, SELECT_LIKE_VISIT_COUNT, \
    SELECT_AMOUNT_TOTAL_BY_WS_IDX, SELECT_AMOUNT_BY_WS_IDX_AND_PRODUCT_IDX, \
    LTA, LBA, WCK, CPC, CPM

counter = itertools.count()
next(counter)


@given('광고테스트를 위한 스키마를 생성한다.')
def clear_and_create_schema(context):
    execute_click_house_multi_line_ddl(DDL)
    pass


@given('광고테스트를 위한 기존 스키마를 그대로 이용한다.')
def do_nothing_schema(context):
    pass


@when(
    '오픈리스팅광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 노출이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 노출 row가 {row_count:d}회 쌓인다.')
def insert_open_listing_filter_log_raw_imp(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count: int
):
    for i in range(row_count):
        query = OPEN_LISTING_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, creative_idx, bid_price, count, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, IMP, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '오픈리스팅광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 클릭이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 클릭 row가 {row_count:d}회 쌓인다.')
def insert_open_listing_filter_log_raw_gck(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count: int
):
    for i in range(row_count):
        query = OPEN_LISTING_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, creative_idx, bid_price, count, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, GCK, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '서치리스팅광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 노출이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 노출 row가 {row_count:d}회 쌓인다.')
def insert_search_listing_filter_log_raw_imp(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count: int
):
    for i in range(row_count):
        query = SEARCH_LISTING_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, creative_idx, bid_price, count, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, IMP, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '서치리스팅광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 클릭이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 클릭 row가 {row_count:d}회 쌓인다.')
def insert_search_listing_filter_log_raw_gck(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count: int
):
    for i in range(row_count):
        query = SEARCH_LISTING_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, creative_idx, bid_price, count, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, GCK, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '브랜드광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 노출이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 노출 row가 {row_count:d}회 쌓인다.')
def insert_brand_filter_log_raw_imp(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count: int
):
    for i in range(row_count):
        query = BRAND_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, creative_idx, bid_price, count, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, IMP, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '브랜드광고로 wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 클릭이 {yyyy_mm_dd_hh_mm_ss}의 시점에 비딩가가 {bid_price}원인 {count}번이 들어간 하나의 클릭 row가 {row_count:d}회 쌓인다.')
def insert_brand_filter_log_raw_gck(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, bid_price, count, row_count: int
):
    for i in range(row_count):
        query = BRAND_FILTER_LOG_RAW_INSERT.format(
            next(counter), ws_idx, creative_idx, bid_price, count, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, GCK, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 상단영역 찜이 {yyyy_mm_dd_hh_mm_ss}의 시점에 {row_count:d}회 발생했다.')
def insert_ad_action_lta(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, row_count: int
):
    for i in range(row_count):
        query = AD_ACTION_RAW_INSERT.format(
            yyyy_mm_dd_hh_mm_ss, CPC, LTA, yyyy_mm_dd_hh_mm_ss, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 하단영역 찜이 {yyyy_mm_dd_hh_mm_ss}의 시점에 {row_count:d}회 발생했다.')
def insert_ad_action_lba(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, row_count: int
):
    for i in range(row_count):
        query = AD_ACTION_RAW_INSERT.format(
            yyyy_mm_dd_hh_mm_ss, CPC, LBA, yyyy_mm_dd_hh_mm_ss, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 매장방문이 {yyyy_mm_dd_hh_mm_ss}의 시점에 {row_count:d}회 발생했다.')
def insert_ad_action_visit(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, row_count: int
):
    for i in range(row_count):
        query = AD_ACTION_RAW_INSERT.format(
            yyyy_mm_dd_hh_mm_ss, CPC, WCK, yyyy_mm_dd_hh_mm_ss, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '과금이 wsIdx가 {ws_idx}인 광고주에 광고상품이 {product_idx}으로 {yyyy_mm_dd_hh_mm_ss}의 시점에 무상으로 {price:d}원 {row_count:d}회 발생했다.')
def insert_ad_payment(
        context, ws_idx, product_idx, yyyy_mm_dd_hh_mm_ss, price: int, row_count: int
):
    for i in range(row_count):
        query = AD_PAYMENT_RAW_INSERT.format(
            next(counter), product_idx, ws_idx, price,
            yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, 0, price, yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '과금이 wsIdx가 {ws_idx}인 광고주에 광고상품이 {product_idx}으로 {yyyy_mm_dd_hh_mm_ss}의 시점에 유상으로 {price:d}원 {row_count:d}회 발생했다.')
def insert_ad_payment(
        context, ws_idx, product_idx, yyyy_mm_dd_hh_mm_ss, price: int, row_count: int
):
    for i in range(row_count):
        query = AD_PAYMENT_RAW_INSERT.format(
            next(counter), product_idx, ws_idx, price,
            yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, price, 0, yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '과금이 wsIdx가 {ws_idx}인 광고주에 광고상품이 {product_idx}으로 {yyyy_mm_dd_hh_mm_ss}의 시점에 유상으로 {paid_price:d}원, 무상으로 {free_price:d}원 {row_count:d}회 발생했다.')
def insert_ad_payment(
        context, ws_idx, product_idx, yyyy_mm_dd_hh_mm_ss, paid_price: int, free_price: int, row_count: int
):
    for i in range(row_count):
        query = AD_PAYMENT_RAW_INSERT.format(
            next(counter), product_idx, ws_idx, paid_price + free_price,
            yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss, paid_price, free_price, yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when(
    '{count:d}개의 계정으로 광고주에 광고상품이 {product_idx}으로 {yyyy_mm_dd_hh_mm_ss}의 시점에 유상으로 {price:d}원 {row_count:d}회 발생했다.')
def insert_ad_payment(
        context, count: int, product_idx, yyyy_mm_dd_hh_mm_ss, price: int, row_count: int
):
    for i in range(count):
        for k in range(row_count):
            query = AD_PAYMENT_RAW_INSERT.format(
                next(counter), product_idx, i, price,
                yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
                yyyy_mm_dd_hh_mm_ss, price, 0, yyyy_mm_dd_hh_mm_ss
            )
            upsert_click_house_query(query)
    pass


@when(
    '소재단위리포트 데이터가 wsIdx가 {ws_idx}인 광고주에 광고상품이 {product_idx}인 소재아이디가 {creative_idx}인 {yyyy_mm_dd_hh_mm_ss}의 시점에 vat포함 {price:d}원으로 {row_count:d}회 발생했다.')
def insert_ad_payment_by_creative(
        context, ws_idx, product_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, price, row_count: int
):
    for i in range(row_count):
        query = AD_PAYMENT_CREATIVE_RAW_INSERT.format(
            next(counter), ws_idx, product_idx, creative_idx,
            yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, price,
            yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@then('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 찜수는 {count}개이다.')
def check_like_count_by_ws_idx_and_creative_idx(context, ws_idx, creative_idx, count):
    result = select_one_click_house_query(SELECT_LIKE_VISIT_COUNT.format(
        ws_idx=ws_idx,
        creative_idx=creative_idx
    ))
    assert result[0][0] == int(count)

    pass


@then('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 매장방문수는 {count}개이다.')
def check_visit_count_by_ws_idx_and_creative_idx(context, ws_idx, creative_idx, count):
    result = select_one_click_house_query(SELECT_LIKE_VISIT_COUNT.format(
        ws_idx=ws_idx,
        creative_idx=creative_idx
    ))
    assert result[0][1] == int(count)

    pass


@then('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 클릭수는 {count}개이다.')
def check_click_count_by_ws_idx_and_creative_idx(context, ws_idx, creative_idx, count):
    result = select_one_click_house_query(SELECT_CLICK_EXPOSE_COUNT.format(
        ws_idx=ws_idx,
        creative_idx=creative_idx
    ))
    assert result[0][0] == int(count)

    pass


@then('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 노출수는 {count:d}개이다.')
def check_expose_count_by_ws_idx_and_creative_idx(context, ws_idx, creative_idx, count):
    pass


@then('wsIdx가 {ws_idx}인 광고주의 광고상품이 {product_idx:d}인 총 과금액은 {amount:d}원이다.')
def check_click_count_by_ws_idx_and_creative_idx(context, ws_idx, product_idx, amount):
    query = SELECT_AMOUNT_BY_WS_IDX_AND_PRODUCT_IDX.format(ws_idx, product_idx)
    result = select_one_click_house_query(query)
    ad_amount = AdAmount.getInstance(result)
    if product_idx == 1:
        assert ad_amount.openlisting_total_payment is amount
    elif product_idx == 2:
        assert ad_amount.searchlisting_total_payment is amount
    elif product_idx == 3:
        assert ad_amount.vedio_total_payment is amount
    elif product_idx == 4:
        assert ad_amount.vedio_total_payment is amount
    else:
        assert True is False


@then('wsIdx가 {ws_idx}인 광고주의 과금액은 {amount:d}원이다.')
def check_click_count_by_ws_idx_and_creative_idx(context, ws_idx, amount):
    query = SELECT_AMOUNT_TOTAL_BY_WS_IDX.format(ws_idx)
    result = select_one_click_house_query(query)
    ad_amount = AdAmount.getInstance(result)
    assert ad_amount.total_payment is amount


@then('테스트 데이터 생성이 완료되었다.')
def step_impl(context):
    # for item in Client('localhost').execute('SHOW DATABASES'):
    #    sys.stdout.write("stdout:%s;\n" % item)
    assert context.failed is False
