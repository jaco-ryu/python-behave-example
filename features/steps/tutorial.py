import itertools

from behave import *

from MODEL.AdAmount import AdAmount

from REPOSITORY.clickhouse import execute_click_house_multi_line_ddl, upsert_click_house_query, \
    select_one_click_house_query

from QUERY.clickhouse import IMP, GCK, DDL, \
    OPEN_LISTING_FILTER_LOG_RAW_INSERT, SEARCH_LISTING_FILTER_LOG_RAW_INSERT, BRAND_FILTER_LOG_RAW_INSERT, \
    AD_ACTION_RAW_INSERT, AD_PAYMENT_CREATIVE_RAW_INSERT, AD_PAYMENT_RAW_INSERT, \
    init_ad_payment_average_data, init_openlisting_average_data, init_searchlisting_average_data, \
    init_brand_average_data, \
    correct_ad_payment_average_data, correct_openlisting_average_data, correct_searchlisting_average_data, \
    correct_brand_average_data, \
    SELECT_CLICK_EXPOSE_COUNT, SELECT_LIKE_VISIT_COUNT, \
    SELECT_AMOUNT_TOTAL_BY_WS_IDX, SELECT_AMOUNT_BY_WS_IDX_AND_PRODUCT_IDX, \
    SELECT_AMOUNT_TOTAL_IN_AD_PAYMENT_BY_CREARIVE_BY_WS_IDX, \
    SELECT_AMOUNT_TOTAL_IN_AD_PAYMENT_BY_CREARIVE_BY_WS_IDX_AND_PRODUCT_IDX, \
    BRAND_AD_CONVERSION_INSERT, BRAND_AD_CONVERSION_SELECT, \
    LTA, LBA, WCK, CPC, CPM, \
    SELECT_AVERAGE_TOTAL_AMOUNT_BY_CREATED_AT, SELECT_AVERAGE_TOTAL_AMOUNT_BY_CREATED_AT_AND_PRODUCT, \
    SELECT_OPENLISTING_CLICK_RATE_BY_CREATED_AT, SELECT_SEARCHLISTING_CLICK_RATE_BY_CREATED_AT, \
    SELECT_BRAND_CLICK_RATE_BY_CREATED_AT, SELECT_CLICK_EXPOSE_COUNT_BY_WS_IDX

counter = itertools.count()
next(counter)


def assert_equals(actual, expected):
    try:
        assert actual == expected
    except AssertionError as err:
        print(f"actual: {actual}, expected: {expected}")
        raise err


@given('광고테스트를 위한 스키마를 생성한다.')
def clear_and_create_schema(context):
    execute_click_house_multi_line_ddl(DDL)
    pass


@given('광고테스트를 위한 평균 데이터를 생성한다.')
def init_average_data(context):
    init_ad_payment_average_data()
    init_openlisting_average_data()
    init_searchlisting_average_data()
    init_brand_average_data()
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
            yyyy_mm_dd_hh_mm_ss, CPC, LTA, yyyy_mm_dd_hh_mm_ss, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 하단영역 찜이 {yyyy_mm_dd_hh_mm_ss}의 시점에 {row_count:d}회 발생했다.')
def insert_ad_action_lba(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, row_count: int
):
    for i in range(row_count):
        query = AD_ACTION_RAW_INSERT.format(
            yyyy_mm_dd_hh_mm_ss, CPC, LBA, yyyy_mm_dd_hh_mm_ss, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
        )
        upsert_click_house_query(query)
    pass


@when('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 매장방문이 {yyyy_mm_dd_hh_mm_ss}의 시점에 {row_count:d}회 발생했다.')
def insert_ad_action_visit(
        context, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss, row_count: int
):
    for i in range(row_count):
        query = AD_ACTION_RAW_INSERT.format(
            yyyy_mm_dd_hh_mm_ss, CPC, WCK, yyyy_mm_dd_hh_mm_ss, ws_idx, creative_idx, yyyy_mm_dd_hh_mm_ss,
            yyyy_mm_dd_hh_mm_ss
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
    '외부전환 지표가 wsIdx가 {ws_idx}인 광고주에 {yyyy_mm_dd_hh_mm_ss}의 시점에 찜수 {like_count:d}건, 매장방문수 {ws_visit_count:d}건, 거래처 요청수 {trade_request_count:d}건, 광고 본 사용자 수 {viewer_count:d}건, {row_count:d}회 발생했다.')
def insert_brand_ad_conversion(
        context, ws_idx: int, yyyy_mm_dd_hh_mm_ss: str,
        like_count: int, ws_visit_count: int, trade_request_count: int, viewer_count: int,
        row_count: int
):
    for i in range(row_count):
        query = BRAND_AD_CONVERSION_INSERT.format(
            next(counter), ws_idx,
            like_count, ws_visit_count, trade_request_count, viewer_count,
            yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss, yyyy_mm_dd_hh_mm_ss,
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


@then('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx}인 찜수는 {count:d}개이다.')
def check_like_count_by_ws_idx_and_creative_idx(context, ws_idx, creative_idx, count):
    sql = SELECT_LIKE_VISIT_COUNT.format(
        ws_idx=ws_idx,
        creative_idx=creative_idx
    )  # print(sql)
    result = select_one_click_house_query(sql)
    assert_equals(result[0][0], count)


@then('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx:d}인 매장방문수는 {count:d}개이다.')
def check_visit_count_by_ws_idx_and_creative_idx(context, ws_idx, creative_idx, count):
    result = select_one_click_house_query(SELECT_LIKE_VISIT_COUNT.format(
        ws_idx=ws_idx,
        creative_idx=creative_idx
    ))
    assert_equals(result[0][1], count)


@then('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx:d}인 클릭수는 {count:d}개이다.')
def check_click_count_by_ws_idx_and_creative_idx(context, ws_idx, creative_idx, count):
    sql = SELECT_CLICK_EXPOSE_COUNT.format(
        ws_idx=ws_idx,
        creative_idx=creative_idx
    )
    result = select_one_click_house_query(sql)  # print("{} --> {} = {}".format(sql, result[0][0], count))
    assert_equals(result[0][0], count)


@then('wsIdx가 {ws_idx}인 광고주에 소재아이디가 {creative_idx:d}인 노출수는 {count:d}개이다.')
def check_expose_count_by_ws_idx_and_creative_idx(context, ws_idx, creative_idx, count):
    result = select_one_click_house_query(SELECT_CLICK_EXPOSE_COUNT.format(
        ws_idx=ws_idx,
        creative_idx=creative_idx
    ))
    assert_equals(result[0][1], count)


@then('wsIdx가 {ws_idx}인 광고주에 클릭수는 {count:d}개이다.')
def check_expose_count_by_ws_idx(context, ws_idx, count):
    sql = SELECT_CLICK_EXPOSE_COUNT_BY_WS_IDX.format(ws_idx=ws_idx)
    result = select_one_click_house_query(sql)  # print("{} = {} = {}".format(result[0][0], count, sql))
    assert_equals(result[0][0], count)


@then('wsIdx가 {ws_idx}인 광고주에 노출수는 {count:d}개이다.')
def check_expose_count_by_ws_idx(context, ws_idx, count):
    result = select_one_click_house_query(SELECT_CLICK_EXPOSE_COUNT_BY_WS_IDX.format(
        ws_idx=ws_idx
    ))
    assert_equals(result[0][1], count)


@then('wsIdx가 {ws_idx}인 광고주의 광고상품이 {product_idx:d}인 총 과금액은 {amount:d}원이다.')
def check_click_count_by_ws_idx_and_creative_idx(context, ws_idx, product_idx, amount):
    query = SELECT_AMOUNT_BY_WS_IDX_AND_PRODUCT_IDX.format(ws_idx, product_idx)  # print(query)
    result = select_one_click_house_query(query)
    ad_amount = AdAmount.getInstance(result)
    if product_idx == 1:
        assert ad_amount.openlisting_total_payment == amount
    elif product_idx == 2:
        assert ad_amount.searchlisting_total_payment == amount
    elif product_idx == 3:
        assert ad_amount.vedio_total_payment == amount
    elif product_idx == 4:
        assert ad_amount.brand_total_payment == amount
    else:
        print(f"product_idx: {product_idx}")
        assert True is False


@then('wsIdx가 {ws_idx}인 광고주의 과금액은 {amount:d}원이다.')
def check_ad_payment_amount(context, ws_idx, amount: int):
    query = SELECT_AMOUNT_TOTAL_BY_WS_IDX.format(ws_idx)
    result = select_one_click_house_query(query)
    ad_amount = AdAmount.getInstance(result)
    assert_equals(ad_amount.total_payment, amount)


@then('소재 단위리포트에서 wsIdx가 {ws_idx}인 광고주의 광고상품이 {product_idx:d}인 총 과금액은 {amount:d}원이다.')
def check_ad_payment_by_creative_amount_with_product_idx(context, ws_idx, product_idx, amount):
    query = SELECT_AMOUNT_TOTAL_IN_AD_PAYMENT_BY_CREARIVE_BY_WS_IDX_AND_PRODUCT_IDX.format(ws_idx, product_idx)
    result = select_one_click_house_query(query)
    assert_equals(result[0][1], amount)


@then('소재 단위리포트에서 wsIdx가 {ws_idx}인 광고주의 과금액은 {amount:d}원이다.')
def check_ad_payment_by_creative_amount(context, ws_idx, amount: int):
    query = SELECT_AMOUNT_TOTAL_IN_AD_PAYMENT_BY_CREARIVE_BY_WS_IDX.format(ws_idx)
    result = select_one_click_house_query(query)
    assert_equals(result[0][1], amount)


@when(
    '외부전환 지표가 wsIdx가 {ws_idx}인 광고주에 찜수 {like_count:d}건, 매장방문수 {ws_visit_count:d}건, 거래처 요청수 {trade_request_count:d}건, 광고 본 사용자 수 {viewer_count:d}건, {row_count:d}회 발생했다.')
def check_brand_ad_conversion(
        context, ws_idx: int, like_count: int, ws_visit_count: int, trade_request_count: int, viewer_count: int
):
    query = BRAND_AD_CONVERSION_SELECT.format(ws_idx)
    result = select_one_click_house_query(query)
    assert_equals(result[0][1], like_count)
    assert_equals(result[0][2], ws_visit_count)
    assert_equals(result[0][3], trade_request_count)
    assert_equals(result[0][4], viewer_count)


@then('테스트 데이터 생성이 완료되었다.')
def step_impl(context):
    # for item in Client('localhost').execute('SHOW DATABASES'):
    #    sys.stdout.write("stdout:%s;\n" % item)
    assert context.failed is False


@then('평균 데이터 보정')
def correct_average_data(context):
    previous_week = ['2022-06-06 00:00:00', '2022-06-12 23:59:59']
    this_week = ['2022-06-13 00:00:00', '2022-06-19 23:59:59']
    correct_ad_payment_average_data(previous_week[0], previous_week[1])
    correct_ad_payment_average_data(this_week[0], this_week[1])

    correct_openlisting_average_data(previous_week[0], previous_week[1])
    correct_openlisting_average_data(this_week[0], this_week[1])

    correct_searchlisting_average_data(previous_week[0], previous_week[1])
    correct_searchlisting_average_data(this_week[0], this_week[1])

    correct_brand_average_data(previous_week[0], previous_week[1])
    correct_brand_average_data(this_week[0], this_week[1])
    pass


@then('시작 일시가 {start_date_time} 이고 종료 일시가 {end_date_time} 인 평균 데이터 검증')
def validate_average_data(context, start_date_time, end_date_time):
    # 총액 평균 검증
    target_total_amount_average = 30000

    total_total_amount = select_one_click_house_query(
        SELECT_AVERAGE_TOTAL_AMOUNT_BY_CREATED_AT.format(
            start_date_time=start_date_time,
            end_date_time=end_date_time
        )
    )
    # assert total_total_amount[0][0] == target_total_amount_average

    openlisting_total_amount = select_one_click_house_query(
        SELECT_AVERAGE_TOTAL_AMOUNT_BY_CREATED_AT_AND_PRODUCT.format(
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            product_idx=1
        )
    )
    assert openlisting_total_amount[0][0] == target_total_amount_average

    searchlisting_total_amount = select_one_click_house_query(
        SELECT_AVERAGE_TOTAL_AMOUNT_BY_CREATED_AT_AND_PRODUCT.format(
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            product_idx=2
        ))
    assert searchlisting_total_amount[0][0] == target_total_amount_average

    brand_total_amount = select_one_click_house_query(
        SELECT_AVERAGE_TOTAL_AMOUNT_BY_CREATED_AT_AND_PRODUCT.format(
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            product_idx=4
        ))
    assert brand_total_amount[0][0] == target_total_amount_average

    # 평균 클릭률 검증
    target_click_rate = 4.5
    openlisting_click_rate = select_one_click_house_query(
        SELECT_OPENLISTING_CLICK_RATE_BY_CREATED_AT.format(
            start_date_time=start_date_time,
            end_date_time=end_date_time
        ))
    assert openlisting_click_rate[0][0] == target_click_rate

    searchlisting_click_rate = select_one_click_house_query(
        SELECT_SEARCHLISTING_CLICK_RATE_BY_CREATED_AT.format(
            start_date_time=start_date_time,
            end_date_time=end_date_time
        ))
    assert searchlisting_click_rate[0][0] == target_click_rate

    brand_click_rate = select_one_click_house_query(
        SELECT_BRAND_CLICK_RATE_BY_CREATED_AT.format(
            start_date_time=start_date_time,
            end_date_time=end_date_time
        ))
    assert brand_click_rate[0][0] == target_click_rate

    pass
