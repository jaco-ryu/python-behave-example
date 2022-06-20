Feature: 주간리포트 테스트 데이터 셋 생성 ( https://docs.google.com/spreadsheets/d/1VaCwzePrZdsEmM43sieEfD8eRdeeXZYh_xn9sg5eZjE/edit#gid=1809453365 )

  Scenario: 기본적인 데이터 INSERT 테스트
    Given 광고테스트를 위한 스키마를 생성한다.
    When 오픈리스팅광고로 wsIdx가 4인 광고주에 소재아이디가 1인 노출이 2022-06-13 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 노출 row가 1회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 4인 광고주에 소재아이디가 1인 클릭이 2022-06-13 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 클릭 row가 1회 쌓인다.
    When 서치리스팅광고로 wsIdx가 4인 광고주에 소재아이디가 2인 노출이 2022-06-13 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 노출 row가 1회 쌓인다.
    When 서치리스팅광고로 wsIdx가 4인 광고주에 소재아이디가 2인 클릭이 2022-06-13 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 클릭 row가 1회 쌓인다.
    When 브랜드광고로 wsIdx가 4인 광고주에 소재아이디가 3인 노출이 2022-06-13 10:23:59의 시점에 비딩가가 400원인 1번이 들어간 하나의 노출 row가 1회 쌓인다.
    When 브랜드광고로 wsIdx가 4인 광고주에 소재아이디가 3인 클릭이 2022-06-13 10:23:59의 시점에 비딩가가 400원인 1번이 들어간 하나의 클릭 row가 1회 쌓인다.
    When wsIdx가 4인 광고주에 소재아이디가 3인 상단영역 찜이 2022-06-13 10:23:59의 시점에 1회 발생했다.
    When wsIdx가 4인 광고주에 소재아이디가 3인 하단영역 찜이 2022-06-13 10:23:59의 시점에 2회 발생했다.
    When wsIdx가 4인 광고주에 소재아이디가 3인 매장방문이 2022-06-13 10:23:59의 시점에 1회 발생했다.
    When 소재단위리포트 데이터가 wsIdx가 4인 광고주에 광고상품이 1인 소재아이디가 3인 2022-06-13 10:23:59의 시점에 vat포함 4원으로 3회 발생했다.
    When 과금이 wsIdx가 4인 광고주에 광고상품이 1으로 2022-06-13 10:23:59의 시점에 무상으로 13원 1회 발생했다.
    When 과금이 wsIdx가 4인 광고주에 광고상품이 1으로 2022-06-13 10:23:59의 시점에 유상으로 13원 1회 발생했다.
    When 과금이 wsIdx가 4인 광고주에 광고상품이 1으로 2022-06-13 10:23:59의 시점에 유상으로 13원, 무상으로 10원 1회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE1 - 주간요약 ( 지난주 대비 노출수, 클릭 수 높음, 지난주 대비 찜수 매장 낮음 )
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 전전주데이터 노출 생성 ==> (1 * 10) + (10 * 5) + (1 * 40) = 100
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 노출이 2022-06-07 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 노출 row가 10회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 노출이 2022-06-08 10:23:59의 시점에 비딩가가 5.0원인 10번이 들어간 하나의 노출 row가 5회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 노출이 2022-06-08 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 노출 row가 40회 쌓인다.
    # 전주데이터 노출 생성 ==>  (1 * 10) + (10 * 5) + (1 * 40) + (5 * 6) + (1 * 3) = 133
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 노출이 2022-06-14 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 노출 row가 10회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 노출이 2022-06-15 10:23:59의 시점에 비딩가가 5.0원인 10번이 들어간 하나의 노출 row가 5회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 노출이 2022-06-16 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 노출 row가 40회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 노출이 2022-06-17 10:23:59의 시점에 비딩가가 5.0원인 5번이 들어간 하나의 노출 row가 6회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 노출이 2022-06-18 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 노출 row가 3회 쌓인다.
    # 전전주데이터 클릭 생성 ==>  (1 * 10) + (10 * 5) + (1 * 40) = 100
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 클릭이 2022-06-07 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 클릭 row가 10회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 클릭이 2022-06-08 10:23:59의 시점에 비딩가가 5.0원인 10번이 들어간 하나의 클릭 row가 5회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 클릭이 2022-06-09 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 클릭 row가 40회 쌓인다.
    # 전주데이터 클릭 생성 ==>  (1 * 10) + (10 * 5) + (1 * 40) + (5 * 6) + (1 * 3) + (1 * 10) = 143
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 클릭이 2022-06-14 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 클릭 row가 10회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 클릭이 2022-06-15 10:23:59의 시점에 비딩가가 5.0원인 10번이 들어간 하나의 클릭 row가 5회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 클릭이 2022-06-16 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 클릭 row가 40회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 클릭이 2022-06-17 10:23:59의 시점에 비딩가가 5.0원인 5번이 들어간 하나의 클릭 row가 6회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 클릭이 2022-06-18 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 클릭 row가 3회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22863인 광고주에 소재아이디가 2516인 클릭이 2022-06-13 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 클릭 row가 10회 쌓인다.
    # 전전주데이터 찜 생성 ==>  10 + 50 + 40 = 100
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 상단영역 찜이 2022-06-07 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 하단영역 찜이 2022-06-08 10:23:59의 시점에 50회 발생했다.
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 상단영역 찜이 2022-06-09 10:23:59의 시점에 40회 발생했다.
    # 전주데이터 찜 생성 ==>  10 + 50 + 25 = 85
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 상단영역 찜이 2022-06-14 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 하단영역 찜이 2022-06-15 10:23:59의 시점에 50회 발생했다.
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 상단영역 찜이 2022-06-16 10:23:59의 시점에 25회 발생했다.
    # 전전주데이터 매장방문수 생성 ==>  10 + 50 + 40 = 100
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 매장방문이 2022-06-07 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 매장방문이 2022-06-08 10:23:59의 시점에 50회 발생했다.
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 매장방문이 2022-06-09 10:23:59의 시점에 40회 발생했다.
    # 전주데이터 매장방문수 생성 ==>  10 + 50 + 6 = 66
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 매장방문이 2022-06-13 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 매장방문이 2022-06-14 10:23:59의 시점에 50회 발생했다.
    When wsIdx가 22863인 광고주에 소재아이디가 2516인 매장방문이 2022-06-15 10:23:59의 시점에 6회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE2 - 주간요약 ( 지난주 대비 노출수, 클릭 수 낮음, 지난주 대비 찜수 매장 높음 )
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 전전주데이터 노출 생성 ==> (1 * 10) + (10 * 5) + (1 * 40) + (1 * 20) = 120
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-07 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 노출 row가 10회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-08 10:23:59의 시점에 비딩가가 5.0원인 10번이 들어간 하나의 노출 row가 5회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-09 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 노출 row가 40회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-10 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 노출 row가 20회 쌓인다.
    # 전주데이터 노출 생성 ==> (1 * 10) + (10 * 5) + (1 * 40) = 100
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-14 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 노출 row가 10회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-15 10:23:59의 시점에 비딩가가 5.0원인 10번이 들어간 하나의 노출 row가 5회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-16 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 노출 row가 40회 쌓인다.
    # 전전주데이터 클릭 생성 ==>  (1 * 10) + (10 * 5) + (1 * 40) + (5 * 6) + (1 * 10) = 140
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 클릭이 2022-06-07 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 클릭 row가 10회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 클릭이 2022-06-08 10:23:59의 시점에 비딩가가 5.0원인 10번이 들어간 하나의 클릭 row가 5회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 클릭이 2022-06-09 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 클릭 row가 40회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 클릭이 2022-06-10 10:23:59의 시점에 비딩가가 5.0원인 5번이 들어간 하나의 클릭 row가 6회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 클릭이 2022-06-11 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 클릭 row가 10회 쌓인다.
    # 전주데이터 클릭 생성 ==>  (1 * 10) + (10 * 5) + (1 * 40) = 100
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 클릭이 2022-06-14 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 클릭 row가 10회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 클릭이 2022-06-15 10:23:59의 시점에 비딩가가 5.0원인 10번이 들어간 하나의 클릭 row가 5회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 클릭이 2022-06-16 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 클릭 row가 40회 쌓인다.
    # 전전주데이터 찜 생성 ==>  10 + 50 + 28 = 88
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 상단영역 찜이 2022-06-07 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 하단영역 찜이 2022-06-08 10:23:59의 시점에 50회 발생했다.
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 상단영역 찜이 2022-06-09 10:23:59의 시점에 28회 발생했다.
    # 전주데이터 찜 생성 ==>  10 + 50 + 90 = 150
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 상단영역 찜이 2022-06-14 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 하단영역 찜이 2022-06-15 10:23:59의 시점에 50회 발생했다.
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 상단영역 찜이 2022-06-16 10:23:59의 시점에 90회 발생했다.
    # 전전주데이터 매장방문수 생성 ==>  10 + 10 + 40 = 60
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 매장방문이 2022-06-07 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 매장방문이 2022-06-08 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 매장방문이 2022-06-09 10:23:59의 시점에 40회 발생했다.
    # 전주데이터 매장방문수 생성 ==>  10 + 50 + 40 = 100
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 매장방문이 2022-06-14 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 매장방문이 2022-06-15 10:23:59의 시점에 50회 발생했다.
    When wsIdx가 22864인 광고주에 소재아이디가 2517인 매장방문이 2022-06-16 10:23:59의 시점에 40회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE3 - 주간요약 ( 지난주 대비 노출수, 클릭 수, 찜수, 매장방문수 동일 )
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 전전주데이터 노출 생성 ==> (1 * 10) + (10 * 5) + (1 * 40) + (1 * 20) = 100
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-07 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 노출 row가 10회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-08 10:23:59의 시점에 비딩가가 5.0원인 10번이 들어간 하나의 노출 row가 5회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-09 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 노출 row가 40회 쌓인다.
    When 서치리스팅광고로 wsIdx가 22864인 광고주에 소재아이디가 2517인 노출이 2022-06-10 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 노출 row가 20회 쌓인다.
    # 전전주데이터 클릭 생성 ==>  (1 * 10) + (10 * 5) + (1 * 40) + (5 * 6) + (1 * 10) = 100
    # 전주데이터 노출 생성 ==> (1 * 10) + (10 * 5) + (1 * 40) = 140
    # 전주데이터 클릭 생성 ==>  (1 * 10) + (10 * 5) + (1 * 40) = 140
    # 전전주데이터 찜 생성 ==>  10 + 50 + 28 = 80
    # 전주데이터 찜 생성 ==>  10 + 50 + 90 = 80
    # 전전주데이터 매장방문수 생성 ==>  10 + 10 + 40 = 60
    # 전주데이터 매장방문수 생성 ==>  10 + 50 + 40 = 60
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE4 - 평균광고비용 - 내 오픈리스팅 사용 금액이 광고주 평균보다 높음
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 다른 광고주 평균 ==>
    When
    # 이번주 해당 광고주의 평균 ==>
    When 과금이 wsIdx가 22865인 광고주에 광고상품이 1으로 2022-06-07 10:23:59의 시점에 유상으로 10000원 1회 발생했다.
    When 과금이 wsIdx가 22865인 광고주에 광고상품이 1으로 2022-06-08 10:23:59의 시점에 유상으로 20000원 1회 발생했다.
    When 과금이 wsIdx가 22865인 광고주에 광고상품이 1으로 2022-06-09 10:23:59의 시점에 유상으로 10000원 1회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE6 - 평균광고비용 - 내 오픈리스팅 사용 금액이 광고주 평균보다 비슷.. (+ , - 10000)
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 다른 광고주 평균 ==>
    When
    # 이번주 해당 광고주의 평균 ==>
    When 과금이 wsIdx가 22868인 광고주에 광고상품이 1으로 2022-06-07 10:23:59의 시점에 무상으로 10000원 1회 발생했다.
    When 과금이 wsIdx가 22868인 광고주에 광고상품이 1으로 2022-06-08 10:23:59의 시점에 무상으로 5000원 1회 발생했다.
    When 과금이 wsIdx가 22868인 광고주에 광고상품이 1으로 2022-06-09 10:23:59의 시점에 무상으로 10000원 1회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE8 - 평균광고비용 - 내 서치리스팅 사용 금액이 광고주 평균보다 높음
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 다른 광고주 평균 ==>
    When
    # 이번주 해당 광고주의 평균 ==>
    When 과금이 wsIdx가 22870인 광고주에 광고상품이 2으로 2022-06-07 10:23:59의 시점에 무상으로 10000원 2회 발생했다.
    When 과금이 wsIdx가 22870인 광고주에 광고상품이 2으로 2022-06-08 10:23:59의 시점에 유상으로 10000원 1회 발생했다.
    When 과금이 wsIdx가 22870인 광고주에 광고상품이 2으로 2022-06-09 10:23:59의 시점에 무상으로 10000원 1회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

