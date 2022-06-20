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

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE3 -브랜드광고 주간요약 ( 지난주 대비 노출수, 클릭 수, 찜수, 매장방문수 동일 )
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 전전주데이터 노출 생성 ==> (1 * 10) + (10 * 5) + (1 * 20) + (1 * 20) = 100
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 노출이 2022-06-07 10:23:59의 시점에 비딩가가 500원인 1번이 들어간 하나의 노출 row가 10회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 노출이 2022-06-08 10:23:59의 시점에 비딩가가 500원인 10번이 들어간 하나의 노출 row가 5회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 노출이 2022-06-09 10:23:59의 시점에 비딩가가 500원인 1번이 들어간 하나의 노출 row가 20회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 노출이 2022-06-10 10:23:59의 시점에 비딩가가 400원인 1번이 들어간 하나의 노출 row가 20회 쌓인다.
      # 전전주데이터 노출 생성 ==> (1 * 10) + (10 * 5) + (1 * 20) + (1 * 20) = 100
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 노출이 2022-06-14 10:23:59의 시점에 비딩가가 500원인 1번이 들어간 하나의 노출 row가 10회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 노출이 2022-06-15 10:23:59의 시점에 비딩가가 500원인 10번이 들어간 하나의 노출 row가 5회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 노출이 2022-06-16 10:23:59의 시점에 비딩가가 500원인 1번이 들어간 하나의 노출 row가 20회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 노출이 2022-06-17 10:23:59의 시점에 비딩가가 400원인 1번이 들어간 하나의 노출 row가 20회 쌓인다.
    # 전전주데이터 클릭 생성 ==>  (1 * 10) + (10 * 5) + (1 * 40) + (5 * 6) + (1 * 10) = 140
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 클릭이 2022-06-07 10:23:59의 시점에 비딩가가 500원인 1번이 들어간 하나의 클릭 row가 10회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 클릭이 2022-06-08 10:23:59의 시점에 비딩가가 500원인 10번이 들어간 하나의 클릭 row가 5회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 클릭이 2022-06-09 10:23:59의 시점에 비딩가가 500원인 1번이 들어간 하나의 클릭 row가 40회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 클릭이 2022-06-10 10:23:59의 시점에 비딩가가 500원인 5번이 들어간 하나의 클릭 row가 6회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 클릭이 2022-06-11 10:23:59의 시점에 비딩가가 400원인 1번이 들어간 하나의 클릭 row가 10회 쌓인다.
   # 전전주데이터 클릭 생성 ==>  (1 * 10) + (10 * 5) + (1 * 40) + (5 * 6) + (1 * 10) = 140
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 클릭이 2022-06-13 10:23:59의 시점에 비딩가가 500원인 1번이 들어간 하나의 클릭 row가 10회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 클릭이 2022-06-14 10:23:59의 시점에 비딩가가 500원인 10번이 들어간 하나의 클릭 row가 5회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 클릭이 2022-06-15 10:23:59의 시점에 비딩가가 500원인 1번이 들어간 하나의 클릭 row가 40회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 클릭이 2022-06-16 10:23:59의 시점에 비딩가가 500원인 5번이 들어간 하나의 클릭 row가 6회 쌓인다.
    When 브랜드광고로 wsIdx가 22865인 광고주에 소재아이디가 2518인 클릭이 2022-06-17 10:23:59의 시점에 비딩가가 400원인 1번이 들어간 하나의 클릭 row가 10회 쌓인다.
    # 전전주데이터 찜 생성 ==>  10 + 50 + 28 = 88
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 상단영역 찜이 2022-06-07 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 하단영역 찜이 2022-06-08 10:23:59의 시점에 50회 발생했다.
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 상단영역 찜이 2022-06-09 10:23:59의 시점에 28회 발생했다.
    # 전주데이터 찜 생성 ==>  10 + 50 + 28 = 88
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 상단영역 찜이 2022-06-14 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 하단영역 찜이 2022-06-15 10:23:59의 시점에 50회 발생했다.
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 상단영역 찜이 2022-06-16 10:23:59의 시점에 28회 발생했다.
    # 전전주데이터 매장방문수 생성 ==>  10 + 10 + 40 = 60
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 매장방문이 2022-06-07 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 매장방문이 2022-06-08 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 매장방문이 2022-06-09 10:23:59의 시점에 40회 발생했다.
    # 전주데이터 매장방문수 생성 ==>  10 + 10 + 40 = 60
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 매장방문이 2022-06-14 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 매장방문이 2022-06-15 10:23:59의 시점에 10회 발생했다.
    When wsIdx가 22865인 광고주에 소재아이디가 2518인 매장방문이 2022-06-16 10:23:59의 시점에 40회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE4 - 평균광고비용 - 내 오픈리스팅 사용 금액이 광고주 평균보다 높음
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 다른 광고주 평균 ==>
    # When 10개의 계정으로 광고주에 광고상품이 1으로 2022-06-16 10:23:59의 시점에 유상으로 500원 100회 발생했다.
    # 이번주 해당 광고주의 평균 ==>
    When 과금이 wsIdx가 22866인 광고주에 광고상품이 1으로 2022-06-16 10:23:59의 시점에 유상으로 200원, 무상으로 200원 100회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE5 - 평균 광고비용 ( 내 오픈리스팅 사용 금액이 광고주 평균보다 낮음 )
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 오픈리스팅 사용 금액 생성 ==>  130 + (110*2) + (120*2) + 200 + 300 + (100*3) +100 + (300*2) + (250*2) + (350*2) + (250*3) + (200*2) + (270*3) + (220*3) + 300 + (220*3) + (140*3) + (200*3) + (110*4) + (100*5) + 170 + (100*2) + (120*2) + 100 + (150*3) + ( 10 * 1 ) = 10000
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-13 10:23:59의 시점에 무상으로 130원 1회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-13 10:24:59의 시점에 무상으로 110원 2회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-13 11:10:40의 시점에 무상으로 120원 2회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-13 10:24:59의 시점에 무상으로 200원 1회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-14 09:23:59의 시점에 무상으로 300원 1회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-14 11:23:59의 시점에 무상으로 100원 3회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-14 13:12:04의 시점에 무상으로 100원 1회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-15 10:23:59의 시점에 유상으로 300원, 무상으로 250원 2회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-15 10:24:59의 시점에 유상으로 350원 2회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-15 14:10:09의 시점에 유상으로 250원 3회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-15 16:17:12의 시점에 유상으로 200원 2회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-15 19:10:32의 시점에 유상으로 270원 3회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-15 23:11:41의 시점에 유상으로 220원 3회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-16 11:25:59의 시점에 유상으로 300원 1회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-16 13:10:51의 시점에 유상으로 220원 3회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-16 15:25:11의 시점에 유상으로 140원 2회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-16 15:26:35의 시점에 유상으로 140원 1회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-16 17:01:01의 시점에 유상으로 200원 3회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-16 18:14:22의 시점에 유상으로 110원 4회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-16 20:18:46의 시점에 유상으로 100원 5회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-17 10:23:59의 시점에 유상으로 170원 1회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-17 19:10:02의 시점에 유상으로 100원 2회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-17 21:26:15의 시점에 유상으로 120원 2회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-18 10:23:52의 시점에 유상으로 100원 1회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-19 16:57:05의 시점에 유상으로 150원 3회 발생했다.
    When 과금이 wsIdx가 22867인 광고주에 광고상품이 1으로 2022-06-19 16:57:05의 시점에 유상으로 10원 1회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE6 - 평균광고비용 - 내 오픈리스팅 사용 금액이 광고주 평균보다 비슷.. (+ , - 10000)
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 다른 광고주 평균 ==>
    # When 10개의 계정으로 광고주에 광고상품이 1으로 2022-06-16 10:23:59의 시점에 유상으로 500원 100회 발생했다.
    # 이번주 해당 광고주의 평균 ==> ( 10000 * 1 ) + ( 5000 * 1 ) + ( 10000 * 1 ) = 25000
    When 과금이 wsIdx가 22868인 광고주에 광고상품이 1으로 2022-06-07 10:23:59의 시점에 무상으로 10000원 1회 발생했다.
    When 과금이 wsIdx가 22868인 광고주에 광고상품이 1으로 2022-06-08 10:23:59의 시점에 무상으로 5000원 1회 발생했다.
    When 과금이 wsIdx가 22868인 광고주에 광고상품이 1으로 2022-06-09 10:23:59의 시점에 무상으로 10000원 1회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE7 - 평균 광고비용 ( 내 오픈리스팅 사용 금액이 광고주 평균과 동일 )
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 오픈리스팅 사용 금액 생성 ==> (100*20) + (200*20) + (200*20) + (100*30) + (100*25) + (200*25) + (300*10) + (100*25) + (100*30) + (100*10) = 30000
    When 과금이 wsIdx가 22869인 광고주에 광고상품이 1으로 2022-06-13 10:23:59의 시점에 무상으로 100원 20회 발생했다.
    When 과금이 wsIdx가 22869인 광고주에 광고상품이 1으로 2022-06-13 12:23:59의 시점에 무상으로 200원 20회 발생했다.
    When 과금이 wsIdx가 22869인 광고주에 광고상품이 1으로 2022-06-14 10:23:59의 시점에 무상으로 200원 20회 발생했다.
    When 과금이 wsIdx가 22869인 광고주에 광고상품이 1으로 2022-06-15 10:23:59의 시점에 유상으로 100원 30회 발생했다.
    When 과금이 wsIdx가 22869인 광고주에 광고상품이 1으로 2022-06-16 10:23:59의 시점에 유상으로 100원 25회 발생했다.
    When 과금이 wsIdx가 22869인 광고주에 광고상품이 1으로 2022-06-17 10:23:59의 시점에 유상으로 200원 25회 발생했다.
    When 과금이 wsIdx가 22869인 광고주에 광고상품이 1으로 2022-06-17 18:23:59의 시점에 유상으로 300원 10회 발생했다.
    When 과금이 wsIdx가 22869인 광고주에 광고상품이 1으로 2022-06-18 10:23:59의 시점에 유상으로 100원 25회 발생했다.
    When 과금이 wsIdx가 22869인 광고주에 광고상품이 1으로 2022-06-19 10:23:59의 시점에 유상으로 100원 30회 발생했다.
    When 과금이 wsIdx가 22869인 광고주에 광고상품이 1으로 2022-06-19 13:41:10의 시점에 유상으로 100원 10회 발생했다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE8 - 평균광고비용 - 내 서치리스팅 사용 금액이 광고주 평균보다 높음
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 다른 광고주 평균 ==>
    # When 10개의 계정으로 광고주에 광고상품이 1으로 2022-06-16 10:23:59의 시점에 유상으로 500원 100회 발생했다.
    # 이번주 해당 광고주의 평균 ==> (10000 * 2) + (10000 * 1) + (10000 * 1) = 40000
    When 과금이 wsIdx가 22870인 광고주에 광고상품이 2으로 2022-06-07 10:23:59의 시점에 무상으로 10000원 2회 발생했다.
    When 과금이 wsIdx가 22870인 광고주에 광고상품이 2으로 2022-06-08 10:23:59의 시점에 유상으로 10000원 1회 발생했다.
    When 과금이 wsIdx가 22870인 광고주에 광고상품이 2으로 2022-06-09 10:23:59의 시점에 무상으로 10000원 1회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE9 - 평균 광고비용 ( 내 오픈리스팅 사용 금액이 광고주 평균과 동일 )
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 오픈리스팅 사용 금액 생성 ==> (100*10) + (200*10) + (200*10) + (100*15) + (100*12) + (200*12) + (300*5) + (100*13) + (100*16) + (100*5) = 15000
    When 과금이 wsIdx가 22871인 광고주에 광고상품이 2으로 2022-06-13 10:23:59의 시점에 무상으로 100원 10회 발생했다.
    When 과금이 wsIdx가 22871인 광고주에 광고상품이 2으로 2022-06-13 12:23:59의 시점에 무상으로 200원 10회 발생했다.
    When 과금이 wsIdx가 22871인 광고주에 광고상품이 2으로 2022-06-14 10:23:59의 시점에 무상으로 200원 10회 발생했다.
    When 과금이 wsIdx가 22871인 광고주에 광고상품이 2으로 2022-06-15 10:23:59의 시점에 유상으로 100원 15회 발생했다.
    When 과금이 wsIdx가 22871인 광고주에 광고상품이 2으로 2022-06-16 10:23:59의 시점에 유상으로 100원 12회 발생했다.
    When 과금이 wsIdx가 22871인 광고주에 광고상품이 2으로 2022-06-17 10:23:59의 시점에 유상으로 200원 12회 발생했다.
    When 과금이 wsIdx가 22871인 광고주에 광고상품이 2으로 2022-06-17 18:23:59의 시점에 유상으로 300원 5회 발생했다.
    When 과금이 wsIdx가 22871인 광고주에 광고상품이 2으로 2022-06-18 10:23:59의 시점에 유상으로 100원 13회 발생했다.
    When 과금이 wsIdx가 22871인 광고주에 광고상품이 2으로 2022-06-19 10:23:59의 시점에 유상으로 100원 16회 발생했다.
    When 과금이 wsIdx가 22871인 광고주에 광고상품이 2으로 2022-06-19 13:41:10의 시점에 유상으로 100원 5회 발생했다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE10 - 내 서치리스팅 사용 금액이 광고주 평균보다 비슷.. (+ , - 10000)
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 다른 광고주 평균 ==>
    # When 10개의 계정으로 광고주에 광고상품이 1으로 2022-06-16 10:23:59의 시점에 유상으로 500원 100회 발생했다.
    # 이번주 해당 광고주의 평균 ==> (10000 * 1) + (5000 * 1) + (10000 * 1) = 25000
    When 과금이 wsIdx가 22872인 광고주에 광고상품이 2으로 2022-06-07 10:23:59의 시점에 무상으로 10000원 1회 발생했다.
    When 과금이 wsIdx가 22872인 광고주에 광고상품이 2으로 2022-06-08 10:23:59의 시점에 유상으로 5000원 1회 발생했다.
    When 과금이 wsIdx가 22872인 광고주에 광고상품이 2으로 2022-06-09 10:23:59의 시점에 무상으로 10000원 1회 발생했다.
    Then 테스트 데이터 생성이 완료되었다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE11 - 평균 광고비용 ( 내 서치리스팅 사용 금액이 광고주 평균과 동일 )
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 서치리스팅 사용 금액 생성 ==> (100*20) + (200*20) + (200*20) + (100*30) + (100*25) + (200*25) + (300*10) + (100*25) + (100*30) + (100*10) = 30000
    When 과금이 wsIdx가 22873인 광고주에 광고상품이 2으로 2022-06-13 10:23:59의 시점에 무상으로 100원 20회 발생했다.
    When 과금이 wsIdx가 22873인 광고주에 광고상품이 2으로 2022-06-13 12:23:59의 시점에 무상으로 200원 20회 발생했다.
    When 과금이 wsIdx가 22873인 광고주에 광고상품이 2으로 2022-06-14 10:23:59의 시점에 무상으로 200원 20회 발생했다.
    When 과금이 wsIdx가 22873인 광고주에 광고상품이 2으로 2022-06-15 10:23:59의 시점에 유상으로 100원 30회 발생했다.
    When 과금이 wsIdx가 22873인 광고주에 광고상품이 2으로 2022-06-16 10:23:59의 시점에 유상으로 100원 25회 발생했다.
    When 과금이 wsIdx가 22873인 광고주에 광고상품이 2으로 2022-06-17 10:23:59의 시점에 유상으로 200원 25회 발생했다.
    When 과금이 wsIdx가 22873인 광고주에 광고상품이 2으로 2022-06-17 18:23:59의 시점에 유상으로 300원 10회 발생했다.
    When 과금이 wsIdx가 22873인 광고주에 광고상품이 2으로 2022-06-18 10:23:59의 시점에 유상으로 100원 25회 발생했다.
    When 과금이 wsIdx가 22873인 광고주에 광고상품이 2으로 2022-06-19 10:23:59의 시점에 유상으로 100원 30회 발생했다.
    When 과금이 wsIdx가 22873인 광고주에 광고상품이 2으로 2022-06-19 13:41:10의 시점에 유상으로 100원 10회 발생했다.

  Scenario: 광비우스 4차_주간 리포트 시나리오 - CASE20 - 내 오픈리스팅 클릭률이 광고주 평균보다 높음
    Given 광고테스트를 위한 기존 스키마를 그대로 이용한다.
    # 전주데이터 노출 생성/전주데이터 클릭 생성 = 클릭율 ==>  "(1 * 10) + (10 * 2) + (1 * 1) = 31" / "(10 * 10) + (20 * 19) + (1 * 20) = 500" = 6.2%
    When 오픈리스팅광고로 wsIdx가 22882인 광고주에 소재아이디가 2544인 노출이 2022-06-14 10:23:59의 시점에 비딩가가 4.0원인 1번이 들어간 하나의 노출 row가 10회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22882인 광고주에 소재아이디가 2544인 노출이 2022-06-15 10:23:59의 시점에 비딩가가 5.0원인 10번이 들어간 하나의 노출 row가 2회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22882인 광고주에 소재아이디가 2544인 노출이 2022-06-16 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 노출 row가 1회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22882인 광고주에 소재아이디가 2544인 클릭이 2022-06-07 10:23:59의 시점에 비딩가가 4.0원인 10번이 들어간 하나의 클릭 row가 10회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22882인 광고주에 소재아이디가 2544인 클릭이 2022-06-08 10:23:59의 시점에 비딩가가 5.0원인 20번이 들어간 하나의 클릭 row가 19회 쌓인다.
    When 오픈리스팅광고로 wsIdx가 22882인 광고주에 소재아이디가 2544인 클릭이 2022-06-09 10:23:59의 시점에 비딩가가 5.0원인 1번이 들어간 하나의 클릭 row가 20회 쌓인다.
    Then 테스트 데이터 생성이 완료되었다.

