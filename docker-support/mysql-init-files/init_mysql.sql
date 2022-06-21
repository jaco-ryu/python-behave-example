CREATE DATABASE IF NOT EXISTS beluga;
CREATE TABLE IF NOT EXISTS beluga.weekly_ad_report
(
    idx                          BIGINT AUTO_INCREMENT               NOT NULL        COMMENT 'pk',
    ws_idx                       BIGINT                              NOT NULL        COMMENT '도매 고유번호 - 도매 idx',
    version                      INT                                 NOT NULL        COMMENT '리포트 버젼',
    start_date                   DATE                                NOT NULL        COMMENT '보고서 통계 시작일',
    end_date                     DATE                                NOT NULL        COMMENT '보고서 통계 종료일',
    created_at                   DATETIME                            NOT NULL        COMMENT '보고서 생성일',
    month                        INT                                 NOT NULL        COMMENT '월',
    week                         INT                                 NOT NULL        COMMENT '주차',
    weekly_summary               JSON                                NULL            COMMENT '주간 요약',
    average_ad_cost              JSON                                NULL            COMMENT '평균 광고 비용',
    average_ad_click_rate        JSON                                NULL            COMMENT '평균 광고 클릭률',
    popular_exposure_goods       JSON                                NULL            COMMENT '인기 사용자 노출 소재(상품)',
    popular_action_goods         JSON                                NULL            COMMENT '인기 사용자 반응 소재(상품)',
    click_action_goods           JSON                                NULL            COMMENT '사용자 반응 베스트/워스트 상품',
    recommended_product          JSON                                NULL            COMMENT '광고 추천 상품',
    PRIMARY KEY(`idx`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 comment '주간 광고주 리포트';