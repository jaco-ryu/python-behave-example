# 주간리포트 테스트 데이터 생성 cucumber (behave - python 버전) 프로젝트

### 구성
 - clickhouse in docker-composer
 - python code using behave ( 참고: [behave](https://github.com/behave/behave) )  

### 설치 
 - docker 설치 
 - python virtualenv로 실행 추천 (pycharm 실행시 자동셋팅 또는 수동 셋팅 가능)
 - python [behave](https://github.com/behave/behave) 라이브러리 설치 
 - python [clickhouse-driver](https://clickhouse-driver.readthedocs.io/en/latest/index.html) 라이브러리 설치

### 클릭하우스 실행 
```
 console> docker-compose up
```

### BDD 실행
```
 console> behave  
 OR   
 console> behave -f plain --no-capture
```