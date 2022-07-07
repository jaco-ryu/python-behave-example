# 주간리포트 테스트 데이터 생성 cucumber (behave - python 버전) 프로젝트

### 구성
 - clickhouse in docker-composer
 - python code using behave ( 참고: [behave](https://github.com/behave/behave) )  

### 설치 
 - git 설치 https://git-scm.com/downloads
 - docker 설치 https://www.docker.com/get-started/
 - pycharm 설치 https://www.jetbrains.com/ko-kr/pycharm/download/
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

### 영석님과 정범님을 위한 가이드

 - docker 설치 https://www.docker.com/get-started/
   - 재시작 후 wsl2 설치
     - ![image](https://user-images.githubusercontent.com/107824702/177742287-7d99643d-bc8f-4ec0-8459-48758f647770.png)
   - ![image](https://user-images.githubusercontent.com/107824702/177742633-919a377b-be5f-4031-a110-24208ba6af43.png)


 
