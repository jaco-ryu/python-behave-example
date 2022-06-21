from clickhouse_driver import Client

from QUERY.clickhouse import TARGET_URL


def upsert_click_house_query(query: str):
    Client(TARGET_URL).execute(query)  # print(query)


def execute_click_house_multi_line_ddl(multi_line_ddl: str):
    for query in multi_line_ddl.split(";"):
        sql = query.strip()
        if not '' == sql or not "" == sql:
            try:
                Client(TARGET_URL).execute(sql + ";")  # print(sql)
            except:
                print("SQL ERROR : | {} |".format(sql))
                raise


def select_one_click_house_query(query: str):
    return Client(TARGET_URL).execute(query)
