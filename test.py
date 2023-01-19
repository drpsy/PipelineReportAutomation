import psycopg2
import pandas as pd
from utils import DATABASE_INFO
conn = psycopg2.connect(
    "dbname='mp_tla' user='postgres' host='localhost' password='mp_tla'"
)
cur = conn.cursor()
df = pd.read_excel(
    "/home/viethoang/ai-academy/Gmail_preprocess/data/2020-10-26/thanhpham/01 Số liệu xuất nhập tồn.XLSX"
)
print(df)
rows = zip(df)
print(rows)
cur.execute("""CREATE TEMP TABLE codelist(id INTEGER, z INTEGER) ON COMMIT DROP""")
cur.executemany("""INSERT INTO codelist VALUES(%s, %s)""", rows)

cur.execute(
    """
    UPDATE table_name
    SET z = codelist.z
    FROM codelist
    WHERE codelist.id = vehicle.id;
    """
)

cur.rowcount
conn.commit()
cur.close()
conn.close()
if __name__ == '__main__':
    conn = self.connect(DATABASE_INFO)
    mode = self.getMode(filename1)
    postgresCursor = conn.cursor()
    deleteStatement = """DELETE FROM {tbname} WHERE DATE_PART('month',ngay_in_cuoi) = DATE_PART('month',NOW()) AND DATE_PART('year',ngay_in_cuoi) = DATE_PART('year',NOW());""".format(
        tbname=tbname
    )
    print(deleteStatement)
    postgresCursor.execute(deleteStatement)
    print("delete data from yesterday")
    self.execute_values(conn, df, tbname)