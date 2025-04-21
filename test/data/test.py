import sqlite3
import pandas as pd

con = sqlite3.connect("../../pj_1(confluent)/sqlite_db/test.db")

cur = con.cursor()
cur.execute("SELECT * FROM VEHICLE_ING_INFO")

rows = cur.fetchall()
df = pd.DataFrame(rows, columns=[column[0] for column in cur.description])
df = df.dropna(subset=['TTC','ING_ACCELERAION'])


#df = df.isnull().sum(axis=0)
print(df)
cur.close()