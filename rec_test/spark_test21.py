
import psycopg2
conn = psycopg2.connect(host="121.40.28.12",dbname="bymyhopin_django2",user="odoo",password="bysun123")
sql_payment = "select uid,pid from data_payment"
sql_order = "select uid,pid from data_order"
sql_cart = "select uid,pid from data_cart"
sql_favorites = "select uid,pid from data_favorites"
cur = conn.cursor()
data_payment = cur.execute(sql_payment)
data_order = cur.execute(sql_order)
data_cart = cur.execute(sql_cart)
data_favorites = cur.execute(sql_favorites)


conn = psycopg2.connect(dbname="sparktest",user="leo",password="123123")
cur = conn.cursor()
sql = "select * from user_rating;"
cur.execute(sql)
print cur.fetchone()

