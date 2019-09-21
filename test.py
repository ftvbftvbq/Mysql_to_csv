
import time
import logging
import pymysql
import pandas as pd

def Mysql_To_CSV(host,dbname,tbname,user,passwd,num):

	connected = False
	while connected is False:
		try:
			db_con = pymysql.connect(host=host,user=user,passwd=passwd,db=dbname,port=10142,charset='utf8mb4')
			db_cur = db_con.cursor()

			db_cur.execute('SET NAMES utf8mb4')
			db_cur.execute("SET CHARACTER SET utf8mb4")
			db_cur.execute("SET character_set_connection=utf8mb4")
			logging.info(str(dbname) + ' 数据库连接成功。')
			#return db_con, db_cur
			break

		except Exception as e:
			time.sleep(1)
			connected = False
			logging.error(e)
			logging.error('数据库连接失败{}，正在尝试重新连接……'.format(e))
	datas = []
	columns = []
	#Sql = f'''SELECT * FROM {tbname}'''
	Sql_A = "SELECT COLUMN_NAME FROM information_schema.COLUMNS  WHERE TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (dbname, tbname)
	try:
		db_cur.execute(Sql_A)
		column = db_cur.fetchall()
	except Exception:
		logging.error("获取数据出错")


	datas = {}

	Sql_B = f"SELECT * FROM {tbname}"
	try:
		db_cur.execute(Sql_B)
		data = db_cur.fetchall()
	except Exception as e:
		logging.error("获取数据出错")
		logging.error(e)

	Sql_C = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (dbname, tbname)



	try:
		db_cur.execute(Sql_C)
		keys = db_cur.fetchall()
	except Exception:
		logging.error("获取数据出错")

	columns = []
	for col_ in column:
		columns.append(col_[0])


	temp = 0
	data = list(data)
	for i in range(0,len(data),num):

		print(f"正在写入分表{temp}")

		file = pd.DataFrame(data[i:i+num],columns=columns)

		if len(keys) != 0:
			file = file.set_index(keys[0][0])

	
		file.to_excel(f'case_{temp}.xlsx')
	
		print(f"分表{temp}写入完成")
		temp += 1


Mysql_To_CSV('cdb-hluivpkc.bj.tencentcdb.com','Bilibili','Video','root','byl091022',1000)