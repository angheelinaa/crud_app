import pandas as pd
import sqlite3

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

def csv_to_sql(path, table_name):
	df = pd.read_csv(path)
	df.to_sql(table_name, con = conn, if_exists = 'replace', index = False)

def show_data(source):
	print('_-' * 20)
	print(source)
	print('_-' * 20)
	cursor.execute(f'SELECT * FROM {source}')
	for row in cursor.fetchall():
		print(row)
	print('_-' * 20)

def init_auto_hist():
	cursor.execute('''
		CREATE TABLE if not exists auto_hist(
			id integer primary key autoincrement,
			model varchar(128),
			transmission varchar(128),
			body_type varchar(128),
			drive_type varchar(128),
			color varchar(128),
			production_year integer,
			auto_key integer,
			engine_capacity real,
			horsepower integer,
			engine_type varchar(128),
			price integer,
			milage integer,
			deleted_flg integer default 0,
			start_dttm datetime default current_timestamp,
			end_dttm datetime default (datetime('2999-12-31 23:59:59'))
		)
	''')

	cursor.execute('''
		CREATE VIEW if not exists v_auto as 
			SELECT
				model,
				transmission,
				body_type,
				drive_type,
				color,
				production_year,
				auto_key,
				engine_capacity,
				horsepower,
				engine_type,
				price,
				milage
			FROM auto_hist
			WHERE deleted_flg = 0
			AND current_timestamp between start_dttm and end_dttm
	''')


def create_new_rows():
	cursor.execute('''
		CREATE TABLE new_rows_tmp as
			SELECT
				t1.*
			FROM auto_tmp t1 
			LEFT JOIN v_auto t2
			ON t1.auto_key = t2.auto_key
			WHERE t2.auto_key is null
	''')

def create_deleted_rows():
	cursor.execute('''
		CREATE TABLE deleted_rows_tmp as 
			SELECT
				t1.*
			FROM v_auto t1 
			LEFT JOIN auto_tmp t2
			ON t1.auto_key = t2.auto_key
			WHERE t2.auto_key is null
	''')

def create_changed_rows():
	cursor.execute('''
		CREATE TABLE changed_rows_tmp as
			SELECT
				t1.*
			FROM auto_tmp t1
			INNER JOIN v_auto t2
			ON t1.auto_key = t2.auto_key
			AND (t1.model <> t2.model
			OR t1.transmission <> t2.transmission
			OR t1.body_type <> t2.body_type
			OR t1.drive_type <> t2.drive_type
			OR t1.color <> t2.color
			OR t1.production_year <> t2.production_year
			OR t1.engine_capacity <> t2.engine_capacity
			OR t1.horsepower <> t2.horsepower
			OR t1.engine_type <> t2.engine_type
			OR t1.price <> t2.price
			OR t1.milage <> t2.milage)
	''')

def update_auto_hist():
	cursor.execute('''
		INSERT INTO auto_hist (
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage
		) SELECT
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage
		FROM new_rows_tmp
	''')

	cursor.execute('''
		UPDATE auto_hist
		SET end_dttm = datetime('now', '-1 second')
		WHERE auto_key in (SELECT auto_key FROM changed_rows_tmp)
		AND end_dttm = datetime('2999-12-31 23:59:59')
	''')
	cursor.execute('''
		INSERT INTO auto_hist (
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage
		) SELECT
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage
		FROM changed_rows_tmp
	''')

	cursor.execute('''
		UPDATE auto_hist
		SET end_dttm = datetime('now', '-1 second')
		WHERE auto_key in (SELECT auto_key FROM deleted_rows_tmp)
		AND end_dttm = datetime('2999-12-31 23:59:59')
	''')
	cursor.execute('''
		INSERT INTO auto_hist (
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage,
			deleted_flg
		) SELECT
			model,
			transmission,
			body_type,
			drive_type,
			color,
			production_year,
			auto_key,
			engine_capacity,
			horsepower,
			engine_type,
			price,
			milage,
			1
		FROM deleted_rows_tmp
	''')

	conn.commit()

def drop_tmp_tables():
	cursor.execute('DROP TABLE if exists auto_tmp')
	cursor.execute('DROP TABLE if exists new_rows_tmp')
	cursor.execute('DROP TABLE if exists deleted_rows_tmp')
	cursor.execute('DROP TABLE if exists changed_rows_tmp')

init_auto_hist()
drop_tmp_tables()
csv_to_sql('store/data_1.csv', 'auto_tmp')
create_new_rows()
create_deleted_rows()
create_changed_rows()
update_auto_hist()

show_data('auto_tmp')
show_data('new_rows_tmp')
show_data('deleted_rows_tmp')
show_data('changed_rows_tmp')
show_data('auto_hist')


