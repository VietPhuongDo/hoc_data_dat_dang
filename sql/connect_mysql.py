import mysql.connector

sql_file_path = 'schema.sql'
config = {
    "host": "localhost",
    "port": 3307,
    "user": "root",
    "password": "A2092002z!",
    "database": "mydb",
    "auth_plugin": "mysql_native_password"
}

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    with open(sql_file_path, "r") as file:
        sql_script = file.read()
        for statement in sql_script.split(";"):
            if statement.strip():
                cursor.execute(statement)

    conn.commit()

    print("SQL done!")
    cursor.close()
    conn.close()
except mysql.connector.Error as err:
    print(f"error: {err}")
