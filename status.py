import threading
from time import sleep

from flask import Flask,render_template, request
from flask_mysqldb import MySQL
import csv
import io
import calendar
from datetime import datetime

app = Flask(__name__, template_folder = '.')

app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = '4.7.200Cp'
app.config['MYSQL_DB'] = 'mynewdb'

mysql = MySQL(app)

@app.route("/")
def hello():
    return render_template('form.html')

@app.route("/dataupload", methods = ['POST'])
def dataupload():
    cursor = mysql.connection.cursor()
    table_name = ''
    count_proceed = 0
    try:
        content = request.files["fileToUpload"]
        if not content:
            return "No File"
        stream = io.StringIO(content.stream.read().decode("UTF-8"), newline = None)
        csv_input = csv.reader(stream)
        print(csv_input)
        headers = next(csv_input, None)
        input_array = []
        name=calendar.timegm(datetime.now().timetuple())
        table_name = str(name) + 's'
        for x in headers:
            input_array.append(x.lower().replace(" ","_") + " VARCHAR(255)")
        input_query = "Create Table {} (".format(table_name) + ",".join(input_array) + ");"
        print(input_query)
        cursor.execute(input_query)
        mysql.connection.commit()
        for row in csv_input:
            count_proceed = count_proceed + 1
            insertquery = "insert into " + table_name + " values (" + ",".join(
                map(lambda x: '\'' + str(x) + '\'', row)) + ");"
            print(insertquery)
            cursor.execute(insertquery)
            mysql.connection.commit()

    finally:
        cursor.close()
        print('executed successfully')
    return "Uploaded Successfully | table name (" + table_name + ") Number of record (" + len(count_proceed) + ")"

@app.route("/status")
def status():
    return "Hello World!"

if __name__ == "__main__":
    app.run()