import csv
import os
import zipfile
from ftplib import FTP

import psycopg2

import pika

import config


def validate(row):
    return len(row) == 5 and row[0] and row[1].isdigit()\
           and int(row[1]) != 0 and row[2] and row[4] and row[3].isdigit() and int(row[3]) > 1


def add_rows_to_db(rows):
    query = "INSERT INTO Orders (orderId, productId, productPrice, productCount, orderDate) VALUES (%s, %s, %s, %s, %s)"
    cur.executemany(query, rows)
    db.commit()


def do_work(filename):
    # download file
    ftp = FTP(host=config.ftp['host'], user=config.ftp['user'], passwd=config.ftp['password'])
    filename = filename.decode('utf8')

    local_file = os.path.join(os.path.dirname(os.path.abspath(__file__)) + '/tmp', filename)
    ftp.retrbinary("RETR " + path + filename, open('tmp/'+filename, 'wb').write)

    # unzip file and validate lines
    archive = zipfile.ZipFile(local_file, 'r')
    filelist = archive.namelist()
    archive.extractall()

    for fname in filelist:
        with open(fname) as f:
            reader = csv.reader(f)
            next(reader, None)
            rows = []

            for row in reader:
                if validate(row):
                    rows.append(row)

            add_rows_to_db(rows)
        os.remove(fname)
    os.remove(local_file)


def callback(channel, method, properties, body):
    do_work(body)
    channel.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":
    url = config.rabbitmq['url']
    params = pika.URLParameters(url)
    params.socket_timeout = 5
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # create temporary download dir
    if not os.path.exists('tmp'):
        os.makedirs('tmp')

    db = None

    try:
        db = psycopg2.connect("host=%s dbname=%s user=%s password=%s" % (config.db['host'], config.db['db_name'],
                                                                         config.db['user'], config.db['password']))
        cur = db.cursor()
        cur.execute("DROP TABLE IF EXISTS Orders")
        cur.execute("CREATE TABLE Orders(orderId INT, productId INT, productPrice NUMERIC, "
                    "productCount INT, orderDate DATE)")

        path = config.ftp['path']
        channel.basic_consume(callback, queue='process-csv')
        channel.start_consuming()

    except psycopg2.DatabaseError as e:
        print('Error: %s') % e

    finally:
        if db:
            db.close()
        connection.close()

