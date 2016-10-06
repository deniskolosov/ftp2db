import pika

from ftplib import FTP

import config


ftp = FTP(host=config.ftp['host'], user=config.ftp['user'], passwd=config.ftp['password'])


url = 'amqp://guest:guest@localhost/%2f'

params = pika.URLParameters(url)
params.socket_timeout = 5

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='process-csv')


filelist = ftp.mlsd(path=config.ftp['path'])

for el in filelist:
    name = el[0]
    if '.zip' in name:
        channel.basic_publish(exchange='', routing_key='process-csv', body=name)


connection.close()

