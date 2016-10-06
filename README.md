# ftp2db
Simple cli app which downloads scv from ftp and puts it to database

Для запуска нужно добавить в config.py нужные параметры конфигурации
ftp сервера, базы данных и rabbitmq и запустить нужные сервисы.

В `consumer.py` содержится код воркеров, `start.py` рассылает им сообщения.

Нужные библиотеки можно установить с помощью команды
``` bash
pip install -r requirements.txt
```

