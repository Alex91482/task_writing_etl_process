# Пример построения ETL с инкрементальной загрузкой

- Запуск сборки приложения
```commandline
docker-compose up -d
```

При запуске выполняется скрипт ./data/sql/ddl_dml.sql
И дополняются зависимости из файла requirements.txt

- Airflow (логин: admin, пароль: admin)
```
http://localhost:8080
```

- MinIo (логин: admin, пароль: your_strong_password)
```
http://localhost:9001
```
