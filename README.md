# kafka_OPC-UA
Для запуска:
1) Запустить брокер kafka (docker-compose)
2) Запустить проект через Dockerfile или PyCharm (python file)
3) Результаты выполнения запросов (время отклика) будет выведено в response_time.json
4) Новые состояния объекта в OPC-UA обновляются с помощью API
5) utils_file содержит начальные значения:
    MAIVariable 6.7 float
    MAISyn 0 ua.VariantType.Float
    MAIStringVariable "Really nice string" String
    MAIDateTimeVar utcnow DATE
    MAIarrayvar [6.7, 7.9] Array
И команду для запуска Dockerfile из текущей директории
6) pip3libs.txt - необходимые библиотеки, подгружаются сами
7) run.sh запускает .py файл в контейнере
