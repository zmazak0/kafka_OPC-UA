0)	Склонировать репозиторий https://github.com/zmazak0/kafka_OPC-UA 
  git clone https://github.com/zmazak0/kafka_OPC-UA  
1)	Запустить docker-compose:
  cd kafka_OPC-UA
  docker-compose up –d
2)	Установить необходимые python библиотеки:
  pip install -r .\requirements.txt 
3)	Добавить topic в kafka:
  docker exec -it kafka bash
  cd opt/bitnami/kafka/bin
  kafka-topics.sh --create --topic MAI_IoT --bootstrap-server 127.0.0.1:9092
  exit
4)	Запустить скрипт main.py
  python .\main.py
		 
Результат записался в response_time.json и в консоль.
 
