import time
import uuid

from _ctypes_test import func
from numpy import array
from opcua import Client, Server, ua, uamethod
from kafka import KafkaConsumer, KafkaProducer
import json
import sys
import os
from datetime import datetime
import threading
from queue import Queue
import _thread

# Имя приложения
from opcua.ua import NodeId, NodeIdType

INSTANCE_NAME = os.environ.get('INSTANCE_NAME', 'new-kafka-opcua')
# Параметры подключения для OPC UA
OPCUA_SERVER = os.environ.get('OPCUA_SERVER', '0.0.0.0')
OPCUA_PORT = os.environ.get('OPCUA_PORT', '4840')
# Параметры подключения для Apache Kafka
KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'localhost')
KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'new')
# Время между записями в Apache Kafka
SLEEP_DURATION = os.environ.get('SLEEP_DURATION', '1')
# Лог пишем сюда
OUT_FILE = os.environ.get('OUT_FILE', 'response_time.json')
# Сколько пишем сообщений
NUMBER_MESSAGES = os.environ.get('NUMBER_MESSAGES', 10)

number_messages = 0
q = Queue()
file = open(OUT_FILE, "w")

# Создаем инстанс для KafkaConsumer'а
kafka_consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers='%s:%s' % (KAFKA_SERVER, KAFKA_PORT),
                             # auto_offset_reset='earliest',
                             consumer_timeout_ms=30000,
                             api_version=(0, 11, 1))

try:
    from IPython import embed
except ImportError:
    import code

    def embed():
        myvars = globals()
        myvars.update(locals())
        shell = code.InteractiveConsole(myvars)
        shell.interact()

@uamethod
def multiply(parent, x, y):
    print("multiply method call with parameters: ", x, y)
    return x * y

def _monitor_response_time(kafka_consumer, q):
    global number_messages
    while True:
        try:
            # Берем следующее сообщение из топика
            message = next(kafka_consumer)
            # Время получения сообщения
            timestamp_received = datetime.now()
            # Получение сообщения в виде dict
            msg = json.loads(message.value.decode('utf-8'))
            # Время отправки
            timestamp_sent = datetime.strptime(str(msg['TimestampSent']), '%Y-%m-%d %H:%M:%S.%f')
            # Вычисляем время ответа сервера (время отправления минус время получения) и выводим его
            response_time = timestamp_received - timestamp_sent
            print("Response time: %f seconds" % response_time.total_seconds())
            # Пихаем в очередь время ответа
            q.put(response_time.total_seconds())
            # Работаем с количеством сообщений, проверка на >= NUMBER_MESSAGES и выход, если это так
            number_messages += 1
            if int(NUMBER_MESSAGES) > 0:
                if number_messages >= int(NUMBER_MESSAGES):
                    _thread.interrupt_main()
                    sys.exit()
        except StopIteration:
            pass
        # except Exception:
        #     pass

def _server_handling():
    # Базовые конфигурации - создание инстанса сервера, настройка коннекта и создание Namespace'а
    server = Server()
    server.set_endpoint("opc.tcp://%s:%s/freeopcua/server/" % (OPCUA_SERVER, OPCUA_PORT))
    idx = server.register_namespace("Rack1")

    # Создание типа объекта, сенсора и контроллера
    dev = server.nodes.base_object_type.add_object_type(idx, "MAIDevice")
    dev.add_variable(idx, "sensor1", 1.0).set_modelling_rule(True)
    dev.add_property(idx, "device_id", "0340").set_modelling_rule(True)
    ctrl = dev.add_object(idx, "controller")
    ctrl.set_modelling_rule(True)
    ctrl.add_property(idx, "state", "Idle").set_modelling_rule(True)

    # Создаем папку, добавляем девайс и переменную девайса
    myfolder = server.nodes.objects.add_folder(idx, "myEmptyFolder")
    mydevice = server.nodes.objects.add_object(idx, "Device0001", dev)
    mydevice_var = mydevice.get_child(["{}:controller".format(idx), "{}:state".format(idx)])

    # Создаем объект, вариабл для него
    myobj = server.nodes.objects.add_object(idx, "MAIObject")
    myvar = myobj.add_variable(idx, "MAIVariable", 6.7)
    mysin = myobj.add_variable(idx, "MAISyn", 0, ua.VariantType.Float)

    # Разрешаем менять значение
    myvar.set_writable()

    # Добавляем еще переменную (строковую) и делаем ее изменяемой
    mystringvar = myobj.add_variable(idx, "MAIStringVariable", "Really nice string")
    mystringvar.set_writable()

    # Создаем ноду
    myguidvar = myobj.add_variable(NodeId(uuid.UUID('1be5ba38-d004-46bd-aa3a-b5b87940c698'), idx, NodeIdType.Guid),
                                   'MyStringVariableWithGUID', 'NodeId type is guid')

    # Новая переменная - теперь дата
    mydtvar = myobj.add_variable(idx, "MAIDateTimeVar", datetime.utcnow())
    mydtvar.set_writable()

    # Переменная - массив
    myarrayvar = myobj.add_variable(idx, "MAIarrayvar", [6.7, 7.9])

    # Добавляем свойство и метод к myObj
    myprop = myobj.add_property(idx, "myproperty", "I am a property")
    mymethod = myobj.add_method(idx, "mymethod", func, [ua.VariantType.Int64], [ua.VariantType.Boolean])

    # Делаем мгного нод
    multiply_node = myobj.add_method(idx, "multiply", multiply, [ua.VariantType.Int64, ua.VariantType.Int64],
                                     [ua.VariantType.Int64])

    # Создаем генератор событий с интервалом 300
    myevgen = server.get_event_generator()
    myevgen.event.Severity = 300

    server.start()

    time.sleep(float(5 * NUMBER_MESSAGES))
    server.stop()

if __name__== "__main__":

    # Создаем дочерний поток (поток демон) и запускаем его для Kafka Consumer'а
    thread = threading.Thread(target=_monitor_response_time, args=(kafka_consumer, q,))
    thread.daemon = True
    thread.start()

    # Создаем сервак в отдельном потоке и меняем данные в цикле
    serverThread = threading.Thread(target=_server_handling, args=())
    serverThread.daemon = True
    serverThread.start()
    time.sleep(float(10))

    # Создаем клиент и коннектимся нашему серваку
    opcua_client = Client("opc.tcp://localhost:%s/freeopcua/server/" % OPCUA_PORT, timeout=5000)
    opcua_client.connect()
    uri = "http://examples.freeopcua.github.io"
    idx = opcua_client.get_namespace_index("Rack1")
    print(idx)
    print('Connected to OPC UA server localhost:%s/freeopcua/server/' % OPCUA_PORT)
    root = opcua_client.get_root_node()
    # IMMS - Injection Molding Machines - s как окончание
    imms = root.get_child(["0:Objects", "{}:MAIObject".format(idx)])

    # Создаем продьюсер Kafka, закомменчены доп типы сжатия
    kafka_producer = KafkaProducer(bootstrap_servers='%s:%s' % (KAFKA_SERVER, KAFKA_PORT),
                                retries=5,
                                batch_size=0,
                                compression_type=None,
                                api_version=(0, 11, 1))
                                # compression_type='gzip')
                                # compression_type='snappy')
                                # compression_type='lz4')
    kafka_producer.flush()

    print('Connected to Kafka %s:%s' % (KAFKA_SERVER, KAFKA_PORT))

    while True:
        try:
            # Собираем сообщение, используя инстанс IMMS
            msg = {
                "MAIVariable": float(imms.get_variables()[0].get_value()),
                "MAISyn": float(imms.get_variables()[1].get_value()),
                "MAIStringVariable": str(imms.get_variables()[2].get_value()),
                "MAIDateTimeVar": str(imms.get_variables()[3].get_value()),
                "MAIarrayvar": array(imms.get_variables()[4].get_value()),
                "InstanceName": INSTANCE_NAME,
                "TimestampSent": datetime.now()
            }

            # Отправляем продьюсером сообщение в топик KAFKA_TOPIC
            future = kafka_producer.send(KAFKA_TOPIC, json.dumps(msg, default=str).encode('utf-8'))
            kafka_producer.flush()
            print('Update written to topic %s' % KAFKA_TOPIC)

            # Спим, сколько указали в env
            time.sleep(float(SLEEP_DURATION))

            # Безопасно закрываем opcua, Kafka consumer и producer
        except KeyboardInterrupt:
            try:
                opcua_client.disconnect()
                kafka_producer.close()
                kafka_consumer.close()
            except Exception:
                pass
            finally:
                print("Writing response times to", OUT_FILE)
                file.write(json.dumps(list(q.queue)))
                file.close()
                sys.exit()
