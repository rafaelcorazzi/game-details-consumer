#!/usr/bin/env python
from typing import Any

import pika
import sys
import os
from src.repository.base_dao import Repository
from configuration import configuration

configs: Any = configuration.get_configs()

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    conn = Repository.connect()  # connect to the database
    Repository.copy_from_file(conn,  'public.games', body.decode())  # copy the dataframe to SQL
    conn.close()  # close the connection
    print(" [x] Done")


def main():
    #ToDo: colocar com config
    params = pika.URLParameters(configs['rabbit']['host'])
    connection =  pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=configs['rabbit']['exchange'],
                             exchange_type='topic')  # This method creates or checks a queue
    channel.queue_declare(queue=configs['rabbit']['queue'])
    channel.queue_bind(queue=configs['rabbit']['queue'], exchange=configs['rabbit']['exchange'],routing_key = configs['rabbit']['topic'])
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=configs['rabbit']['queue'], on_message_callback=callback, auto_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)