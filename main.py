from asyncio import gather
import logging

import asyncpg
import asyncio
from random import sample, randint
from typing import List, Tuple, Union

import socket
import selectors
from selectors import SelectorKey
from asyncio import AbstractEventLoop

async def echo(connection: socket, loop: AbstractEventLoop):
    while data := await loop.sock_recv(connection, 1024):
        print(f'Данные получены {data}')
        await loop.sock_sendall(connection, data)

async def listen_for_connections(server_socket: socket, loop: AbstractEventLoop):
    while True:
        connection, address = await loop.sock_accept(server_socket)
        connection.setblocking(False)
        print(f'Получен запрос на подключение {address}')
        asyncio.create_task(echo(connection, loop))


async def main():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)


    server_address = ('127.0.0.1', 8000)
    server_socket.bind(server_address)
    server_socket.listen()

    await listen_for_connections(server_socket, asyncio.get_event_loop())


asyncio.run(main())







