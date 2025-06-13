from asyncio import gather
import logging

import asyncpg
import asyncio
import aiohttp
from random import sample, randint
from typing import List, Tuple, Union

import socket
import selectors
from selectors import SelectorKey
from asyncio import AbstractEventLoop

from aiohttp import ClientSession
from util import async_timed, fetch_status


@async_timed.async_timed()
async def main():
    async with aiohttp.ClientSession() as session:
        urls = ["https://www.exaple.com" for _ in range(1000)]
        request = [fetch_status.fetch_status(session, url) for url in urls]
        status_codes = await asyncio.gather(*request)
        print(status_codes)


asyncio.run(main())







