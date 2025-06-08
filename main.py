from asyncio import gather
import logging

import asyncpg
import asyncio
from random import sample, randint
from typing import List, Tuple, Union

CREATE_BRAND_TABLE = """
CREATE TABLE IF NOT EXISTS brand(
brand_id SERIAL PRIMARY KEY,
brand_name TEXT NOT NULL);
"""
CREATE_PRODUCT_TABLE = """
CREATE TABLE IF NOT EXISTS product(
product_id SERIAL PRIMARY KEY,
product_name TEXT NOT NULL,
brand_id INT NOT NULL,
FOREIGN KEY (brand_id) REFERENCES brand(brand_id));
"""

CREATE_PRODUCT_COLOR_TABLE = """
CREATE TABLE IF NOT EXISTS product_color(
product_color_id SERIAL PRIMARY KEY,
product_color_name TEXT NOT NULL);
"""

CREATE_PRODUCT_SIZE_TABLE = """
CREATE TABLE IF NOT EXISTS product_size(
product_size_id SERIAL PRIMARY KEY,
product_size_name TEXT NOT NULL);
"""

CREATE_SKU_TABLE = """
CREATE TABLE IF NOT EXISTS sku(
sku_id SERIAL PRIMARY KEY,
product_id INT NOT NULL,
product_size_id INT NOT NULL,
product_color_id INT NOT NULL,
FOREIGN KEY (product_id)
REFERENCES product(product_id),
FOREIGN KEY (product_size_id)
REFERENCES product_size(product_size_id),
FOREIGN KEY (product_color_id)
REFERENCES product_color(product_color_id));
"""

COLOR_INSERT = """
INSERT INTO product_color VALUES(1, 'Blue');
INSERT INTO product_color VALUES(2, 'Black');
"""

SIZE_INSERT = """
INSERT INTO product_size VALUES(1, 'Small');
INSERT INTO product_size VALUES(2, 'Medium');
INSERT INTO product_size VALUES(3, 'Large')
"""

product_query = """
SELECT
p.product_id,
p.product_name,
p.brand_id,
s.sku_id,
pc.product_color_name,
ps.product_size_name
FROM product as p
JOIN sku as s on s.product_id = p.product_id
JOIN product_color as pc on pc.product_color_id = s.product_color_id
JOIN product_size as ps on ps.product_size_id = s.product_size_id
WHERE p.product_id = 100
"""

def load_common_words() -> List[str]:
    with open('common_words.txt') as common_words:
        result = common_words.readlines()
        result = [line.strip() for line in result]
        return result

def generate_brand_names(words: [List[str]]) -> List[Tuple[Union[str,]]]:
    return [(words[index],) for index in sample(range(100), 100)]

async def insert_brands(common_words, connection) -> int:
    brands = generate_brand_names(common_words)
    insert_brands = 'INSERT INTO brand VALUES(DEFAULT, $1)'
    return await connection.executemany(insert_brands, brands)

def gen_products(common_words: List[str],
                 brand_id_start: int,
                 brand_id_end: int,
                 products_to_create: int) -> List[Tuple[str, int]]:
    products = []
    for _ in range(products_to_create):
        description = [common_words[index] for index in sample(range(100), 10)]
        brand_id = randint(brand_id_start, brand_id_end)
        products.append((''.join(description), brand_id))
    return products



def gen_skus(product_id_start: int,
             product_id_end:  int,
             skus_to_create: int) -> List[Tuple[int, int, int]]:

    skus = []
    for _ in range(skus_to_create):
        product_id = randint(product_id_start, product_id_end)
        size_id = randint(1, 3)
        color_id = randint(1, 2)
        skus.append((product_id, size_id, color_id))

    return skus

async def query_product(pool):
    async with pool.acquire() as connection:
        return await connection.fetchrow(product_query)

async def query_products_synchronously(pool, queries):
    return [await query_product(pool) for _ in range(queries)]

async def query_products_concurrently(pool, queries):
    queries = [query_product(pool) for _ in range(queries)]
    return await asyncio.gather(*queries)

async def main():
    connection = await asyncpg.connect(host='127.0.0.1',
                                       port=5432,
                                       user='postgres',
                                       database='postgres',
                                       password='password')

    try:
        async with connection.transaction():
            insert_brands = "INSERT INTO brand VALUES(9999, 'big_brand')"
            await connection.execute(insert_brands)
            await connection.execute(insert_brands)
    except Exception:
        logging.exception('Ошибка привыполнении транзакции')
    finally:
        query = """SELECT brand_name FROM brand
            WHERE brand_name LIKE 'big_%' """

        brands = await connection.fetch(query)
        print(brands)



    await connection.close()



asyncio.run(main())


