from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text
from fastapi import Depends
from redis.asyncio import Redis
from dotenv import load_dotenv
import os

load_dotenv()


redis_db_host = os.getenv('REDIS_DB_HOST')
redis_db_port = int(os.getenv('REDIS_DB_PORT'))
redis_db_db = int(os.getenv('REDIS_DB_DB'))

redis_connect = Redis(host=redis_db_host, port=redis_db_port, db=redis_db_db)


sql_db_user = os.environ.get('SQL_DB_USER')
sql_db_pass = os.environ.get('SQL_DB_PASSWORD')
sql_db_host = os.environ.get('SQL_DB_HOST')
sql_db_port = int(os.environ.get('SQL_DB_PORT'))
sql_db_name = os.environ.get('SQL_DB_NAME')

database_url = f"postgresql+asyncpg://{sql_db_user}:{sql_db_pass}@{sql_db_host}:{sql_db_port}/{sql_db_name}"
print("fastapi conn sql",sql_db_user, sql_db_pass, sql_db_host, sql_db_port, sql_db_name)
print(database_url)

postgresql_user_connect = create_async_engine(database_url, echo=True)

async_session = sessionmaker(postgresql_user_connect, class_=AsyncSession, expire_on_commit=False)
print("fastapi conn redis",redis_db_db, redis_db_host, redis_db_port)

schema_main_info = "data."


# async def get_session() -> AsyncSession:
#     async with async_session() as session:
#         yield session

async def fast_req_sessions(query):
    async with async_session() as session:
        result = await session.execute(text(query))
        rows = result.mappings().all()
        return rows

async def post_fast_req_sessions(query, params:dict = {}):
    try:
        async with async_session() as session:
            await session.execute(text(query), params)
            await session.commit()
        return True
    except Exception as e:
        print(e)
        return False

async def search_update_count(id_object, object_type):
    # print('object_type', object_type)
    if await object_existence(
            f'select count(id_object) from {schema_main_info}number_of_search_objects where id_object = {id_object} and object_type = {object_type}'):
        await post_fast_req_sessions(
            f"update {schema_main_info}number_of_search_objects set count_search = count_search + 1 where id_object = {id_object} and object_type = {object_type}")
    else:
        await post_fast_req_sessions(
            f"insert into {schema_main_info}number_of_search_objects (count_search, id_object, object_type) values (1, {id_object}, {object_type})")


async def object_existence(query):
    """
    query must have count()
    :param query:
    :return:
    """
    async with async_session() as session:
        result = await session.execute(text(query))
        rows = result.mappings().all()
        if rows[0]['count'] == 0:
            return False
        else:
            return True

async def register_user_inter_object(id_object, object_type , user_id):
    """
    func
    :param id_object: id title/ person/ user/ review where user logged in to
    :param object_type: 1 - title, 2 - person, 3 - user, 4 - review
    :param user_id: who logged in to
    :return: True/ False
    """
    return await post_fast_req_sessions(f"""
    insert into pub_user.user_inter_objects(id_object, object_type, id_user_django) 
    values ({id_object},{object_type},{user_id})""")