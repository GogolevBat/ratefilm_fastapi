from fastapi import APIRouter, Depends

from ..schemas import *
from ..dependencies import *
import json, asyncio

router = APIRouter(
    # prefix="/title",
    tags=["persons"],
    responses={404: {"description": "Not found"}}
                    )

@router.get("/{person_id}")
async def person(person_id: int, user_id: str|int ):

    print(person_id)
    # print('\n\n\n',user_id,'\n\n\n')
    if not(user_id=='None'):
        # await sql_functions.refister_user_inter_object(person_id, False, user_id)
        return {"message": '', "result": await person_show_on_django(person_id) | await user_data_person_on_django(person_id, user_id)}
    else:
        return {"message": '', "result": await person_show_on_django(person_id)}

async def user_data_person_on_django(id_person, user_id):
    content_for_django = {}
    # Получаем пользовательские данные
    content_for_django["isfavorite"] = await is_favorite(user_id, id_person, 1)
    # print(content_for_django)
    await register_user_inter_object(id_person, 2 ,user_id)
    return content_for_django

async def is_favorite(id_user, id_object, type_object):
    return int(await object_existence(
        f'Select count(favorite) from pub_user.user_actions where id_user_django = {id_user} and object_type = {type_object} and id_object = {id_object} and not (favorite is NULL)',
        ))

async def person_show_on_django(id_person):
    # await line_is_create(user_id, id_person, 2)
    # print('film_show_on_django')
    content_for_django = {}
    # if False:
    key_redis = f"p:{id_person}"

    if await redis_connect.exists(key_redis):
        content_for_django = json.loads(await redis_connect.get(key_redis))
    else:
        content_for_django["person"] = await person_info(id_person)
        # Получаем список фильмов где человаек принял участие
        content_for_django['titles'] = await movies_where_person_present(id_person)
        # Получаем список лучших работ
        content_for_django['top_titles'] = await top_movies_where_person_present(id_person)
        await redis_connect.set(key_redis, json.dumps(content_for_django))


    await search_update_count(id_person, 1)
    return content_for_django

async def person_info(id_person):
    result = await fast_req_sessions(f"""select name, photo, enname, age, sex, growth from {schema_main_info}persons where id = {id_person}""")
    content = [dict(Persons.model_validate(row, from_attributes=True)) for row in result]
    return content[0]

async def movies_where_person_present(id_person):
    result = await fast_req_sessions(f"""select ms.id, ms.name, ms.poster, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, COALESCE(rf.rate, 0) as rate_film, ms.year_create, STRING_AGG(prof.name, ', ') as name_profession, STRING_AGG(COALESCE(fp.description, ''), ' ') as description
                                from {schema_main_info}film_person fp 
                                left join {schema_main_info}my_spisok ms on ms.id = fp.id_movie
                                left join {schema_main_info}proffesions prof on prof.id = fp.id_proffesion
                                left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
                                where fp.id_person = {id_person} and not(ms.id is Null) and not (ms.year_create is Null)
                                group by ms.id, ms.name, ms.poster, ms.kp_rate, ms.imdb_rate, ms.year_create, rf.rate
                                order by ms.year_create Desc""")
    content = [dict(My_spisok_with_person_info.model_validate(row, from_attributes=True)) for row in result]
    return content

async def movies_where_person_present(id_person):
    result = await fast_req_sessions(f"""select ms.id, ms.name, ms.poster, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, COALESCE(rf.rate, 0) as rate_film, ms.year_create, STRING_AGG(prof.name, ', ') as name_profession, STRING_AGG(COALESCE(fp.description, ''), ' ') as description
                                from {schema_main_info}film_person fp 
                                left join {schema_main_info}my_spisok ms on ms.id = fp.id_movie
                                left join {schema_main_info}proffesions prof on prof.id = fp.id_proffesion
                                left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
                                where fp.id_person = {id_person} and not(ms.id is Null) and not (ms.year_create is Null)
                                group by ms.id, ms.name, ms.poster, ms.kp_rate, ms.imdb_rate, ms.year_create, rf.rate
                                order by ms.year_create Desc""")
    content = [dict(My_spisok_with_person_info.model_validate(row, from_attributes=True)) for row in result]
    return content

async def top_movies_where_person_present(id_person):
    result = await fast_req_sessions(f"""select ms.id, ms.name, ms.poster, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, COALESCE(rf.rate, 0) as rate_film, ms.imdb_rate, ms.year_create, STRING_AGG(prof.name, ', ') as name_profession, STRING_AGG(COALESCE(fp.description, ''), ' ') as description 
                                from {schema_main_info}film_person fp 
                                left join {schema_main_info}my_spisok ms on ms.id = fp.id_movie
                                left join {schema_main_info}proffesions prof on prof.id = fp.id_proffesion
                                left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
                                where fp.id_person = {id_person} and not(ms.id is Null) and  not(prof.id in (10, 11)) and fp.place_kinopoisk <= 5
                                group by ms.id, ms.name, ms.poster, ms.kp_rate, ms.imdb_rate, ms.year_create, rf.rate
                                order by ms.kp_rate Desc limit 5""")
    content = [dict(My_spisok_with_person_info.model_validate(row, from_attributes=True)) for row in result]
    return content

@router.post("/favoriteit/{person_id}")
async def person_favorite_it(person_id: int, user_id: int ):
    print(person_id)
    return {"message": '', "result": await favorite_it(user_id, person_id, 1)}

@router.post("/favoriteout/{person_id}")
async def person_favorite_out(person_id: int, user_id: int ):
    print(person_id)
    return {"message": '', "result": await favorite_out(user_id, person_id, 1)}

async def favorite_it(id_user, id_object, type_object):
    await line_is_create(id_user, id_object, type_object)
    await post_fast_req_sessions(
        f"""update pub_user.user_actions set favorite = now() where id_user_django = {id_user} and object_type = {type_object} and id_object = {id_object} """)

async def favorite_out(id_user, id_object, type_object):
    await post_fast_req_sessions(
        f"""update pub_user.user_actions set favorite = null where id_user_django = {id_user} and object_type = {type_object} and id_object = {id_object} """)

async def line_is_create(id_user, id_object, type_object):
    if not (await object_existence(
            f'Select count(id_user_django) from pub_user.user_actions where id_user_django = {id_user} and object_type = {id_object} and id_object = {type_object} ')):
        await insert_line_user_action(id_user, id_object, type_object)

async def insert_line_user_action(id_user, id_object, type_object):
    await post_fast_req_sessions(
        f"""insert into pub_user.user_actions (id_user_django, object_type, id_object) values ({id_user},{type_object},{id_object})""")

@router.get("/mini/{person_id}")
async def person(person_id: int):
    print(person_id)
    return {"message": '', "result": await person_show_on_django_mini(person_id)}

async def person_show_on_django_mini(id_person):
    # print('film_show_on_django')
    content_for_django = {}
    # if False:
    key_redis = f"m:p:{id_person}"

    if await redis_connect.exists(key_redis):
        content_for_django["person"] = json.loads(await redis_connect.get(key_redis))
    else:
        content_for_django["person"] = await person_info(id_person)
        await redis_connect.set(key_redis, json.dumps(content_for_django["person"]))
    # Получаем список фильмов где человаек принял участие
    return content_for_django
