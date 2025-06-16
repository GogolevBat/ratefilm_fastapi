from fastapi import APIRouter, Request

from ..schemas import *
from ..dependencies import *
import json, asyncio, time

router = APIRouter(
    # prefix="/title",
    tags=["title"],
    responses={404: {"description": "Not found"}}
                    )
@router.post('/{title_id}/save/review')
async def save_review(request:Request, title_id:int):
    body = await request.body()
    await review_save(body,title_id)

async def review_save(body, title_id):
    data = json.loads(body)
    # print(data)
    await post_fast_req_sessions(
        f'insert into {schema_main_info}reviews (date_review, id_user_django, id_movie, type_review, title_name, title_content) values(now(), :id_user_django, :id_movie, :type_review, :title_name, :title_content)',
        {"id_user_django": data['user_id'],
                 "id_movie": title_id,
                 "type_review": int(data['type']),
                 "title_name": data['title'],
                 "title_content": data['text']})

@router.get("/{title_id}/reviews")
async def title_reviews(title_id: int, page: int):
    return {"message": 'title_reviews', "result": await show_reviews_on_title(title_id, page)}

async def show_reviews_on_title(title_id, page):
    result = (await fast_req_sessions(f"""select ms.poster, ms.name, ms.id, ms.imdb_rate, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate,rf.rate as rate_film
                                                    from {schema_main_info}my_spisok ms
                                                    left join {schema_main_info}rate_film rf on rf.id_movie = ms.id 
                                                    where ms.id = {title_id}"""))
    content = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]

    profile_data = {
        'title': content[0],
        'reviews': await title_page_reviews(title_id, page),
        }
    return profile_data

async def title_page_reviews(title_id, page):
    count_data = (page - 1) * 20
    query = f"""
        select u.nickname, u.block_account  ,uh.hash_id as profile_hash_user_id ,r.title_name, r.type_review ,r.title_content, TO_CHAR(r.date_review, 'YYYY-MM-DD HH24:MI') as date_review, COALESCE(ua.rate, 0) as user_rate,
        (select count(ra.user_dislike) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_dislike is null)) as count_dislike,
        (select count(ra.user_like) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_like is null)) as count_like
        from {schema_main_info}reviews r  
        left join pub_user.user_actions ua on ua.object_type = 0 and ua.id_object = r.id_movie and r.id_user_django = ua.id_user_django
        left join pub_user.user_hash uh on uh.id_user_django = r.id_user_django
        left join pub_user.users u on u.id_user_django = r.id_user_django
        where r.id_movie = {title_id}
        order by r.date_review DESC
        {f'offset {count_data}' if count_data > 0 else ''} Limit {20 + count_data}
        """
    result = await fast_req_sessions(query)
    return [dict(Reviews_my_spisok_with_user_rate.model_validate(row, from_attributes=True)) for row in result]

@router.get("/{title_id}")
async def title(title_id: int, user_id: Optional[str] = None ):
    object_type = 0
    print('usering', title_id, user_id)
    if user_id == "None":
        user_id = None
    if user_id:
        # await sql_functions.refister_user_inter_object(title_id, True, user_id)
        return {"message": '', "result": await title_show_on_django(title_id) | await user_data_film_on_django(title_id, object_type, user_id)}
    else:
        return {"message": '', "result": await title_show_on_django(title_id)}


async def title_show_on_django(title_id):
    content_for_django = {}
    key_redis = f"t:{title_id}"

    if False:
            # await redis_connect.exists(key_redis):
        # print("redis")
        content_for_django = json.loads(await redis_connect.get(key_redis))
    else:
        # print("sql")
        content_for_django["dict_title"] = await my_spisok_title_get(title_id)

        # Получаем список жанров
        content_for_django['genres'] = await genres_for_id_movie(title_id)
        # Получаем список стран
        content_for_django['countries'] = await countries_for_id_movie(title_id)
        # print(content_for_django['countries'])
        # Получаем список сиквелов и приквелов
        content_for_django["seq_and_preq"] = await sequils_and_prequels_for_id_movie(title_id)
        # Получаем список похожих фильмов
        content_for_django["similars"] = await similars_movies_for_id_movie(title_id)
        # Получаем список актеров
        content_for_django["persons"] = await movies_persons_get_list(title_id)
        # await redis_connect.set(key_redis,
        #                   json.dumps(content_for_django))  # {'count':1, 'date':datetime.now().strftime("%m/%d/%Y")}

    # Регестрируем данные
    content_for_django['reviews'] = await reviews_for_id_movie(title_id)
    await search_update_count(title_id, 0)
    return content_for_django

async def genres_for_id_movie(id_movie):
    result = await fast_req_sessions(f"""
                                            select g.name 
                                            from {schema_main_info}film_genres fg
                                            left join {schema_main_info}genres g on g.id = fg.id_genres
                                            where fg.id_film = {id_movie}""")
    content = [dict(Genres.model_validate(row, from_attributes=True)) for row in result]
    return content

async def countries_for_id_movie(id_movie):
    result = await fast_req_sessions(f"""
                                            select c.name 
                                            from {schema_main_info}film_countries fc
                                            left join {schema_main_info}countries c on c.id = fc.id_country
                                            where fc.id_film = {id_movie}""")
    content = [dict(Countries.model_validate(row, from_attributes=True)) for row in result]

    return content

async def sequils_and_prequels_for_id_movie(id_movie):
    result = await fast_req_sessions(f"""
                                SELECT ms.id, ms.name, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, ms.year_create, ms.description, ms.poster, COALESCE(rf.rate, 0) as rate_film
                                FROM {schema_main_info}sequels_and_prequels sap
                                inner join {schema_main_info}my_spisok ms on sap.id_sap_film = ms.id
                                left join {schema_main_info}rate_film rf on rf.id_movie = ms.id 
                                where sap.id_movie = {id_movie} and not (ms.name IS NULL)""")
    content = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]

    return content

async def similars_movies_for_id_movie(id_movie):
    result = await fast_req_sessions(f"""
                                            SELECT ms.id, ms.name, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, ms.year_create, ms.description, ms.poster, COALESCE(rf.rate, 0) as rate_film
                                            FROM {schema_main_info}similar_movies sm 
                                            left join {schema_main_info}my_spisok ms on sm.id_similar_film = ms.id 
                                            left join {schema_main_info}rate_film rf on rf.id_movie = ms.id 
                                            where sm.id_movie = {id_movie} and not (ms.name IS NULL)""")
    content = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]
    return content

async def reviews_for_id_movie(title_id):
    result = await fast_req_sessions(f"""
                    SELECT 
                    uh.hash_id as profile_hash_user_id,
                    r.id_movie,
                    r.type_review,
                    u.nickname,
                    u.block_account,
                    (select count(ra.user_dislike) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_dislike is null)) as count_dislike,
                    (select count(ra.user_like) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_like is null)) as count_like,
                    TO_CHAR(r.date_review, 'YYYY-MM-DD HH24:MI') as date_review,
                    r.title_name,
                    SUBSTRING(title_content, 1, 300) as title_content
                    from {schema_main_info}reviews r 
                    left join pub_user.users u on u.id_user_django = r.id_user_django
                    left join pub_user.user_hash uh on uh.id_user_django = r.id_user_django
                    where r.id_movie = {title_id} 
                    order by RANDOM() limit 5""")
    content = [dict(Reviews_my_spisok.model_validate(row, from_attributes=True)) for row in result]
    return content

async def my_spisok_title_get(id_movie):
    result = await fast_req_sessions(f"""select ms.id, ms.name, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, COALESCE(rf.rate, 0) as rate_film, 
    ms.year_create, ms.description, ms.poster, ms.age_rating, ms.movie_length, ms.total_series_length, ms.type_number in (1,3,4) as is_film, tt.ru_name as type_name,
    vl.id_link, vl.oid_link 
    from {schema_main_info}my_spisok ms
    left join {schema_main_info}type_title tt on tt.id = ms.type_number 
    left join {schema_main_info}rate_film rf on rf.id_movie = ms.id 
    left join {schema_main_info}vkvideo_links vl on vl.id_movie = ms.id
    where ms.id = {id_movie}""")
    content = [dict(My_spisok_full.model_validate(row, from_attributes=True)) for row in result]

    return content[0]
    # return [result.to_dict() for result in rows]

async def movies_persons_get_list(id_movie):
    result = await fast_req_sessions(f"""
                SELECT p.id ,p.name, p.photo, prof.name as name_profession, fp.description
                FROM {schema_main_info}my_spisok ms
                LEFT JOIN {schema_main_info}film_person fp ON fp.id_movie = ms.id
                LEFT JOIN {schema_main_info}persons p ON fp.id_person = p.id
                left join {schema_main_info}proffesions prof on prof.id = fp.id_proffesion 
                left join {schema_main_info}type_title tt on tt.id = ms.type_number 
                where ms.id = {id_movie}
                group by p.id ,p.name, prof.name, fp.id_proffesion, fp.place_kinopoisk, fp.description
                order by fp.id_proffesion, fp.place_kinopoisk""")

    contents = [dict(Persons_for_title.model_validate(row, from_attributes=True)) for row in result]


    list_persons_per_prof = {}
    for content in contents:
        if content['name_profession'] not in ['актеры', 'композиторы', 'режиссеры', 'актеры дубляжа']:
            continue
        if content['name_profession'] is None:
            continue
        name_profession = content['name_profession'].capitalize()
        if name_profession not in list_persons_per_prof:
            list_persons_per_prof[name_profession] = []
        if len(list_persons_per_prof[name_profession]) > 6:
            continue
        list_persons_per_prof[name_profession].append(
            {
                'person_name': content['name'],
                'person_id': content['id'],
                'photo': content['photo'],
                'description': content['description'],
            })
    return list_persons_per_prof

# %% pub_user
async def user_data_film_on_django(id_movie, type_object, user_id):
    content_for_django = {}
    print("keingermantoster")
    # Получаем пользовательские данные
    content_for_django["ispin"] = await is_pin(user_id, id_movie, type_object)
    content_for_django["isfavorite"] = await is_favorite(user_id, id_movie, type_object)
    content_for_django["have_review"] = await have_review(id_movie, user_id)
    if content_for_django["have_review"]:
        content_for_django['user_review'] = await user_review(id_movie, user_id)
    content_for_django = content_for_django | await get_rate_user(user_id, id_movie)
    print('user', content_for_django)

    await register_user_inter_object(id_movie, 1 ,user_id)


    return content_for_django

async def user_review(title_id, user_id):
    query = f"""SELECT 
        uh.hash_id as profile_hash_user_id,
        r.id_movie,
        r.type_review,
        u.nickname,
        u.block_account,
        (select count(ra.user_dislike) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_dislike is null)) as count_dislike,
        (select count(ra.user_like) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_like is null)) as count_like,
        TO_CHAR(r.date_review, 'YYYY-MM-DD HH24:MI') as date_review,
        r.title_name,
        SUBSTRING(title_content, 1, 300) as title_content
        from data.reviews r 
        left join pub_user.users u on u.id_user_django = r.id_user_django
        left join pub_user.user_hash uh on uh.id_user_django = r.id_user_django
        where r.id_movie = {title_id} and r.id_user_django = {user_id}"""

    result = await fast_req_sessions(query)
    return [dict(Reviews_my_spisok_with_user_rate.model_validate(row, from_attributes=True)) for row in result]



async def have_review(title_id, user_id):
    if await object_existence(f"select count(*) from data.reviews where id_movie = {title_id} and id_user_django = {user_id}"):
        return True
    else:
        return False
async def is_pin(id_user, id_object, type_object):
    if await object_existence(
        f'Select count(pin) from pub_user.user_actions where id_user_django = {id_user} and object_type = {type_object} and id_object = {id_object} and not (pin is NULL)',
        ):
        return 1
    else:
        return 0

async def is_favorite(id_user, id_object, type_object):
    if await object_existence(
        f'Select count(favorite) from pub_user.user_actions where id_user_django = {id_user} and object_type = {type_object} and id_object = {id_object} and not (favorite is NULL)',
        ):
        return 1
    else:
        return 0

async def get_rate_user(user_id, title_id):
    result = await fast_req_sessions(
        f"Select rate as user_rate from pub_user.user_actions where id_user_django = {user_id} and object_type = 0 and id_object = {title_id} and not (rate = 0)",
        )
    contents = [dict(User_Rate.model_validate(row, from_attributes=True)) for row in result]

    try:
        return contents[0]
    except:
        return {}

@router.post("/rate")
async def rate(id_object: int, id_user: int, rate: int):
    type_object = 0
    result = await rate_it(id_user, id_object, type_object, rate)
    return {"message": 'rate it', 'result': result}

async def rate_it(id_user, id_object, type_object, rate):
    await line_is_create(id_user, id_object, type_object)
    await post_fast_req_sessions(
        f"""update pub_user.user_actions set rate = {rate}, date_rate = now(), pin = null where id_user_django = {id_user} and object_type = {type_object} and id_object = {id_object} """)
#
@router.post("/favoriteit")
async def title_favorite_it(object_id: int, type_object: int, user_id: int):
    result = await favorite_it(user_id, object_id, type_object)
    return {"message": 'rate it', 'result': result}

async def favorite_it(id_user, id_object, type_object):
    await line_is_create(id_user, id_object, type_object)
    await post_fast_req_sessions(
        f"""update pub_user.user_actions set favorite = now() where id_user_django = {id_user} and object_type = {type_object} and id_object = {id_object} """)

@router.post("/favoriteout")
async def title_favorite_out(object_id: int, type_object: int, user_id: int):
    result = await favorite_out(user_id, object_id, type_object)
    return {"message": 'rate it', 'result': result}

async def favorite_out(id_user, id_object, type_object):
    await post_fast_req_sessions(
        f"""update pub_user.user_actions set favorite = null where id_user_django = {id_user} and object_type = {type_object} and id_object = {id_object} """)



@router.post("/pinit")
async def title_pin_it(object_id: int, type_object: int, user_id: int):
    result = await pin_it(user_id, object_id, type_object)
    return {"message": 'rate it', 'result': result}

async def pin_it(id_user, id_object, type_object):
    await line_is_create(id_user, id_object, type_object)
    await post_fast_req_sessions(
        f"""update pub_user.user_actions set pin = now() where id_user_django = {id_user} and object_type = {type_object} and id_object = {id_object} """)

@router.post("/pinout")
async def title_pin_out(object_id: int, type_object: int, user_id: int):
    result = await pin_out(user_id, object_id, type_object)
    return {"message": 'rate it', 'result': result}

async def pin_out(id_user, id_object, type_object):
    await post_fast_req_sessions(
        f"""update pub_user.user_actions set pin = null where id_user_django = {id_user} and object_type = {type_object} and id_object = {id_object} """)


async def line_is_create(id_user, id_object, type_object):
    if not (await object_existence(
            f'Select count(id_user_django) from pub_user.user_actions where id_user_django = {id_user} and object_type = {id_object} and id_object = {type_object} ')):
        await insert_line_user_action(id_user, id_object, type_object)

async def insert_line_user_action(id_user, id_object, type_object):
    await post_fast_req_sessions(
        f"""insert into pub_user.user_actions (id_user_django, object_type, id_object) values ({id_user},{type_object},{id_object})""")

