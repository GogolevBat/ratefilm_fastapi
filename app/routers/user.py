from curses.ascii import isdigit

from fastapi import APIRouter, Request

from ..schemas import *
from ..dependencies import *
import json, asyncio, datetime, random

router = APIRouter(
    # prefix="/title",
    tags=["user"],
    responses={404: {"description": "Not found"}}
                    )


@router.get('/profile/{profile_hash_user_id}')
async def profile(profile_hash_user_id: str, user_id: str):
    return {'result': await show_user_profile(profile_hash_user_id, user_id)}


async def show_user_profile(profile_hash_user_id, user_id):
    try:
        user_id = int(user_id)
    except:
        pass
    profile_user = await profile_user_from_hash_get(profile_hash_user_id)

    profile_user_id = profile_user[0]['id_user_django']

    isshow = True

    if profile_user[0]['block_account']:
        isshow = False
    if isinstance(user_id, int):
        await register_user_inter_object(profile_user_id, 3, user_id)

    profile_data = {
        'isshow': isshow,
        'profile_hash_user_id':profile_hash_user_id,
        'nickname': profile_user[0]['nickname'],
        'own_page': profile_user_id == user_id,
        'user_avg_rate': await user_avg_rate_for_show(profile_user_id),
        'reviews': await user_reviews_for_show(profile_user_id),
        }
    return profile_data

async def user_reviews_for_show(profile_user_id):
    query = f"""select r.title_name, r.type_review ,r.title_content, TO_CHAR(r.date_review, 'YYYY-MM-DD HH24:MI') as date_review, ms.poster, ms.name, ms.id as id_title, ms.imdb_rate, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, rf.rate as rate_film, COALESCE(ua.rate, 0) as user_rate,
                (select count(ra.user_dislike) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_dislike is null)) as count_dislike,
                (select count(ra.user_like) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_like is null)) as count_like
                from {schema_main_info}reviews r  
                left join {schema_main_info}my_spisok ms on ms.id = r.id_movie
                left join {schema_main_info}rate_film rf on rf.id_movie = ms.id 
                left join pub_user.user_actions ua on ua.object_type = 0 and ua.id_object = ms.id and r.id_user_django = ua.id_user_django 
                where r.id_user_django = {profile_user_id}
                order by random()
                Limit 5"""
    result = await fast_req_sessions(query)
    return [dict(User_Reviews_my_spisok.model_validate(row, from_attributes=True)) for row in result]



async def user_avg_rate_for_show(profile_user_id):
    query = f"""select ua.id_user_django, ROUND(AVG(ua.rate), 1) as user_avg_rate 
            from pub_user.user_actions ua
            left join auth_user au on au.id = ua.id_user_django
            where ua.id_user_django = {profile_user_id} and ua.rate <> 0
            group by ua.id_user_django"""
    result_user_avg_rate = await fast_req_sessions(query)
    user_avg_rate = [dict(UserAvgRate.model_validate(row, from_attributes=True)) for row in result_user_avg_rate]
    if len(user_avg_rate) > 0:
        return user_avg_rate[0]['user_avg_rate']
    return '~'

@router.get('/favorites/{profile_hash_user_id}')
async def user_favorites(profile_hash_user_id: str):
    return {'result': await show_user_favorites(profile_hash_user_id)}

async def show_user_favorites(profile_hash_user_id):
    profile_user = await profile_user_from_hash_get(profile_hash_user_id)

    profile_user_id = profile_user[0]['id_user_django']

    if profile_user[0]['block_account']:
        return {'isshow':False}

    profile_data = {
        'isshow': True,
        'nickname': profile_user[0]['nickname'],
        'favorites': await user_titles_favorites(profile_user_id),
        }
    return profile_data

async def user_titles_favorites(profile_user_id):
    query = f"""select ms.poster, ms.name, ms.id as id_title, ms.imdb_rate, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, rf.rate as rate_film
                from pub_user.user_actions ua
                left join {schema_main_info}my_spisok ms on ms.id = ua.id_object
                left join {schema_main_info}rate_film rf on rf.id_movie = ms.id 
                where ua.id_user_django = {profile_user_id} and not (favorite is null) and ua.object_type = 0"""
    result =  await fast_req_sessions(query)
    return [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]


@router.get('/reviews/{profile_hash_user_id}')
async def user_reviews(profile_hash_user_id: str, page: int):
    return {'result': await show_user_reviews(profile_hash_user_id, page)}



async def show_user_reviews(profile_hash_user_id, page):
    profile_user = await profile_user_from_hash_get(profile_hash_user_id)

    profile_user_id = profile_user[0]['id_user_django']

    if profile_user[0]['block_account']:
        return {'isshow':False}

    profile_data = {
        'isshow': True,
        'nickname': profile_user[0]['nickname'],
        'reviews': await user_page_reviews(profile_user_id, page),
        }
    return profile_data

async def user_page_reviews(profile_user_id, page):
    count_data = (page - 1) * 20
    query = f"""select r.title_name, r.type_review ,r.title_content, TO_CHAR(r.date_review, 'YYYY-MM-DD HH24:MI') as date_review, ms.poster, ms.name, ms.id as id_title, ms.imdb_rate, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, rf.rate as rate_film, COALESCE(ua.rate, 0) as user_rate,
                u.block_account,
                (select count(ra.user_dislike) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_dislike is null)) as count_dislike,
                (select count(ra.user_like) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_like is null)) as count_like
                from {schema_main_info}reviews r  
                left join {schema_main_info}my_spisok ms on ms.id = r.id_movie
                left join {schema_main_info}rate_film rf on rf.id_movie = ms.id 
                left join pub_user.user_actions ua on ua.object_type = 0 and ua.id_object = ms.id and r.id_user_django = ua.id_user_django 
                left join pub_user.users u on u.id_user_django = r.id_user_django
                where r.id_user_django = {profile_user_id}
                order by r.date_review DESC
                {f'offset {count_data}' if count_data > 0 else ''} Limit {20 + count_data}"""
    result = await fast_req_sessions(query)
    content = [dict(User_Reviews_my_spisok.model_validate(row, from_attributes=True)) for row in result]

    return content

@router.get('/{profile_hash_user_id}/review/{title_id}')
async def user_review(profile_hash_user_id: str, title_id: int, user_id: int|str):
    return {'result': await show_user_review(profile_hash_user_id, title_id, user_id)}

async def show_user_review(profile_hash_user_id, title_id, user_id):
    profile_user = await profile_user_from_hash_get(profile_hash_user_id)

    profile_user_id = profile_user[0]['id_user_django']

    if user_id == "None":
        user_id = None
    # else:
    #     await register_user_inter_object(profile_user_id, 4, user_id)

    query = f"""select ms.poster, ms.name, ms.id as id_title, ms.imdb_rate, ROUND(CAST(ms.kp_rate AS NUMERIC), 1) as kp_rate, Coalesce(rf.rate,0) as rate_film,
                r.title_name, r.title_content, r.type_review, TO_CHAR(r.date_review, 'YYYY-MM-DD HH24:MI:SS') as date_review,
                u.block_account,
                (select count(ra.user_dislike) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_dislike is null)) as count_dislike,
                (select count(ra.user_like) from pub_user.review_action ra where ra.id_movie = r.id_movie and ra.id_user_django_creator_review = r.id_user_django and not(ra.user_like is null)) as count_like
                {f",TO_CHAR(ra.user_dislike, '1') as user_dislike, TO_CHAR(ra.user_like, '1') as user_like" if not user_id is None else ""}
                from {schema_main_info}reviews r 
                left join {schema_main_info}my_spisok ms on ms.id = r.id_movie
                left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
                left join pub_user.users u on u.id_user_django = r.id_user_django
                {f"left join pub_user.review_action ra on ra.id_movie = ms.id and ra.id_user_django_creator_review = {profile_user_id} and ra.id_user_django_consumer = {user_id}" if not user_id is None else ""}
                where r.id_user_django = {profile_user_id} and r.id_movie = {title_id}"""
    result = await fast_req_sessions(query)
    user_title_review = [dict(User_Review.model_validate(row, from_attributes=True)) for row in result]

    if len(user_title_review) == 0:
        return {}
    # print(user_title_review[0],'\n\n\n\n')
    isshow = True
    if profile_user[0]['block_account']:
        isshow = False
    return {
        "isshow": isshow,
        'profile_hash_user_id':profile_hash_user_id,
        'profile':profile_user[0],
        'own_page': user_id == profile_user_id,
        "content": user_title_review[0]}

@router.post('/{profile_hash_user_id}/review/{title_id}/reaction')
async def user_reaction(profile_hash_user_id: str, title_id: int, user_id: int, type: str):
    if type == 'like':
        await like_review(profile_hash_user_id,title_id, user_id)
    elif type == 'dislike':
        await dislike_review(profile_hash_user_id, title_id, user_id)
    else:
        await none_like_review(profile_hash_user_id,title_id, user_id)
    return {"result":"ok"}

async def dislike_review(profile_hash_user_id, title_id, user_id):
    print("dislike_review")

    await post_fast_req_sessions(f"""Do $$
        Declare user_id int;
        begin
            user_id := (select uh.id_user_django from pub_user.user_hash uh where uh.hash_id = '{profile_hash_user_id}');
            if exists (select 1 from pub_user.review_action ra where ra.id_user_django_creator_review = user_id and ra.id_movie = {title_id} and ra.id_user_django_consumer = {user_id}) then
                update pub_user.review_action set user_dislike = now(), user_like = null 
                where id_user_django_creator_review = user_id and id_movie = {title_id} and id_user_django_consumer = {user_id};
            else
                insert into pub_user.review_action (id_user_django_creator_review,id_movie,id_user_django_consumer,user_like, user_dislike) values (user_id ,{title_id}, {user_id}, null, now());
            end if;
        end $$;""")
async def like_review(profile_hash_user_id, title_id, user_id):
    print("like_review")
    await post_fast_req_sessions(f"""Do $$
            Declare user_id int;
            begin
                user_id := (select uh.id_user_django from pub_user.user_hash uh where uh.hash_id = '{profile_hash_user_id}');
                if exists (select 1 from pub_user.review_action ra where ra.id_user_django_creator_review = user_id and ra.id_movie = {title_id} and ra.id_user_django_consumer = {user_id}) then
                    update pub_user.review_action set user_dislike = null, user_like = now() 
                    where id_user_django_creator_review = user_id and id_movie = {title_id} and id_user_django_consumer = {user_id};
                else
                    insert into pub_user.review_action (id_user_django_creator_review,id_movie,id_user_django_consumer,user_like, user_dislike) values (user_id ,{title_id}, {user_id}, now(), null);
                end if;
            end $$;""")
async def none_like_review(profile_hash_user_id, title_id, user_id):
    print("none_like_review")

    await post_fast_req_sessions(f"""Do $$
                Declare user_id int;
                begin
                    user_id := (select uh.id_user_django from pub_user.user_hash uh where uh.hash_id = '{profile_hash_user_id}');
                    if exists (select 1 from pub_user.review_action ra where ra.id_user_django_creator_review = user_id and ra.id_movie = {title_id} and ra.id_user_django_consumer = {user_id}) then
                        update pub_user.review_action set user_dislike = null, user_like = null 
                        where id_user_django_creator_review = user_id and id_movie = {title_id} and id_user_django_consumer = {user_id};
                    else
                        insert into pub_user.review_action (id_user_django_creator_review,id_movie,id_user_django_consumer,user_like, user_dislike) values (user_id ,{title_id}, {user_id}, null, null);
                    end if;
                end $$;""")


@router.get('/userhash')
async def test_func(user_id: int):
    ret = await userhash_on_id(user_id)
    return {'result': ret}

async def userhash_on_id(user_id):
    result = await fast_req_sessions(f"""select uh.hash_id as user_id from
    pub_user.user_hash uh
    where uh.id_user_django = {user_id}""")
    return result[0]['user_id']

@router.post('/change/nickname')
async def change_nickname(nickname:str, user_id:int):
    await change_user_nickname(nickname, user_id)
async def change_user_nickname(new_nickname, user_id):
    query = f"""update pub_user.users
                set nickname = '{new_nickname}'
                where id_user_django = {user_id}"""
    return await post_fast_req_sessions(query)


@router.post('/change/block')
async def change_block(user_id:int):
    await change_user_block(user_id)

async def change_user_block(user_id):
    query = f"""update pub_user.users
                set block_account = (select not(u_s.block_account) from pub_user.users u_s where id_user_django = {user_id})
                where id_user_django = {user_id};"""
    return await post_fast_req_sessions(query)

@router.patch('/{profile_hash_user_id}/review/{title_id}/change/')
async def change_review(request:Request, profile_hash_user_id:str, title_id:int):
    body = await request.body()
    body = json.loads(body)
    if await check_user_review_change(profile_hash_user_id, body['user_id']):
        await review_change(body,title_id)
    else:
        print('\n\n\n\n\n\nПользователь не тот\n\n\n\n\n\n\n')

async def check_user_review_change(profile_hash_user_id, user_id):
    profile_creator = await profile_user_from_hash_get(profile_hash_user_id)
    return profile_creator[0]['id_user_django'] == user_id

async def review_change(body, title_id):
    data = body

    await post_fast_req_sessions(
    f"""update {schema_main_info}reviews 
        set type_review = {data['type']},
        date_review = now(),
        title_name = '{data['title']}',
        title_content = '{data['text']}'
        where id_user_django = {data['user_id']} and id_movie = {title_id}"""
    )





async def profile_user_from_hash_get(profile_hash_user_id):
    query = f"""select uh.id_user_django, u.nickname, u.block_account
            from pub_user.user_hash uh
            left join pub_user.users u on u.id_user_django = uh.id_user_django
            where uh.hash_id = '{profile_hash_user_id}'"""
    result_profile_user = await fast_req_sessions(query)
    return [dict(Users.model_validate(row, from_attributes=True)) for row in result_profile_user]
