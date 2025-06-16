from fastapi import APIRouter

from ..schemas import *
from ..dependencies import *
import json, asyncio, datetime, random

router = APIRouter(
    # prefix="/title",
    tags=["menu"],
    responses={404: {"description": "Not found"}}
                    )
@router.get('/favorited')
async def show_favorited(user_id: int):
    return {'result':await favorite_objects(user_id)}

@router.get('/showpin')
async def show_pin(user_id: int):
    return {'result':await pined_titles(user_id)}

@router.get('/viewed')
async def show_watched(user_id: int):
    return {'result':await viewed_titles(user_id)}

@router.get('/{user_id}')
async def get_menu(user_id:int|str):
    data = {'contents_for_menu': await test_menu(user_id)}
    return {"message": 'get_menu', 'result': data}


async def test_menu(user_id):
    """
    Словарь:
    Для всех
    Content.titles
    Content.heading
    Для остальных где есть доп
    Content.person
    Content.title

    Типы:
    top
    list_titles
    info_fact
    person_titles
    movie_titles
    """
    if user_id == "None":
        user_id = None
    key_redis = str(user_id) + ':u:m'
    # if redis_connect.exists(key_redis) and not (user_id is None):
    #     # print("redis")
    #     list_menu_lines = redis_connect.get(key_redis)
    #
    # else:

    list_menu_lines = []
    list_menu_lines.append({'type': 'top', 'content': await now_top_titles_of_frequently_visited()})
    list_menu_lines.append({'type': 'top', 'content': await now_top_persons_of_frequently_visited()})
    list_menu_lines.append({'type': 'list_titles', 'content': await menu_random_cool_titles()})
    list_menu_lines.append({'type': 'list_titles', 'content': await menu_random_cool_genres_titles()})
    # list_menu_lines.append({'type': 'info_fact', 'content': await movie_fact(user_id)})
    if not (user_id is None):
        list_menu_lines = list_menu_lines + await add_menu(user_id)
        random.shuffle(list_menu_lines)
        privet = datetime.datetime.now().date().strftime('YYMMDD')
            # print("sql")
            # redis_connect.set(key_redis, json.dumps(list_menu_lines))
    return list_menu_lines

async def menu_random_cool_titles():
    result = await fast_req_sessions(f"""
    select id, name, poster, COALESCE(rf.rate, 0) as rate_film, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, imdb_rate 
    from {schema_main_info}my_spisok ms
    left join {schema_main_info}rate_film rf on rf.id_movie = ms.id 
    where not(backdrop is NULL) and kp_rate > 7 and type_number = 1 and not(movie_length is Null)
    ORDER BY RANDOM() 
    limit 10""")
    content = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]
    return {"heading": "Хорошие оценки" ,"titles": content}

async def menu_random_cool_genres_titles():
    result_genre = await fast_req_sessions(f"""select id ,name from data.genres where not (id in (8, 26, 32, 21)) order by random() limit 1""")
    content_genre = [dict(Genres.model_validate(row, from_attributes=True)) for row in result_genre]

    result = await fast_req_sessions(f"""
        select ms.name, ROUND(CAST(ms.kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, COALESCE(rf.rate, 0) as rate_film, ms.id, ms.poster
        from (select fg.id_film from data.film_genres fg
        where fg.id_genres = {content_genre[0]["id"]}
        order by random()
        limit 100) random_titles_on_genre
        left join {schema_main_info}my_spisok ms on ms.id = random_titles_on_genre.id_film
        left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
        where ms.kp_rate >= 6
        limit 10
        """)
    content = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]
    return {"heading": f'Произведения с жанром "{content_genre[0]["name"]}"'
        , "titles":content}

async def now_top_titles_of_frequently_visited():
    result = await fast_req_sessions(f"""
    select id, name, poster, COALESCE(rf.rate, 0) as rate_film, ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, imdb_rate, 1 as type_object
    from
    (select * from {schema_main_info}number_of_search_objects noso
    where noso.object_type = 0
    order by noso.count_search DESC limit 10) noso_films
    left join {schema_main_info}my_spisok ms on noso_films.id_object = ms.id
    left join {schema_main_info}rate_film rf on rf.id_movie = ms.id""")
    content = [dict(My_spisokTop.model_validate(row, from_attributes=True)) for row in result]
    return {'heading': 'Топ тайтлов', 'titles': content}

async def now_top_persons_of_frequently_visited():
    result = await fast_req_sessions(f"""
    select id, name, photo as poster, 2 as type_object
    from
    (select * from {schema_main_info}number_of_search_objects noso
    where noso.object_type = 1
    order by noso.count_search DESC limit 10) noso_films
    left join {schema_main_info}persons p on noso_films.id_object = p.id""")
    content = [dict(MiniPersonsTop.model_validate(row, from_attributes=True)) for row in result]

    return {'heading': 'Топ персон', 'titles':content}
#
# async def movie_fact(user_id):
#     query = f"""select ms.id, ms.kp_rate, ms.name, ms.poster, user_rand_films_with_facts.value as fact
#         from
#             (select user_lasted_films.id_object, facts_movie.value
#             from
#                     (select ua.id_object
#                     from pub_user.user_actions ua
#                     where ua.id_user_django = {user_id} and ua.rate >= 6
#                     order by ua.date_rate Desc limit 50) user_lasted_films
#             left join {schema_main_info}facts_movie on facts_movie.id_movie = user_lasted_films.id_object
#             where not(facts_movie.value is null) and facts_movie.value <> ''
#             order by random()
#             limit 4) user_rand_films_with_facts
#         left join {schema_main_info}my_spisok ms on user_rand_films_with_facts.id_object = ms.id"""
#     # print('\n\n\n\n\n',query,'\n\n\n\n\n')
#     result_dict_movie_fact = await fast_req_sessions(query)
#     dict_movie_fact = [dict(MySpisokFacts.model_validate(row, from_attributes=True)) for row in result_dict_movie_fact]
#
#     for index in range(len(dict_movie_fact)):
#         dict_movie_fact[index]['text'] = await choice_fact_value(dict_movie_fact[index]['fact'])
#     # print('\n\n\n\n')
#     # print(dict_movie_fact)
#     return {"titles": dict_movie_fact}
#
# async def choice_fact_value(text):
#     print(text, type(text))
#     # while text.find('<a') != -1 and text.find('a>') != -1:
#     #     close_breaks = text.find("</a>")
#     #     text = text[:text.find('<a')] + text[text.find('>') + 1: close_breaks] + text[close_breaks + 4:]
#     facts = json.loads(text)
#     # print(facts)
#     facts_list = []
#     for fact in facts:
#         if len(fact['value']) <= 180 and fact['spoiler'] == False and fact['value'].find('delete') == -1:
#             facts_list.append(fact['value'])
#     if facts_list:
#         return random.choice(facts_list)
#     return []

async def add_menu(user_id):
    list_menu_lines = []
    list_random_content = [1,2,3,4,5,6,7,8,9,10]
    random.shuffle(list_random_content)
    for random_content in list_random_content[:6]:
        try:
            match random_content:
                case 1:
                    list_menu_lines.append({'type': 'list_titles', 'content': await menu_random_cool_titles()})
                case 2:
                    list_menu_lines.append({'type': 'list_titles', 'content': await menu_random_cool_genres_titles()})
                case 3:
                    list_menu_lines.append({'type': 'list_titles', 'content': await pined_titles_on_menu(user_id)})
                case 4:
                    list_menu_lines.append({'type': 'info_fact', 'content': await favorite_person_for_menu(user_id, True)})
                case 5:
                    list_menu_lines.append({'type': 'movie_titles', 'content': await movie_favorite_titles_for_menu_on_genres(user_id)})
                case 6:
                    list_menu_lines.append({'type': 'movie_titles', 'content': await movie_titles_for_menu_on_genres(user_id)})
                case 7:
                    list_menu_lines.append({'type': 'person_titles', 'content': await person_titles_for_menu(user_id)})
                case 8:
                    list_menu_lines.append({'type': 'list_titles', 'content': await titles_on_user_top_genre(user_id)})
                case 9:
                    list_menu_lines.append({'type': 'info_fact', 'content': await titles_on_user_top_genre_with_short_description(user_id)})
                case 10:
                    list_menu_lines.append({'type': 'list_titles', 'content': await titles_on_user_top_country(user_id)})

        except:
            pass
    random.shuffle(list_menu_lines)
    return list_menu_lines

async def person_titles_for_menu(user_id):
    result_user_person = await fast_req_sessions(f"""
    select user_lasted_films.id_object, p.id,  p.name, p.photo, ms.name as title_name, ms.id as title_id from
            (select ua.id_object 
            from pub_user.user_actions ua 
            where ua.id_user_django = {user_id} and ua.rate >= 7
            order by ua.date_rate Desc limit 5) user_lasted_films
    left join {schema_main_info}film_person fp_o on user_lasted_films.id_object = fp_o.id_movie and place_kinopoisk <= 5 and fp_o.id_proffesion = 1
    left join {schema_main_info}persons p on p.id = fp_o.id_person
    left join {schema_main_info}my_spisok ms on user_lasted_films.id_object = ms.id
    group by fp_o.id_person, p.id,  p.name, p.photo, ms.name, user_lasted_films.id_object
    order by random()
    limit 1""")
    if len(result_user_person) == 0:
        return []
    content = [dict(Person_titles_for_menu.model_validate(row, from_attributes=True)) for row in result_user_person]
    id_person = content[0]['id']
    id_object = content[0]['id_object']
    if id_person is None or id_object is None:
        return []
    result =  await fast_req_sessions(f"""
                                        select ms.id, ms.name, ROUND(CAST(ms.kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, COALESCE(rf.rate, 0) as rate_film, ms.poster from 
                                                (select {id_person} as id_person , {id_object} as id_object) user_person
                                        left join {schema_main_info}film_person fp on user_person.id_person = fp.id_person and place_kinopoisk <= 5 and fp.id_proffesion = 1 and fp.id_movie <> user_person.id_object
                                        left join {schema_main_info}my_spisok ms on ms.id = fp.id_movie 
                                        left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
                                        order by random() limit 9""")
    content_titles = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]

    p_person_titles = {"heading": f'Фильмы, где есть "<a class="menu_link" href="/person/{content[0]['id']}">{content[0]['name']}</a>" из "<a class="menu_link" href="/person/{content[0]['title_id']}">{content[0]['title_name']}</a>"',
                       'person': content[0],
                       'titles':content_titles}
    if len(p_person_titles['titles']) == 0 or p_person_titles['titles'][0]['name'] is None:
        return []
    # print('\n\n\n\n\n\n',p_person_titles['titles'])
    return p_person_titles

async def favorite_person_for_menu(user_id, is_info_fact=False):
    query = f"""select p.id,  p.name, p.photo 
                from pub_user.user_actions ua
                left join {schema_main_info}persons p on p.id = ua.id_object
                where ua.id_user_django = {user_id} and ua.object_type = 1 and not(ua.favorite is null)
                order by random()
                limit 1"""
    result_favorite_person = await fast_req_sessions(query)
    favorite_person = [dict(mini_Persons.model_validate(row, from_attributes=True)) for row in result_favorite_person]

    if len(favorite_person) == 0:
        return []

    id_favorite_person = favorite_person[0]["id"]
    query = f"""select ms.id, ms.name, ROUND(CAST(ms.kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, COALESCE(rf.rate, 0) as rate_film, ms.year_create, ms.shortdescription as text, ms.poster from
                        (select fp.id_movie from
                            (select {id_favorite_person} as id_object) user_favorite_person
                        left join {schema_main_info}film_person fp on fp.id_person = user_favorite_person.id_object
                        left join pub_user.user_actions ua_m on fp.id_movie = ua_m.id_object and ua_m.object_type = 0 and ua_m.id_user_django = {user_id}
                        where ua_m.rate = 0 or ua_m.rate is null
                        group by fp.id_movie
                        order by random()) movies_with_favorite_person
                left join {schema_main_info}my_spisok ms on ms.id = movies_with_favorite_person.id_movie
                left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
                where not(name is null) {"and not (ms.shortdescription is null)" if is_info_fact else ""}
                limit {4 if is_info_fact else 10}"""
    result_titles = await fast_req_sessions(query)
    titles = [dict(My_spisok_menu_info_fact.model_validate(row, from_attributes=True)) for row in result_titles]

    if len(titles) == 0 or titles[0]['name'] is None:
        return []
    return {"heading": f'Фильмы, с "<a class="menu_link" href="/person/{id_favorite_person}">{favorite_person[0]['name']}</a>" (Любимое)',
            'person': favorite_person[0],
            'titles': titles}

async def pined_titles_on_menu(user_id):
    query = f"""
    select ms.name, ROUND(CAST(ms.kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, COALESCE(rf.rate, 0) as rate_film, ms.id, ms.poster
    from pub_user.user_actions ua
    left join {schema_main_info}my_spisok ms on ms.id = ua.id_object
    left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
    where ua.id_user_django = {user_id} and not (ua.pin is null) and ua.rate = 0
    order by random()
    limit 10"""
    result_titles = await fast_req_sessions(query)
    titles = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result_titles]

    return {
        "heading": f'Пришло время посмотреть',
        "titles": titles
    }

async def movie_titles_for_menu_on_genres(user_id):
    result_genres_random_user_title = await fast_req_sessions(f"""select film_genres.id_genres, gen.name, ms.name as movie_name, ms.id as id, ms.poster as poster 
                                    from
                                            (select user_lasted_films.id_object from
                                                    (select ua.id_object
                                                    from pub_user.user_actions ua
                                                    where ua.id_user_django = {user_id} and ua.rate >= 7
                                                    order by ua.date_rate Desc limit 5) user_lasted_films
                                            order by random()
                                            limit 1) user_film
                                    left join {schema_main_info}film_genres ON user_film.id_object = film_genres.id_film
                                    left join {schema_main_info}my_spisok ms on ms.id = user_film.id_object
                                    left join {schema_main_info}genres gen on gen.id = film_genres.id_genres""")
    genres_random_user_title = [dict(MovieTitlesForMenuOnGenres.model_validate(row, from_attributes=True)) for row in result_genres_random_user_title]


    genres = [f"{genres['id_genres']}" for genres in genres_random_user_title]
    result_titles = await fast_req_sessions(f"""
                                select ms.name, ROUND(CAST(ms.kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate,COALESCE(rf.rate, 0) as rate_film,  ms.id, ms.poster 
                                from
                                        (select fg.id_film, count(fg.id_genres)
                                        from {schema_main_info}film_genres fg
                                        where fg.id_genres in ({" ,".join(genres)})
                                        group by fg.id_film
                                        having count(fg.id_genres) = {len(genres)}
                                        order by Random()) choose_movies
                                left join {schema_main_info}my_spisok ms on ms.id = choose_movies.id_film
                                left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
                                where ms.kp_rate >= 6
                                limit 9""")

    titles = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result_titles]

    title_find = {'id': genres_random_user_title[0]['id'],
                  'name': genres_random_user_title[0]['movie_name'],
                  'poster': genres_random_user_title[0]['poster']}
    # print(" ,".join(genres))
    return {
        "heading": f'Похожие на "<a class="menu_link" href="/person/{title_find['id']}">{title_find['name']}</a>" по жанрам',
        "title": title_find,
        "titles": titles
    }

async def movie_favorite_titles_for_menu_on_genres(user_id):
    result_genres_random_user_title = await fast_req_sessions(f"""
    select film_genres.id_genres, gen.name, ms.name as movie_name, ms.id as id, ms.poster as poster from
            (select user_lasted_films.id_object from
                    (select ua.id_object
                    from pub_user.user_actions ua
                    where ua.object_type = 0 and ua.id_user_django = {user_id} and not (ua.favorite is null)) user_lasted_films
            order by random()
            limit 1) user_film
    left join {schema_main_info}film_genres ON user_film.id_object = film_genres.id_film
    left join {schema_main_info}my_spisok ms on ms.id = user_film.id_object
    left join {schema_main_info}genres gen on gen.id = film_genres.id_genres""")
    genres_random_user_title = [dict(MovieTitlesForMenuOnGenres.model_validate(row, from_attributes=True)) for row in result_genres_random_user_title]

    genres = [f"{genres['id_genres']}" for genres in genres_random_user_title]
    result_titles = await fast_req_sessions(f"""select ms.name, ROUND(CAST(ms.kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate,COALESCE(rf.rate, 0) as rate_film,  ms.id, ms.poster from
                                                                                (select fg.id_film, count(fg.id_genres)
                                                                                from {schema_main_info}film_genres fg
                                                                                where fg.id_genres in ({" ,".join(genres)})
                                                                                group by fg.id_film
                                                                                having count(fg.id_genres) = {len(genres)}
                                                                                order by Random()) choose_movies
                                                                        left join {schema_main_info}my_spisok ms on ms.id = choose_movies.id_film
                                                                        left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
                                                                        where ms.kp_rate >= 6
                                                                        limit 9""")
    titles = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result_titles]

    title_find = {'id': genres_random_user_title[0]['id'],
                  'name': genres_random_user_title[0]['movie_name'],
                  'poster': genres_random_user_title[0]['poster']}
    # print(" ,".join(genres))
    return {
        "heading": f'Похожие на избраное "<a class="menu_link" href="/person/{title_find['id']}">{title_find['name']}</a>" по жанрам',
        "title": title_find,
        "titles": titles
    }

async def titles_on_user_top_genre(user_id):
    result_user_genre = await fast_req_sessions(f"""select gen.name, gen.id
        from (select top_user_genres.id_genres from

                (select fg1.id_genres from

                        (select id_object
                        from pub_user.user_actions ua
                        where ua.id_user_django = {user_id} and not (ua.date_rate is null) and ua.rate >= 7
                        order by ua.date_rate DESC, ua.rate DESC) user_rates

                left join data.film_genres fg1 on fg1.id_film = user_rates.id_object
                group by fg1.id_genres
                order by count(fg1.id_film) DESC limit 5) top_user_genres

            order by random() limit 1) choose_user_genre
        left join data.genres gen on gen.id = choose_user_genre.id_genres""")
    user_genre = [dict(Genres.model_validate(row, from_attributes=True)) for row in result_user_genre]

    genre_id, genre_name = user_genre[0]['id'], user_genre[0]['name']
    result_titles = await fast_req_sessions(f"""select ms.name, ROUND(CAST(ms.kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, COALESCE(rf.rate, 0) as rate_film, ms.id, ms.poster from (select fg.id_film from
                
                            (select {genre_id} as id_genres) choose_user_genre
                
                    left join data.film_genres fg on choose_user_genre.id_genres = fg.id_genres
                    order by random()
                    limit 500)  films_user_recomendation
                    left join data.my_spisok ms on films_user_recomendation.id_film = ms.id
                    left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
                    where ms.kp_rate > 6 and not(ms.poster is null) limit 9""")
    titles = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result_titles]

    return {
        'heading': f'Фильмы по одному из ваших популярных жанров, а именно {genre_name}',
        'genre': genre_name,
        'titles': titles
    }

async def titles_on_user_top_genre_with_short_description(user_id):
    result_user_genre = await fast_req_sessions(f"""select gen.name, gen.id
        from (select top_user_genres.id_genres from

                (select fg1.id_genres from

                        (select id_object
                        from pub_user.user_actions ua
                        where ua.id_user_django = {user_id} and not (ua.date_rate is null) and ua.rate >= 7
                        order by ua.date_rate DESC, ua.rate DESC) user_rates

                left join data.film_genres fg1 on fg1.id_film = user_rates.id_object
                group by fg1.id_genres
                order by count(fg1.id_film) DESC limit 5) top_user_genres

            order by random() limit 1) choose_user_genre
        left join data.genres gen on gen.id = choose_user_genre.id_genres""")
    user_genre = [dict(Genres.model_validate(row, from_attributes=True)) for row in result_user_genre]

    genre_id, genre_name = user_genre[0]['id'], user_genre[0]['name']
    result_titles = await fast_req_sessions(f"""select ms.name, ROUND(CAST(ms.kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate,  ms.id, ms.poster, ms.shortdescription as text
    from
    (select fg.id_film from

            (select {genre_id} as id_genres) choose_user_genre

    left join data.film_genres fg on choose_user_genre.id_genres = fg.id_genres
    order by random()
    limit 500)  films_user_recomendation
    left join data.my_spisok ms on films_user_recomendation.id_film = ms.id
    where ms.kp_rate > 6 and not(ms.poster is null) and not(ms.shortdescription is Null)  limit 4""")
    titles = [dict(My_spisok_menu_info_fact.model_validate(row, from_attributes=True)) for row in result_titles]

    return {
        'heading': f'Описание фильмов с жанром "{genre_name}"',
        'genre': genre_name,
        'titles': titles
    }

async def titles_on_user_top_country(user_id):
    result_user_country = await fast_req_sessions(f"""select c.id, c.name  from
                                (select top_user_countries.id_country from
                                
                                        (select fc1.id_country from
                                
                                                (select id_object
                                                from pub_user.user_actions ua
                                                where ua.id_user_django = {user_id} and not (ua.date_rate is null) and ua.rate >= 7
                                                order by ua.date_rate DESC, ua.rate DESC) user_rates
                                
                                        left join data.film_countries fc1 on fc1.id_film = user_rates.id_object
                                        group by fc1.id_country
                                        order by count(fc1.id_film) DESC limit 5) top_user_countries
                                
                                order by random() limit 1) choose_user_country
                                left join data.countries c on c.id = choose_user_country.id_country""")

    user_country = [dict(Countries.model_validate(row, from_attributes=True)) for row in result_user_country]

    country_id, country_name = user_country[0]['id'], user_country[0]['name']
    result_titles = await fast_req_sessions(f"""
                            select ms.name, ROUND(CAST(ms.kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, COALESCE(rf.rate, 0) as rate_film, ms.id, ms.poster 
                            from
                                    (select fc.id_film from
                            
                                            (select {country_id} as id_country) choose_user_country
                            
                                    left join data.film_countries fc on choose_user_country.id_country = fc.id_country
                                    order by random()
                                    limit 1000)  films_user_recomendation
                            left join {schema_main_info}my_spisok ms on films_user_recomendation.id_film = ms.id
                            left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
                            where ms.kp_rate > 6 and not(ms.poster is null) limit 10""")
    titles = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result_titles]

    return {
        'heading': f'Фильмы из одной из популярных у вас страны "{country_name}"',
        'country': country_name,
        'titles': titles
    }




# @router.get('/{user_id}/update')
# async def get_menu_update(user_id):
#     data = {'contents_for_menu': await update_menu(user_id)}
#     return {"message": 'get_menu', 'result': data}

@router.get('/add/content/{user_id}')
async def menu_add_content(user_id):
    data = {'contents_for_menu': await add_menu(user_id)}
    return {"message": 'menu_add_content', 'result': data}



async def pined_titles(user_id):
    result = await fast_req_sessions(f""" select ms.id, ms.name, COALESCE(rf.rate, 0) as rate_film,ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, ms.poster, ms.year_create 
    from pub_user.user_actions ua
    join  {schema_main_info}my_spisok ms on ua.id_object = ms.id
    left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
    where ua.id_user_django = {user_id} and not (ua.pin is Null)
    order by ua.pin Desc""")
    content = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]
    return content



async def favorite_objects(user_id):
    result = await fast_req_sessions(f"""
    select ua.object_type, Concat(ms.id,p.id) as id, concat(ms.name, p.name) as name, concat(ms.poster, p.photo) as poster, 
    COALESCE(rf.rate, 0) as rate_film, COALESCE(ua.rate, 0) as user_rate,ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, ms.year_create 
    from pub_user.user_actions ua
    left join {schema_main_info}my_spisok ms on ua.id_object = ms.id and ua.object_type = 0
    left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
    left join {schema_main_info}persons p on p.id = ua.id_object and ua.object_type = 1
    where ua.id_user_django = {user_id} and not (ua.favorite is Null) 
    order by ua.favorite Desc""")
    content = [dict(My_spiok_favorite.model_validate(row, from_attributes=True)) for row in result]
    return content

async def viewed_titles(user_id):
    result = await fast_req_sessions(f""" select ms.id, ms.name, COALESCE(rf.rate, 0) as rate_film, COALESCE(ua.rate, 0) as user_rate,ROUND(CAST(kp_rate AS NUMERIC), 1) as kp_rate, ms.imdb_rate, ms.poster, ms.year_create from pub_user.user_actions ua
    join  {schema_main_info}my_spisok ms on ua.id_object = ms.id
    left join {schema_main_info}rate_film rf on rf.id_movie = ms.id
    where ua.id_user_django = {user_id} and ua.rate <> 0
    order by ua.date_rate Desc""")
    content = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]
    return content

