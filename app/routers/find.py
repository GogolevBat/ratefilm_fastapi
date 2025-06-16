from fastapi import APIRouter, Depends

from ..schemas import *
from ..dependencies import *
import json, asyncio, time

router = APIRouter(
    tags=["find"],
    responses={404: {"description": "Not found"}}
                    )

@router.get("/title")
async def title_filter_find(query:str,
                            genre_id:Optional[int] = None,
                            country_id:Optional[int] = None,
                            rate_to:Optional[int] = 10,
                            rate_from:Optional[int] = 1,
                            year_to:Optional[int] = 10000,
                            year_from:Optional[int] = 1900,
                            persons:Optional[str] = None,
                            count_persons:Optional[int] = None):
    # print("\n\n\n\n\n\nУхты", query, genre_id, country_id, rate_to, rate_from, year_to, year_from,'\n\n\n\n\n\n')
    return {"message": 'title_find', "result": await filter_find(query, genre_id, country_id, rate_to, rate_from, year_to, year_from, persons, count_persons)}



async def filter_find(query, genre_id, country_id, rate_to, rate_from, year_to, year_from, persons, count_persons):
    if persons is not None:
        text_query = f"""select ms_main.id, ms_main.name, ms_main.kp_rate, COALESCE(rf.rate, 0) as rate_film, ms_main.imdb_rate, ms_main.year_create, ms_main.poster 
        from (select fp.id_movie from {schema_main_info}film_person fp
        WHERE fp.id_person IN ({persons})
        GROUP BY fp.id_movie            
        HAVING COUNT(DISTINCT fp.id_person) >= {count_persons}) ex_ac
        left join {schema_main_info}my_spisok ms_main on ms_main.id = ex_ac.id_movie
        left join {schema_main_info}number_of_search_objects noso on noso.id_object = ms_main.id and noso.object_type = 0
        where ms_main.name ILIKE '%{query}%' 
        and ms_main.id in (SELECT ms.id
        FROM {schema_main_info}my_spisok ms
        """
    else:
        text_query = f"""select ms_main.id, ms_main.name, ms_main.kp_rate, ms_main.imdb_rate, COALESCE(rf.rate, 0) as rate_film, ms_main.year_create, ms_main.poster from {schema_main_info}my_spisok ms_main
        left join {schema_main_info}number_of_search_objects noso on noso.id_object = ms_main.id and noso.object_type = 0
        left join {schema_main_info}rate_film rf on rf.id_movie = ms_main.id
        where ms_main.name ILIKE '%{query}%' 
        and ms_main.id in (SELECT ms.id
        FROM {schema_main_info}my_spisok ms """
    if genre_id is not None:
        text_query += f""" JOIN {schema_main_info}film_genres fg ON ms.id = fg.id_film
                        JOIN {schema_main_info}genres g ON fg.id_genres = g.id """
    if country_id is not None:
        text_query += f""" JOIN {schema_main_info}film_countries fc ON ms.id = fc.id_film
                        JOIN {schema_main_info}countries c ON fc.id_country = c.id """
    text_query += f""" WHERE  
    {f"g.id IN ({genre_id}) AND" if genre_id is not None else ''} 
    {f"c.id IN ({country_id}) AND" if country_id is not None else ''} 
    ((ms.kp_rate <= {rate_to} AND ms.kp_rate >= {rate_from}) 
    OR (ms.imdb_rate <= {rate_to} AND ms.imdb_rate >= {rate_from})) 
    AND (ms.year_create >= {year_from} AND ms.year_create <= {year_to})
    GROUP BY ms.id) order by COALESCE(noso.count_search, 0) DESC, ms_main.kp_rate DESC limit 50
    """
    # print(text_query)
    result = await fast_req_sessions(text_query)
    content = [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]

    return content

@router.get("/person")
async def person_find(query: str):
    return {"message": 'person_find', 'result': await find_person(query)}

async def find_person(query):
    if query:
        result = await fast_req_sessions(f"select id, name, photo from {schema_main_info}persons where name ILIKE '%{query}%' limit 50")
        content = [dict(mini_Persons.model_validate(row, from_attributes=True)) for row in result]
        return content


@router.get("/mini")
async def find_mini(query: str):
    return {"message": 'find_mini', 'result': await find_mini_req(query)}

async def find_mini_req(query):
    time_start = time.time()
    content = {}
    # time_start = time.time()
    titles_find = await find_5_titles(query)
    # print('titles', time.time() - time_start)
    if len(titles_find) > 0:
        content['most_likely'] = titles_find[0]
        content['titles'] = titles_find[1:]
    # persons_st = time.time()
    content['persons'] = await find_5_persons(query)
    # print('persons', time.time() - persons_st, time.time() - time_start)
    print('java_mini_find_edn', time.time() - time_start)
    return content

async def find_5_persons(query):
    result = await fast_req_sessions(f"""select ps.id, ps.name, ps.photo from {schema_main_info}persons ps
    left join {schema_main_info}number_of_search_objects noso on noso.id_object = ps.id and object_type = 1
    where ps.name ilike '%{query}%'
    order by Coalesce(noso.count_search, 0) * (case when POSITION(LOWER('{query}') IN LOWER(ps.name)) < 9 then 1 else 0.5 end) DESC limit 4;""")

    return [dict(mini_Persons.model_validate(row, from_attributes=True)) for row in result]

async def find_5_titles(query):
    result = await fast_req_sessions(f"""
    select ms.id, ms.name, ms.poster, ms.kp_rate 
    from {schema_main_info}my_spisok ms
    left join {schema_main_info}number_of_search_objects noso on noso.id_object = ms.id and object_type = 0
    where ms.name ilike '%{query}%'
    order by Coalesce(noso.count_search, 0) DESC, ms.kp_rate DESC limit 5""")

    return [dict(My_spisok.model_validate(row, from_attributes=True)) for row in result]
