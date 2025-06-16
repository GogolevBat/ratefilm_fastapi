from pydantic import BaseModel
from typing import Optional

class My_spisok(BaseModel):
    id: Optional[int] = None
    id_time: Optional[int] = None
    name: str|None
    kp_rate: float|None
    imdb_rate: Optional[float] = None
    rate_film: Optional[float] = None
    poster:  Optional[str] = None
    year_create:  Optional[int] = None



class My_spisok_with_person_info(My_spisok):
    description: str|None
    name_profession: str|None

class My_spisok_full(My_spisok):
    description: str|None
    age_rating: int|None
    movie_length: int|None
    total_series_length: int|None
    is_film: bool|None
    type_name: str|None
    id_link: int|None
    oid_link: int|None

class Countries(BaseModel):
    id:  Optional[int] = None
    name: str|None

class Genres(BaseModel):
    id:  Optional[int] = None
    name: str|None

class Reviews_my_spisok(BaseModel):
    profile_hash_user_id: str|None
    id_title: Optional[int] = None
    type_review: int|None
    nickname: str|None
    block_account: bool|None

    count_dislike: int|None
    count_like: int|None
    date_review: str|None
    title_name: str|None
    title_content: str|None

class Reviews_my_spisok_with_user_rate(Reviews_my_spisok):
    user_rate: Optional[int] = None

class mini_Persons(BaseModel):
    id: Optional[int] = None
    name: str|None
    photo: Optional[str] = None
    poster: Optional[str] = None


class Persons(BaseModel):
    name: str | None
    photo: str | None
    enname: str | None
    age: int | None
    sex: str | None
    growth: int | None

class Persons_for_title(BaseModel):
    id: int|None
    name: str|None
    photo: str|None
    name_profession: str|None
    description: str|None

class User_Rate(BaseModel):
    user_rate: int|None

# %% for menu
class MiniPersonsTop(mini_Persons):
    type_object: int
class My_spisokTop(My_spisok):
    type_object: int
class MySpisokFacts(My_spisok):
    fact: str


class Person_titles_for_menu(BaseModel):
    id_object: int|None
    id: int | None
    name: str|None
    photo: str|None
    title_name: str|None
    title_id: int | None

class My_spisok_menu_info_fact(My_spisok):
    text: Optional[str] = None

class MovieTitlesForMenuOnGenres(BaseModel):
    id_genres: int|None
    name: str|None
    movie_name: str|None
    id: int|None
    poster: str|None

class My_spiok_favorite(My_spisok):
    object_type: int|None
    user_rate: int|None

# %% users
class Users(BaseModel):
    id_user_django: int|None
    nickname: Optional[str] = None
    block_account: bool|None

"""r.title_name, r.type_review ,r.title_content, as date_review, ms.poster, ms.name, id_title,
 ms.imdb_rate, ms.kp_rate, rf.rate as rate_film,  user_rate"""
class UserReviews(BaseModel):
    id_title: int|None
    poster: str|None
    name: str|None
    # count_dislike: Optional[int] | None
    # count_like: Optional[int] | None

    date_review: str | None
    type_review: int | None
    title_name: str | None
    title_content: str | None

    kp_rate: float | None
    imdb_rate: Optional[float] = None
    rate_film: Optional[float] = None
    user_rate: Optional[int] = None
class UserAvgRate(BaseModel):
    user_avg_rate: float|None

class User_Review_block(BaseModel):
    id_title: Optional[int] = None
    name: str | None
    poster: str | None

    type_review: int | None
    date_review: str | None
    title_name: str | None
    title_content: str | None

    kp_rate: float | None
    imdb_rate: Optional[float] = 0
    rate_film: Optional[float] = 0
    user_rate: Optional[int] = 0

class User_Reviews_my_spisok(User_Review_block):
    count_dislike: Optional[int] = None
    count_like: Optional[int] = None

class User_Review(User_Review_block):
    user_like: Optional[int] = None
    user_dislike: Optional[int] = None