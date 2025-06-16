from fastapi import APIRouter, Depends

from ..schemas import *
from ..dependencies import *
from datetime import datetime
import json, asyncio, random

router = APIRouter(
    # prefix="/title",
    tags=["account"],
    responses={404: {"description": "Not found"}}
                    )

@router.post('/new')
async def new_user(user_nickname:str, user_id:int):
    await generate_user_hash(user_nickname, user_id)
    await create_user(user_id)

async def generate_user_hash(user_nickname, user_id):
    nickname_list = list(user_nickname)
    random.shuffle(nickname_list)
    user_hash = str(user_id) + ''.join(nickname_list) + datetime.now().strftime('%d%m%Y%H%M%S')
    query = f"""insert into pub_user.user_hash (hash_id ,id_user_django) values (md5('{user_hash}'),{user_id})"""
    await post_fast_req_sessions(query)

async def create_user(user_id):
    query = f"""insert into pub_user.users (id_user_django) values ({user_id})"""
    await post_fast_req_sessions(query)