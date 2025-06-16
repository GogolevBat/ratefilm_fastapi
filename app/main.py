from fastapi import FastAPI
from .routers import title, person, find, menu, account, user
import asyncio, uvicorn, sys


app = FastAPI(title="FastAPI")

app.include_router(title.router, prefix="/title")
app.include_router(person.router, prefix="/person")
app.include_router(find.router, prefix="/find")
app.include_router(menu.router, prefix="/menu")
app.include_router(account.router, prefix="/account")
app.include_router(user.router, prefix="/user")


@app.get('/')
async def get_menu_update():
    return {"message": 'test', 'result': "hello"}

if __name__ == "__main__":
    uvicorn.run("main:app", port=8000, reload=True)
