import uvicorn
from fastapi import FastAPI
from parser.views import router as router_parser

app = FastAPI()
app.include_router(router_parser)

@app.get('/hello')
def get_hellow():
    return 'hello'


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}


if __name__ == '__main__':
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
