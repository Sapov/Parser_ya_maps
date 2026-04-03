import uvicorn
from fastapi import FastAPI
from parser import router as router_parser
from core.config import settings

app = FastAPI(title=settings.app_name)

app.include_router(router_parser)


if __name__ == '__main__':
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
