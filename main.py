import logging
import uvicorn
from fastapi import FastAPI

from parser.views import router as router_parser
from parser.view_get import router as router_data
from core.config import settings
logger = logging.getLogger(__name__)

app = FastAPI(title=settings.app_name)

app.include_router(router_parser)
app.include_router(router_data)



if __name__ == '__main__':
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
