from fastapi import APIRouter
from .parser_card import ParserCard

router = APIRouter(prefix='/parser', tags=['parser'])


@router.get('/')
def hello():
    return 'HE'


@router.post('/run/')
def run_parser(category: str, city: str, quantity:int):
    search = ParserCard(category, city, quantity)
    search.run()
    # ParserPage().run()

    return 'wait'