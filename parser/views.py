from fastapi import APIRouter

router = APIRouter(prefix='/parser', tags=['parser'])


@router.get('/')
def hello():
    return 'HE'


@router.post('/run/')
def run_parser(category: str, city: str):
    pass
