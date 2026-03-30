from fastapi import APIRouter

router = APIRouter(prefix='/parser', tags=['parser'])


@router.get('/')
def hello():
    return 'HE'
