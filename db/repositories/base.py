from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List, Optional
from sqlalchemy.orm import Session

T = TypeVar('T')


class BaseRepository(Generic[T], ABC):
    """Базовый репозиторий с общими методами"""

    def __init__(self, session: Session):
        self.session = session

    @abstractmethod
    def get_model(self) -> T:
        pass

    def get_by_id(self, id: int) -> Optional[T]:
        return self.session.get(self.get_model(), id)

    def get_all(self) -> List[T]:
        return self.session.query(self.get_model()).all()

    def add(self, entity: T) -> T:
        self.session.add(entity)
        self.session.flush()
        return entity

    def delete(self, entity: T) -> None:
        self.session.delete(entity)

    def bulk_add(self, entities: List[T]) -> None:
        self.session.bulk_save_objects(entities)
