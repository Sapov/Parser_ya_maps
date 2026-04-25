from parser.models.category import Category
from .base import BaseRepository
import logging

logger = logging.getLogger(__name__)


class CategoryRepository(BaseRepository[Category]):
    """Репозиторий для работы с категориями"""

    def get_model(self):
        return Category

    def get_or_create(self, category_name: str) -> Category:
        """Получить или создать категорию"""
        category = self.session.query(Category).filter_by(category=category_name).first()
        if not category:
            category = Category(category=category_name)
            self.session.add(category)
            self.session.flush()
            logger.info(f"Создана новая категория: {category_name}")
        return category

    def get_by_name(self, category_name: str) -> Category:
        """Получить категорию по имени"""
        return self.session.query(Category).filter_by(category=category_name).first()
