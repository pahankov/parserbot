# parser.py
import requests
import logging
import time
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm
from database import DatabaseManager
import os

class Config:
    DB_NAME = 'recipes.db'
    BASE_URL = 'https://www.povarenok.ru'
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    DELAY = (0.1, 0.1)  # Увеличьте задержку при необходимости
    START_ID = 1     # Начальный ID
    END_ID = 200     # Конечный ID


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('parser.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


setup_logger()
logger = logging.getLogger(__name__)


class PovarenokParser:
    def __init__(self):

        if os.path.exists(Config.DB_NAME):
            os.remove(Config.DB_NAME)

        self.db = DatabaseManager(Config.DB_NAME)  # Инициализация базы данных
        self.session = requests.Session()
        self.session.headers.update(Config.HEADERS)
        self.processed_urls = set()
        self.failed_urls = set()
        self.total_recipes = 0

    def parse_recipes(self):
        logger.info("=== НАЧАЛО ПАРСИНГА РЕЦЕПТОВ ===")
        total_ids = Config.END_ID - Config.START_ID + 1

        # Прогресс-бар для отслеживания
        with tqdm(total=total_ids, desc="Обработка рецептов") as pbar:
            for recipe_id in range(Config.START_ID, Config.END_ID + 1):
                recipe_url = f"{Config.BASE_URL}/recipes/show/{recipe_id}/"
                self._parse_recipe(recipe_url)
                pbar.update(1)

    def _parse_recipe(self, url):
        if url in self.processed_urls:
            logger.debug(f"Рецепт уже обработан: {url}")
            return

        logger.debug(f"Загрузка рецепта: {url}")
        try:
            time.sleep(random.uniform(*Config.DELAY))
            response = self.session.get(url, timeout=15)

            if response.status_code != 200:
                logger.warning(f"Ошибка {response.status_code}: {url}")
                self.failed_urls.add(url)
                return

            soup = BeautifulSoup(response.text, 'lxml')
            self.processed_urls.add(url)

            # Актуальный селектор для заголовка
            title_element = soup.select_one('h1[itemprop="name"]')
            if not title_element:
                logger.error(f"Заголовок не найден: {url}")
                return

            # Извлечение данных
            recipe_data = {
                'title': title_element.get_text(strip=True),
                'url': url,
                'image_url': self._get_image(soup),
                'category': self._get_category(soup),
                'nutrition': self._get_nutrition(soup),
                'ingredients': self._get_ingredients(soup),
                'instructions': self._get_instructions(soup)
            }

            if self.db.insert_recipe(recipe_data):
                logger.info(f"Сохранен: {recipe_data['title']}")
                self.total_recipes += 1

        except Exception as e:
            logger.error(f"Ошибка парсинга: {url} ({str(e)})", exc_info=True)
            self.failed_urls.add(url)

    @staticmethod
    def _get_text(soup, selector):
        element = soup.select_one(selector)
        return element.get_text(strip=True) if element else ''


    def _get_image(self, soup):  # Убран декоратор @staticmethod
        img = soup.select_one('img.recipe-image')
        return urljoin(Config.BASE_URL, img['src']) if img else ''

    def _get_category(self, soup):
        breadcrumbs = soup.select('ul.breadcrumbs li a[href*="/recipes/category/"]')
        return breadcrumbs[-1].text.strip() if breadcrumbs else 'Без категории'

    def _get_ingredients(self, soup):
        ingredients = []
        # Новые селекторы для ингредиентов
        items = soup.select('div.ingredients-list li.ingredient')
        for item in items:
            ingredient = {
                'name': self._get_text(item, 'span.ingredient-name'),
                'quantity': self._get_text(item, 'span.ingredient-quantity'),
                'unit': self._get_text(item, 'span.ingredient-unit')
            }
            if ingredient['name']:
                ingredients.append(ingredient)
        return ingredients

    def _get_instructions(self, soup):
        steps = soup.select('div.recipe-steps div.step-text')
        return '\n'.join([step.get_text(strip=True) for step in steps])

    def _get_nutrition(self, soup):
        nutrition = {}
        # Новые селекторы для таблицы с питательной ценностью
        rows = soup.select('div.nutrition-facts table tr')
        for row in rows:
            cells = row.select('td')
            if len(cells) == 2:
                key = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                # Преобразуем в число, если возможно
                try:
                    nutrition[key] = float(value)
                except ValueError:
                    nutrition[key] = value  # или 0.0, если нужно число
        return nutrition

    def close(self):
        self.db.close()


if __name__ == '__main__':
    parser = PovarenokParser()
    try:
        parser.parse_recipes()
    except KeyboardInterrupt:
        logger.warning("Парсинг прерван пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
    finally:
        parser.close()
        logger.info(f"""
            === ИТОГИ ===
            Обработано URL: {len(parser.processed_urls)}
            Сохранено рецептов: {parser.total_recipes}
            Ошибок: {len(parser.failed_urls)}
        """)
