# parser.py
import requests
import random
import logging
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from tqdm import tqdm
from database import DatabaseManager


class Config:
    DB_NAME = 'recipes.db'
    BASE_URL = 'https://www.povarenok.ru'
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    DELAY = (1, 3)  # Случайная задержка между запросами
    MAX_DEPTH = 3  # Максимальная глубина вложенности категорий


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PovarenokParser:
    def __init__(self):
        self.db = DatabaseManager(Config.DB_NAME)
        self.session = requests.Session()
        self.session.headers.update(Config.HEADERS)
        self.processed_urls = set()

    def get_page(self, url):
        """Загрузка страницы с обработкой ошибок"""
        try:
            time.sleep(random.uniform(*Config.DELAY))
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error loading {url}: {str(e)}")
            return None

    def parse_categories(self, url=None, parent_id=None, depth=0, path=''):
        """Рекурсивный парсинг категорий"""
        if depth > Config.MAX_DEPTH:
            return

        url = url or urljoin(Config.BASE_URL, '/recipes/')
        if url in self.processed_urls:
            return

        logger.info(f"Parsing category: {url}")
        html = self.get_page(url)
        if not html:
            return

        soup = BeautifulSoup(html, 'lxml')
        self.processed_urls.add(url)

        # Парсим подкатегории
        for item in soup.select('ul.recipe-categories > li'):
            category = self.parse_category_item(item, parent_id, depth, path)
            if category:
                self.parse_categories(
                    url=category['url'],
                    parent_id=category['id'],
                    depth=depth + 1,
                    path=category['path']
                )

    def parse_category_item(self, item, parent_id, depth, path):
        """Обработка отдельной категории"""
        link = item.find('a', href=True)
        if not link:
            return None

        category_url = urljoin(Config.BASE_URL, link['href'])
        category_name = link.get_text(strip=True)

        # Получаем количество рецептов
        count_tag = item.find('span', class_='count')
        recipe_count = int(count_tag.text.strip().replace(' ', '')) if count_tag else 0

        # Формируем путь
        new_path = f"{path}/{category_name}" if path else category_name

        # Сохраняем в БД
        category_id = self.db.insert_category(
            name=category_name,
            url=category_url,
            parent_id=parent_id,
            recipe_count=recipe_count,
            path=new_path
        )

        logger.info(f"Category saved: {new_path} ({recipe_count} recipes)")
        return {'id': category_id, 'url': category_url, 'path': new_path}

    def parse_recipes(self, category_url):
        """Парсинг рецептов в категории"""
        page = 1
        while True:
            url = f"{category_url}?p={page}"
            html = self.get_page(url)
            if not html:
                break

            soup = BeautifulSoup(html, 'lxml')
            recipes = soup.select('div.recipe-item a[href^="/recipes/show/"]')

            if not recipes:
                break

            for recipe in recipes:
                recipe_url = urljoin(Config.BASE_URL, recipe['href'])
                if recipe_url not in self.processed_urls:
                    self.parse_recipe(recipe_url)
                    self.processed_urls.add(recipe_url)

            # Проверяем наличие следующей страницы
            if not soup.select_one('a.paginator__item-arrow[rel="next"]'):
                break

            page += 1

    def parse_recipe(self, url):
        """Парсинг конкретного рецепта"""
        logger.info(f"Parsing recipe: {url}")
        html = self.get_page(url)
        if not html:
            return

        soup = BeautifulSoup(html, 'lxml')

        recipe_data = {
            'title': self.get_recipe_title(soup),
            'url': url,
            'image_url': self.get_recipe_image(soup),
            'category_id': self.get_recipe_category(url),
            'nutrition': self.get_nutrition_info(soup),
            'ingredients': self.get_ingredients(soup)
        }

        if self.db.insert_recipe(recipe_data):
            logger.info(f"Recipe saved: {recipe_data['title']}")

    def get_recipe_title(self, soup):
        title_tag = soup.find('h1', itemprop='name')
        return title_tag.text.strip() if title_tag else 'No title'

    def get_recipe_image(self, soup):
        img_tag = soup.find('img', itemprop='image')
        return urljoin(Config.BASE_URL, img_tag['src']) if img_tag else ''

    def get_recipe_category(self, url):
        path_parts = urlparse(url).path.split('/')
        category_url = urljoin(Config.BASE_URL, '/'.join(path_parts[:4]))
        self.db.cursor.execute('SELECT id FROM categories WHERE url = ?', (category_url,))
        return self.db.cursor.fetchone()[0] if self.db.cursor.fetchone() else None

    def get_nutrition_info(self, soup):
        nutrition = {}
        table = soup.find('table', class_='nutrition-table')
        if table:
            for row in table.select('tr'):
                cells = row.select('td')
                if len(cells) == 2:
                    key = cells[0].text.strip().lower()
                    value = cells[1].text.strip()
                    nutrition[key] = value
        return nutrition

    def get_ingredients(self, soup):
        ingredients = []
        container = soup.find('div', class_='ingredients')
        if not container:
            return ingredients

        for group in container.select('div.component, div.ingredient-group'):
            if 'component' in group.get('class', []):
                ingredients.extend(self.parse_complex_ingredient(group))
            else:
                ingredients.extend(self.parse_simple_ingredient(group))

        return ingredients

    def parse_complex_ingredient(self, group):
        component_name = group.find('div', class_='component-title').text.strip()
        ingredients = []
        for item in group.select('li.component-item'):
            ingredient = self.parse_ingredient_item(item)
            ingredient['component'] = component_name
            ingredient['is_compound'] = True
            ingredients.append(ingredient)
        return ingredients

    def parse_simple_ingredient(self, group):
        ingredients = []
        for item in group.select('li:not(.component-item)'):
            ingredient = self.parse_ingredient_item(item)
            ingredients.append(ingredient)
        return ingredients

    def parse_ingredient_item(self, item):
        link = item.find('a', href=True)
        quantity = item.find('span', class_='quantity')
        unit = item.find('span', class_='unit')

        return {
            'name': link.text.strip() if link else item.text.strip(),
            'url': urljoin(Config.BASE_URL, link['href']) if link else '',
            'quantity': quantity.text.strip() if quantity else '',
            'unit': unit.text.strip() if unit else '',
            'is_compound': False
        }

    def run(self):
        try:
            # Парсим категории
            if not self.db.get_categories():
                logger.info("Starting category parsing...")
                self.parse_categories()

            # Парсим рецепты
            logger.info("Starting recipe parsing...")
            for category_url, path in tqdm(self.db.get_categories(), desc="Categories"):
                self.parse_recipes(category_url)

        except KeyboardInterrupt:
            logger.warning("Parsing interrupted by user")
        finally:
            self.db.close()


if __name__ == '__main__':
    parser = PovarenokParser()
    parser.run()