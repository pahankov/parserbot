# parser.py
import requests
import random
import logging
import time
import os
from logging.handlers import RotatingFileHandler
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm
from database import DatabaseManager


class Config:
    DB_NAME = 'recipes.db'
    BASE_URL = 'https://www.povarenok.ru'
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    DELAY = (1, 3)
    MAX_DEPTH = 3
    MAX_LOG_SIZE = 1 * 1024 * 1024  # 1 MB


def setup_logger():
    if os.path.exists('parser.log'):
        os.remove('parser.log')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = RotatingFileHandler(
        'parser.log',
        maxBytes=Config.MAX_LOG_SIZE,
        backupCount=1,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


setup_logger()
logger = logging.getLogger(__name__)


class PovarenokParser:
    def __init__(self):
        self.db = DatabaseManager(Config.DB_NAME)
        self.session = requests.Session()
        self.session.headers.update(Config.HEADERS)
        self.processed_urls = set()
        self.failed_urls = set()
        self.total_recipes = 0
        self.main_pbar = None

    def get_page(self, url):
        try:
            delay = random.uniform(*Config.DELAY)
            time.sleep(0.1)
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Ошибка загрузки {url}: {str(e)}")
            self.failed_urls.add(url)
            return None

    def parse_categories(self):
        logger.info("=== НАЧАЛО ПАРСИНГА ===")
        start_url = urljoin(Config.BASE_URL, '/recipes/')
        self.main_pbar = tqdm(desc="Общий прогресс", position=0, ncols=100)
        self._parse_category(start_url)
        self.main_pbar.close()

    def _parse_category(self, url, parent_id=None, depth=0):
        if depth > Config.MAX_DEPTH or url in self.processed_urls:
            return

        html = self.get_page(url)
        if not html:
            return

        self.processed_urls.add(url)
        soup = BeautifulSoup(html, 'lxml')

        categories = soup.select('ul.recipe-categories > li > a[href]')
        category_pbar = tqdm(
            total=len(categories),
            desc=f"Уровень {depth}",
            position=depth + 1,
            leave=False,
            ncols=100
        )

        for cat in categories:
            cat_url = urljoin(Config.BASE_URL, cat['href'])
            cat_name = cat.get_text(strip=True)
            count = self._extract_count(cat.parent)

            cat_id = self.db.insert_category(
                name=cat_name,
                url=cat_url,
                parent_id=parent_id,
                recipe_count=count
            )

            if count > 0:
                self._parse_recipe_list(cat_url, cat_id)

            self._parse_category(cat_url, cat_id, depth + 1)
            category_pbar.update(1)
            self.main_pbar.update(1)

        category_pbar.close()

    def _extract_count(self, element):
        count_tag = element.find('span', class_='count')
        if count_tag:
            try:
                return int(count_tag.text.replace(' ', ''))
            except ValueError:
                pass
        return 0

    def _parse_recipe_list(self, url, category_id):
        try:
            recipe_links = []
            page = 1
            while True:
                page_url = f"{url}?p={page}" if page > 1 else url
                html = self.get_page(page_url)
                if not html:
                    break

                soup = BeautifulSoup(html, 'lxml')
                links = [
                    urljoin(Config.BASE_URL, a['href'])
                    for a in soup.select('a.recipe-title[href^="/recipes/show/"]')
                ]
                if not links:
                    break

                recipe_links.extend(links)
                page += 1
                time.sleep(random.uniform(0.1, 0.2))

            recipe_pbar = tqdm(
                total=len(recipe_links),
                desc="Рецепты",
                position=Config.MAX_DEPTH + 2,
                leave=False,
                ncols=100
            )

            for recipe_url in recipe_links:
                self._parse_recipe(recipe_url, category_id)
                self.total_recipes += 1
                recipe_pbar.update(1)
                self.main_pbar.update(1)

            recipe_pbar.close()

        except Exception as e:
            logger.error(f"Ошибка парсинга категории {url}: {str(e)}")

    def _parse_recipe(self, url, category_id):
        try:
            if url in self.processed_urls:
                return

            html = self.get_page(url)
            if not html:
                return

            self.processed_urls.add(url)
            soup = BeautifulSoup(html, 'lxml')

            title = soup.select_one('h1[itemprop="name"]')
            if not title:
                logger.warning(f"Не найден заголовок: {url}")
                return

            recipe_data = {
                'title': title.get_text(strip=True),
                'url': url,
                'image_url': self._get_image(soup),
                'category_id': category_id,
                'ingredients': self._get_ingredients(soup),
                'nutrition': self._get_nutrition(soup),
                'instructions': self._get_instructions(soup)
            }

            if self.db.insert_recipe(recipe_data):
                logger.info(f"Успешно: {recipe_data['title']}")
            else:
                logger.warning(f"Ошибка сохранения: {recipe_data['title']}")

        except Exception as e:
            logger.error(f"Ошибка парсинга рецепта {url}: {str(e)}")

    @staticmethod
    def _get_image(soup):
        img = soup.select_one('img[itemprop="image"]')
        return urljoin(Config.BASE_URL, img['src']) if img else ''

    @staticmethod
    def _get_nutrition(soup):
        nutrition = {}
        table = soup.find('table', class_='nutrition-table')
        if table:
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) == 2:
                    key = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    nutrition[key] = value
        return nutrition

    @staticmethod
    def _get_instructions(soup):
        steps = []
        container = soup.find('div', class_='steps')
        if container:
            for step in container.find_all('p'):
                steps.append(step.get_text(strip=True))
        return '\n'.join(steps)

    def _get_ingredients(self, soup):
        ingredients = []
        container = soup.find('div', class_='ingredients')
        if container:
            for item in container.find_all('li'):
                ingredient = {
                    'name': self._get_clean_text(item, 'a'),
                    'quantity': self._get_clean_text(item, 'span.quantity'),
                    'unit': self._get_clean_text(item, 'span.unit'),
                    'component': self._get_component(item)
                }
                ingredients.append(ingredient)
        return ingredients

    @staticmethod
    def _get_clean_text(element, selector):
        elem = element.select_one(selector)
        return elem.get_text(strip=True) if elem else ''

    @staticmethod
    def _get_component(element):
        parent = element.find_parent('div', class_='component')
        if parent:
            title = parent.find('div', class_='component-title')
            return title.get_text(strip=True) if title else ''
        return ''

    def run(self):
        try:
            self.parse_categories()
        except KeyboardInterrupt:
            logger.warning("Парсинг прерван пользователем")
        finally:
            self.db.close()
            logger.info(f"""
                === ИТОГИ ===
                Обработано: {len(self.processed_urls)} URL
                Сохранено рецептов: {self.total_recipes}
                Ошибок: {len(self.failed_urls)}
            """)


if __name__ == '__main__':
    parser = PovarenokParser()
    parser.run()