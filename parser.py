import sqlite3
import requests
import logging
import hashlib
import time
import random
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
import colorlog
from requests.adapters import HTTPAdapter
from urllib.parse import urljoin

class Config:
    DB_NAME = 'recipes.db'
    BASE_URL = 'https://www.russianfood.com'
    CATALOG_URL = f'{BASE_URL}/recipes/'
    TIMEOUT = 10
    MIN_DELAY = 1
    MAX_DELAY = 5
    LOG_FILE = 'parser.log'
    USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    RETRIES = 3

def setup_logging():
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    ))

    file_handler = logging.FileHandler(Config.LOG_FILE, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))

    _logger = colorlog.getLogger()
    _logger.addHandler(handler)
    _logger.addHandler(file_handler)
    _logger.setLevel(logging.DEBUG)
    return _logger

logger = setup_logging()

def init_db():
    conn = sqlite3.connect(Config.DB_NAME)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cuisine (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS category (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dish_type (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS purpose (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS recipe (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        url TEXT UNIQUE NOT NULL,
        cuisine_id INTEGER,
        category_id INTEGER,
        dish_type_id INTEGER,
        purpose_id INTEGER,
        content_hash TEXT,
        last_updated TIMESTAMP,
        FOREIGN KEY (cuisine_id) REFERENCES cuisine(id),
        FOREIGN KEY (category_id) REFERENCES category(id),
        FOREIGN KEY (dish_type_id) REFERENCES dish_type(id),
        FOREIGN KEY (purpose_id) REFERENCES purpose(id)
    )''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ingredient (
        id INTEGER PRIMARY KEY,
        recipe_id INTEGER,
        name TEXT NOT NULL,
        quantity REAL,
        unit TEXT,
        FOREIGN KEY (recipe_id) REFERENCES recipe(id)
    )''')

    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")

def get_retry_session():
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=Config.RETRIES)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    session.trust_env = False  # Игнорировать системные настройки прокси
    session.proxies = {}  # Явно указываем, что прокси не используется

    return session

def get_or_create(table: str, name: str, cursor: sqlite3.Cursor) -> int:
    cursor.execute(f'SELECT id FROM {table} WHERE name = ?', (name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(f'INSERT INTO {table} (name) VALUES (?)', (name,))
    return cursor.lastrowid

def calculate_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()

def parse_quantity(text: str) -> tuple:
    try:
        text = text.replace('½', '0.5').replace('¼', '0.25').replace('¾', '0.75')
        parts = text.split()
        for i, part in enumerate(parts):
            if '/' in part:
                numerator, denominator = part.split('/')
                parts[i] = str(float(numerator) / float(denominator))
        cleaned = ' '.join(parts)
        quantity = ''.join(c for c in cleaned if c.isdigit() or c in ['.', ','])
        quantity = quantity.replace(',', '.').strip()
        unit = cleaned[len(quantity):].strip()
        return float(quantity), unit
    except Exception as e:
        logger.warning(f"Error parsing quantity '{text}': {str(e)}")
        return None, text
class RecipeParser:
    def __init__(self, cursor: sqlite3.Cursor):
        self.cursor = cursor
        self.session = get_retry_session()

    def get_recipe_links(self):
        logger.info("Сбор ссылок на рецепты через каталог")
        try:
            response = self.session.get(
                Config.CATALOG_URL,
                headers={'User-Agent': Config.USER_AGENT},
                timeout=Config.TIMEOUT
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Собираем все категории (пример для "Борщи")
            categories = soup.select('dl.catalogue dt a.resList[href^="/recipes/bytype/?fid="]')

            recipe_links = []
            for category in tqdm(categories, desc="Обработка категорий"):
                try:
                    category_url = urljoin(Config.BASE_URL, category['href'])
                    logger.debug(f"Обработка категории: {category_url}")

                    # Пагинация
                    category_response = self.session.get(category_url)
                    category_soup = BeautifulSoup(category_response.text, 'html.parser')
                    pages = category_soup.select('div.pages a:not(.current)')
                    numeric_pages = [a for a in pages if a.text.strip().isdigit()]
                    last_page = int(numeric_pages[-1].text) if numeric_pages else 1

                    # Сбор ссылок на рецепты
                    for page in range(1, last_page + 1):
                        page_url = f"{category_url}?page={page}"
                        page_response = self.session.get(page_url)
                        page_soup = BeautifulSoup(page_response.text, 'html.parser')

                        # Извлечение ссылок на рецепты
                        links = page_soup.select('div.recipe_title a[href^="/recipes/recipe.php"]')
                        logger.debug(f"Найдено ссылок: {len(links)}")
                        if not links:
                            logger.error(f"Ссылки не найдены!")
                            continue
                        for link in links:
                            recipe_url = urljoin(Config.BASE_URL, link['href'])
                            recipe_links.append(recipe_url)
                            logger.debug(f"Найдена ссылка: {recipe_url}")

                        time.sleep(random.uniform(2, 5))  # Увеличьте задержку

                except Exception as e:
                    logger.error(f"Ошибка обработки {category_url}: {str(e)}")
                    continue

            return list(set(recipe_links))

        except Exception as e:
            logger.error(f"Фатальная ошибка: {str(e)}", exc_info=True)
            return []

    def parse_recipe_page(self, url: str):
        try:
            response = self.session.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Название рецепта
            title = soup.find('h1').text.strip()

            # Ингредиенты
            ingredients = []
            ingr_block = soup.find('div', class_='ingr')
            if ingr_block:
                for li in ingr_block.find_all('li'):
                    text = li.text.strip()
                    name_part, quantity_part = text.split(' - ', 1) if ' - ' in text else (text, '')
                    quantity, unit = parse_quantity(quantity_part)
                    ingredients.append({
                        'name': name_part.strip(),
                        'quantity': quantity,
                        'unit': unit
                    })

            # Инструкция
            instructions = []
            steps = soup.find('div', class_='steps')
            if steps:
                for step in steps.find_all('p'):
                    instructions.append(step.text.strip())

            # Сохранение в БД
            self.save_to_db(title, url, ingredients, instructions)

        except Exception as e:
            logger.error(f"Ошибка парсинга {url}: {str(e)}")

class CaptchaError(Exception):
    pass

def main():
    init_db()
    conn = sqlite3.connect(Config.DB_NAME)
    cursor = conn.cursor()

    try:
        parser = RecipeParser(cursor)
        recipe_urls = parser.get_recipe_links()

        if not recipe_urls:
            logger.error("Не удалось собрать ссылки на рецепты!")
            return

        progress_bar = tqdm(recipe_urls, desc="Обработка рецептов", unit="recipe")
        for url in progress_bar:
            try:
                parser.parse_recipe_page(url)
                conn.commit()
            except Exception as e:
                logger.error(f"Ошибка обработки {url}: {str(e)}")
                conn.rollback()
                time.sleep(random.uniform(Config.MIN_DELAY, Config.MAX_DELAY))

        logger.info("Обработка завершена")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {str(e)}", exc_info=True)
    finally:
        conn.close()

if __name__ == '__main__':
    main()
