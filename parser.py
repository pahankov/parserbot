import sqlite3
import requests
import logging
import hashlib
import time
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm

# ---------------------------
# Конфигурация
# ---------------------------
class Config:
    DB_NAME = 'recipes.db'
    BASE_URL = 'https://www.russianfood.com'
    SITEMAP_URL = f'{BASE_URL}/sitemap.xml'
    REQUEST_DELAY = 1  # Задержка между запросами в секундах
    TIMEOUT = 10  # Таймаут для HTTP-запросов
    LOG_FILE = 'parser.log'

# ---------------------------
# Настройка логирования
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Config.LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------------------
# Инициализация БД
# ---------------------------
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

# ---------------------------
# Вспомогательные функции
# ---------------------------
def get_or_create(table: str, name: str, cursor: sqlite3.Cursor) -> int:
    try:
        cursor.execute(f'SELECT id FROM {table} WHERE name = ?', (name,))
        result = cursor.fetchone()
        if result:
            return result[0]
        cursor.execute(f'INSERT INTO {table} (name) VALUES (?)', (name,))
        return cursor.lastrowid
    except Exception as e:
        logger.error(f"Error in get_or_create: {str(e)}")
        raise

def calculate_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()

def parse_quantity(text: str) -> tuple:
    try:
        # Обработка дробей и сложных форматов
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

# ---------------------------
# Парсер рецептов
# ---------------------------
class RecipeParser:
    def __init__(self, cursor: sqlite3.Cursor):
        self.cursor = cursor

    def parse_recipe_page(self, url: str):
        try:
            # Загрузка страницы
            time.sleep(Config.REQUEST_DELAY)
            response = requests.get(url, timeout=Config.TIMEOUT)
            response.raise_for_status()

            # Расчет хеша контента
            content_hash = calculate_hash(response.text)

            # Проверка на существование
            self.cursor.execute('''
                SELECT id, content_hash FROM recipe
                WHERE url = ?
            ''', (url,))
            existing = self.cursor.fetchone()

            if existing:
                if existing[1] == content_hash:
                    logger.debug(f"No changes detected: {url}")
                    return
                logger.info(f"Updating recipe: {url}")

            soup = BeautifulSoup(response.text, 'html.parser')

            # Извлечение данных
            recipe_data = self._extract_recipe_data(soup, url, content_hash)
            ingredients = self._extract_ingredients(soup)

            # Сохранение в БД
            self._save_recipe(recipe_data, ingredients, existing)

        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")

    def _extract_recipe_data(self, soup: BeautifulSoup, url: str, content_hash: str) -> dict:
        data = {
            'url': url,
            'content_hash': content_hash,
            'last_updated': datetime.now()
        }

        # Заголовок
        title_elem = soup.find('h1', itemprop='name')
        data['title'] = title_elem.text.strip() if title_elem else ''

        # Характеристики
        characteristics = {}
        table = soup.find('div', class_='recipe_about')
        if table:
            for row in table.find_all('tr'):
                key = row.th.text.strip().replace(':', '')
                value = row.td.text.strip()
                characteristics[key] = value

        # Парсинг характеристик
        mapping = {
            'Категория': ('category', 'category'),
            'Кухня': ('cuisine', 'cuisine'),
            'Тип блюда': ('dish_type', 'dish_type'),
            'Повод': ('purpose', 'purpose')
        }

        for key, (table_name, field) in mapping.items():
            if key in characteristics:
                value = characteristics[key]
                data[field] = get_or_create(table_name, value, self.cursor)
            else:
                data[field] = None

        return data

    def _extract_ingredients(self, soup: BeautifulSoup) -> list:
        ingredients = []
        ingr_block = soup.find('div', class_='ingr')
        if ingr_block:
            for li in ingr_block.find_all('li'):
                text = li.text.strip()
                if ' - ' in text:
                    name_part, quantity_part = text.split(' - ', 1)
                else:
                    name_part, quantity_part = text, ''

                quantity, unit = parse_quantity(quantity_part)
                ingredients.append({
                    'name': name_part.strip(),
                    'quantity': quantity,
                    'unit': unit
                })
        return ingredients

    def _save_recipe(self, recipe_data: dict, ingredients: list, existing: tuple):
        try:
            if existing:
                # Обновление существующего рецепта
                recipe_id = existing[0]
                self.cursor.execute('''
                    UPDATE recipe SET
                        title = ?,
                        category_id = ?,
                        cuisine_id = ?,
                        dish_type_id = ?,
                        purpose_id = ?,
                        content_hash = ?,
                        last_updated = ?
                    WHERE id = ?
                ''', (
                    recipe_data['title'],
                    recipe_data['category'],
                    recipe_data['cuisine'],
                    recipe_data['dish_type'],
                    recipe_data['purpose'],
                    recipe_data['content_hash'],
                    recipe_data['last_updated'],
                    recipe_id
                ))
            else:
                # Создание нового рецепта
                self.cursor.execute('''
                    INSERT INTO recipe (
                        title, url, category_id, cuisine_id,
                        dish_type_id, purpose_id, content_hash, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    recipe_data['title'],
                    recipe_data['url'],
                    recipe_data['category'],
                    recipe_data['cuisine'],
                    recipe_data['dish_type'],
                    recipe_data['purpose'],
                    recipe_data['content_hash'],
                    recipe_data['last_updated']
                ))
                recipe_id = self.cursor.lastrowid

            # Сохранение ингредиентов
            self.cursor.execute('DELETE FROM ingredient WHERE recipe_id = ?', (recipe_id,))
            for ingr in ingredients:
                self.cursor.execute('''
                    INSERT INTO ingredient
                    (recipe_id, name, quantity, unit)
                    VALUES (?, ?, ?, ?)
                ''', (recipe_id, ingr['name'], ingr['quantity'], ingr['unit']))

            logger.info(f"{'Updated' if existing else 'Added'} recipe: {recipe_data['title']}")

        except Exception as e:
            logger.error(f"Error saving recipe: {str(e)}")
            raise

# ---------------------------
# Основная логика
# ---------------------------
def main():
    init_db()
    conn = sqlite3.connect(Config.DB_NAME)
    cursor = conn.cursor()

    try:
        # Получение списка URL
        logger.info("Fetching sitemap...")
        response = requests.get(Config.SITEMAP_URL, timeout=Config.TIMEOUT)
        soup = BeautifulSoup(response.content, 'xml')
        urls = [loc.text for loc in soup.find_all('loc') if 'recipe' in loc.text]
        logger.info(f"Found {len(urls)} recipe URLs")

        # Инициализация парсера
        parser = RecipeParser(cursor)

        # Обработка рецептов с прогресс-баром
        for url in tqdm(urls, desc="Processing recipes"):
            try:
                parser.parse_recipe_page(url)
                conn.commit()
            except Exception as e:
                logger.error(f"Critical error processing {url}: {str(e)}")
                conn.rollback()

    finally:
        conn.close()
        logger.info("Processing completed")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
