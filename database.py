# database.py
import sqlite3
import logging
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.init_db()

    def init_db(self):
        """Инициализация таблиц в базе данных"""
        tables = [
            '''CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                parent_id INTEGER,
                url TEXT UNIQUE NOT NULL,
                recipe_count INTEGER DEFAULT 0,
                path TEXT
            )''',
            '''CREATE TABLE IF NOT EXISTS recipes (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                image_url TEXT,
                category_id INTEGER,
                calories TEXT,
                proteins TEXT,
                fats TEXT,
                carbohydrates TEXT,
                content_hash TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (category_id) REFERENCES categories(id)
            )''',
            '''CREATE TABLE IF NOT EXISTS ingredients (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                url TEXT
            )''',
            '''CREATE TABLE IF NOT EXISTS recipe_ingredients (
                recipe_id INTEGER NOT NULL,
                ingredient_id INTEGER NOT NULL,
                quantity TEXT,
                unit TEXT,
                component TEXT,
                is_compound BOOLEAN DEFAULT 0,
                PRIMARY KEY (recipe_id, ingredient_id),
                FOREIGN KEY (recipe_id) REFERENCES recipes(id),
                FOREIGN KEY (ingredient_id) REFERENCES ingredients(id)
            )'''
        ]
        try:
            for table in tables:
                self.cursor.execute(table)
            self.conn.commit()
        except Exception as e:
            logger.error(f"Database initialization failed: {str(e)}")
            raise

    # Методы для работы с категориями
    def insert_category(self, name, url, parent_id=None, recipe_count=0, path=''):
        try:
            self.cursor.execute('''
                INSERT OR IGNORE INTO categories 
                (name, parent_id, url, recipe_count, path)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, parent_id, url, recipe_count, path))
            self.conn.commit()
            return self.cursor.lastrowid
        except Exception as e:
            logger.error(f"Error inserting category: {str(e)}")
            return None

    def get_categories(self):
        self.cursor.execute('SELECT url, path FROM categories ORDER BY path')
        return self.cursor.fetchall()

    # Методы для работы с рецептами
    def insert_recipe(self, recipe_data):
        try:
            content_hash = hashlib.sha256((
                                                  recipe_data['title'] +
                                                  recipe_data['url'] +
                                                  str(recipe_data['ingredients'])
                                          ).hexdigest())

            self.cursor.execute('''
                INSERT OR REPLACE INTO recipes (
                    title, url, image_url, category_id,
                    calories, proteins, fats, carbohydrates, content_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                recipe_data['title'],
                recipe_data['url'],
                recipe_data['image_url'],
                recipe_data['category_id'],
                recipe_data['nutrition'].get('calories', ''),
                recipe_data['nutrition'].get('proteins', ''),
                recipe_data['nutrition'].get('fats', ''),
                recipe_data['nutrition'].get('carbohydrates', ''),
                content_hash
            ))
            recipe_id = self.cursor.lastrowid

            # Сохраняем ингредиенты
            for ingredient in recipe_data['ingredients']:
                self.cursor.execute('''
                    INSERT OR IGNORE INTO ingredients (name, url)
                    VALUES (?, ?)
                ''', (ingredient['name'], ingredient.get('url', '')))

            ingredient_id = self.cursor.execute('''
                    SELECT id FROM ingredients WHERE name = ?
                ''', (ingredient['name'],)).fetchone()[0]

            self.cursor.execute('''
                    INSERT OR REPLACE INTO recipe_ingredients
                    (recipe_id, ingredient_id, quantity, unit, component, is_compound)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                recipe_id,
                ingredient_id,
                ingredient.get('quantity', ''),
                ingredient.get('unit', ''),
                ingredient.get('component', ''),
                ingredient.get('is_compound', 0)
            ))

            self.conn.commit()
            return recipe_id
        except Exception as e:
            logger.error(f"Error inserting recipe: {str(e)}")
            self.conn.rollback()
            return None

    def close(self):
        self.conn.close()
        logger.info("Database connection closed")