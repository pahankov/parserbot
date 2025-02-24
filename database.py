# database.py
import sqlite3
import logging
import os

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self._drop_tables()  # Удаляем старые таблицы
        self._create_tables()

    def _drop_tables(self):
        try:
            self.cursor.execute("DROP TABLE IF EXISTS ingredients")
            self.cursor.execute("DROP TABLE IF EXISTS recipes")
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка удаления таблиц: {str(e)}")
            raise

    def _create_tables(self):
        try:
            # Создаем таблицы с правильным синтаксисом
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS recipes (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    url TEXT UNIQUE NOT NULL,
                    image_url TEXT,
                    category TEXT,
                    calories REAL,
                    proteins REAL,
                    fats REAL,
                    carbohydrates REAL,
                    instructions TEXT
                )
            ''')

            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS ingredients (
                    recipe_id INTEGER,
                    name TEXT NOT NULL,
                    quantity TEXT,
                    unit TEXT,
                    FOREIGN KEY (recipe_id) REFERENCES recipes(id)
                )
            ''')
            self.conn.commit()
        except Exception as e:
            logger.error(f"Ошибка создания таблиц: {str(e)}")
            raise

    # ... остальные методы ...

    def insert_recipe(self, recipe_data):
        try:
            self.cursor.execute('''
                INSERT INTO recipes (
                    title, url, image_url, category,
                    calories, proteins, fats, carbohydrates, instructions
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                recipe_data['title'],
                recipe_data['url'],
                recipe_data['image_url'],
                recipe_data.get('category', ''),
                recipe_data['nutrition'].get('калорийность', 0),
                recipe_data['nutrition'].get('белки', 0),
                recipe_data['nutrition'].get('жиры', 0),
                recipe_data['nutrition'].get('углеводы', 0),
                recipe_data.get('instructions', '')
            ))
            recipe_id = self.cursor.lastrowid
            for ingredient in recipe_data['ingredients']:
                self.cursor.execute('''
                    INSERT INTO ingredients 
                    (recipe_id, name, quantity, unit)
                    VALUES (?, ?, ?, ?)
                ''', (
                    recipe_id,
                    ingredient['name'],
                    ingredient.get('quantity', ''),
                    ingredient.get('unit', '')
                ))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            logger.warning(f"Дубликат рецепта: {recipe_data['url']}")
            return False
        except Exception as e:
            logger.error(f"Ошибка сохранения: {str(e)}")
            self.conn.rollback()
            return False

    def close(self):
        self.conn.close()
