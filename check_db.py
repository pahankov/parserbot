import sqlite3

# Подключение к базе данных
conn = sqlite3.connect('recipes.db')
cursor = conn.cursor()

# Запрос данных из таблицы recipes
cursor.execute("SELECT * FROM recipes")
recipes = cursor.fetchall()
print("=== Recipes ===")
for row in recipes:
    print(row)

# Запрос данных из таблицы ingredients
cursor.execute("SELECT * FROM ingredients")
ingredients = cursor.fetchall()
print("\n=== Ingredients ===")
for row in ingredients:
    print(row)

conn.close()