# pyspark-product-category

PySpark-функция, которая:

1. Склеивает продукты, категории и связывающую таблицу.
2. Возвращает **все** пары «product_name – category_name».
3. Продукты без категорий тоже попадают (category_name = `null`).

---

## Быстрый старт

```bash
git clone https://github.com/<you>/pyspark-product-category.git
cd pyspark-product-category
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
pytest            # должно быть green
