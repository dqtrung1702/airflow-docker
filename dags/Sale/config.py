CONFIG = {
    "chunk_size": 1000,
    "max_workers": 4,
    "postgres_conn_id": "postgres_default",
    "customers": {
        "table_name": "dev.customers",
        "delete_sql": "DELETE FROM dev.customers WHERE customer_id = ANY(%s)",
        "insert_sql": """
            INSERT INTO dev.customers (
                customer_id, name, email, telephone, city, country, 
                gender, date_of_birth, job_title
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    },
    "employees": {
        "table_name": "dev.employees",
        "delete_sql": "DELETE FROM dev.employees WHERE employee_id = ANY(%s)",
        "insert_sql": """
            INSERT INTO dev.employees (
                employee_id, store_id, name, position
            ) VALUES (%s, %s, %s, %s)
        """
    },
    "transactions": {
        "table_name": "dev.transactions",
        "delete_sql": """
            DELETE FROM dev.transactions 
            WHERE invoice_id = ANY(%s::varchar[])
        """,
        "insert_sql": """
            INSERT INTO dev.transactions (
                invoice_id, line, customer_id, product_id, size, color, unit_price, quantity, date, discount, 
                line_total, store_id, employee_id, currency, currency_symbol, sku, transaction_type, 
                payment_method, invoice_total
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    },
    "stores": {
        "table_name": "dev.stores",
        "delete_sql": "DELETE FROM dev.stores WHERE store_id = ANY(%s::int[])",
        "insert_sql": """
            INSERT INTO dev.stores (
                store_id, country, city, store_name,
                number_of_employees, zip_code, latitude, longitude
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
    },
    "products": {
        "table_name": "dev.products",
        "delete_sql": "DELETE FROM dev.products WHERE product_id = ANY(%s)",
        "insert_sql": """
            INSERT INTO dev.products (
                product_id, category, sub_category, description_pt, description_de, 
                description_fr, description_es, description_en, description_zh, 
                color, sizes, production_cost
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    },
    "discounts": {
        "table_name": "dev.discounts",
        "delete_sql": "DELETE FROM dev.discounts WHERE start_date >= %s AND end_date <= %s",
        "insert_sql": """
            INSERT INTO dev.discounts (
                start_date, end_date, discount, description, category, sub_category
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
    },
    "weather_forecast": {
        "table_name": "dev.weather_forecast",
        "delete_sql": "DELETE FROM dev.weather_forecast WHERE date in %s",
        "insert_sql": """
            INSERT INTO dev.weather_forecast (
                date, temperature_2m, rain, weather_code, cloud_cover, wind_speed_10m, soil_temperature_0cm
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
    }
}