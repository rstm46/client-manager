import psycopg2


# 1) Создать структуру БД (таблицы)
def create_db(conn):
    """Создаёт структуру БД: клиенты + телефоны (связь один-ко-многим)"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS client_info (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                email VARCHAR(254) UNIQUE NOT NULL
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS number_client (
                id SERIAL PRIMARY KEY,
                phone VARCHAR(20),
                client_id INTEGER NOT NULL REFERENCES client_info(id) ON DELETE CASCADE
            );
        """)
    conn.commit()


# 2) Добавить нового клиента
def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client_info (first_name, last_name, email) 
            VALUES (%s, %s, %s) 
            RETURNING id;
        """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]
        
        if phones:
            phone_list = [phones] if isinstance(phones, str) else phones
            for phone in phone_list:
                cur.execute("""
                    INSERT INTO number_client (phone, client_id) 
                    VALUES (%s, %s);
                """, (phone, client_id))
    
    conn.commit()
    return client_id


# 3) Добавить телефон для существующего клиента
def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO number_client (phone, client_id) 
            VALUES (%s, %s);
        """, (phone, client_id))
    conn.commit()


# 4) Изменить данные о клиенте
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        updates = []
        params = []
        if first_name is not None:
            updates.append("first_name = %s")
            params.append(first_name)
        if last_name is not None:
            updates.append("last_name = %s")
            params.append(last_name)
        if email is not None:
            updates.append("email = %s")
            params.append(email)
        
        if updates:
            params.append(client_id)
            cur.execute(f"""
                UPDATE client_info 
                SET {', '.join(updates)} 
                WHERE id = %s;
            """, params)
        
        if phones is not None:
            cur.execute("""
                DELETE FROM number_client 
                WHERE client_id = %s;
            """, (client_id,))
            
            phone_list = [phones] if isinstance(phones, str) else phones
            for phone in phone_list:
                cur.execute("""
                    INSERT INTO number_client (phone, client_id) 
                    VALUES (%s, %s);
                """, (phone, client_id))
    
    conn.commit()

# 5) Удалить телефон для существующего клиента
def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM number_client 
            WHERE client_id = %s AND phone = %s;
        """, (client_id, phone))
    conn.commit()

# 6) Удалить существующего клиента
def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM client_info 
            WHERE id = %s;
        """, (client_id,))
    conn.commit()


# 7) Найти клиента по его данным (имя, фамилия, телефон, email)
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        conditions = []
        params = []
        if first_name is not None:
            conditions.append("ci.first_name = %s")
            params.append(first_name)
        if last_name is not None:
            conditions.append("ci.last_name = %s")
            params.append(last_name)
        if email is not None:
            conditions.append("ci.email = %s")
            params.append(email)
        if phone is not None:
            conditions.append("nc.phone = %s")
            params.append(phone)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        cur.execute(f"""
            SELECT DISTINCT ci.id, ci.first_name, ci.last_name, ci.email
            FROM client_info ci
            LEFT JOIN number_client nc ON ci.id = nc.client_id
            WHERE {where_clause};
        """, params)
        
        return cur.fetchall()


# Пример использования
if __name__ == "__main__":
    with psycopg2.connect(database="clients_db", user="postgres", password="postgres") as conn:
        # 1. Создаём структуру БД
        create_db(conn)
        
        # 2. Добавляем клиентов
        client1 = add_client(conn, "Иван", "Петров", "ivan@example.com", "+79991112233")
        client2 = add_client(conn, "Мария", "Сидорова", "maria@example.com", ["+79994445566", "+79997778899"])
        add_client(conn, "Анна", "Козлова", "anna@example.com")  # без телефона
        
        # 3. Добавляем телефон существующему клиенту
        add_phone(conn, client1, "+79990001122")
        
        # 4. Изменяем данные
        change_client(conn, client1, email="ivan_new@example.com", phones=["+79993334455"])
        
        # 5. Удаляем телефон
        delete_phone(conn, client1, "+79993334455")
        
        # 6. Поиск клиента по его данным
        print("Поиск по имени 'Иван':", find_client(conn, first_name="Иван"))
        print("Поиск по телефону '+79994445566':", find_client(conn, phone="+79994445566"))
        
        # 7. Удаляем клиента
        delete_client(conn, client2)
    
    # conn.close() вызывается автоматически благодаря with
