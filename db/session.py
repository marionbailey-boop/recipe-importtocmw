from contextlib import contextmanager
from db.connection import get_connection


@contextmanager
def db_cursor():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
