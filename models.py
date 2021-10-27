import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple

ENABLE_FOREIGN_KEY = "PRAGMA foreign_keys = ON;"

DATA = [
    {'id': 1, 'title': 'A Byte of Python', 'author': 'Swaroop C. H.'},
    {'id': 2, 'title': 'Moby-Dick; or, The Whale', 'author': 'Herman Melville'},
    {'id': 3, 'title': 'War and Peace', 'author': 'Leo Tolstoy'},
]

BOOKS_TABLE_NAME = 'books'
AUTHORS_TABLE_NAME = 'authors'


@dataclass
class Author:
    name: str
    id: Optional[int] = None


@dataclass
class Book:
    title: str
    author: str
    id: Optional[int] = None


def init_db(initial_records: List[dict]) -> None:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master "
            f"WHERE type='table' AND name='{AUTHORS_TABLE_NAME}';"
        )
        exists = cursor.fetchone()
        # если таблицы "authors" в БД не существует - производим первоначальную инициализацию данных в БД:
        # создаем две связанные таблицы "authors" и "books", наполняем их первоначальными данными из DATA
        if not exists:
            cursor.executescript(
                f'CREATE TABLE `{AUTHORS_TABLE_NAME}`'
                '(id INTEGER PRIMARY KEY AUTOINCREMENT, name)'
            )
            cursor.executemany(
                f'INSERT INTO `{AUTHORS_TABLE_NAME}` '
                '(id, name) VALUES (?, ?)',
                [(item['id'], item['author']) for item in initial_records]
            )
            cursor.executescript(
                f'CREATE TABLE `{BOOKS_TABLE_NAME}`'
                '(id INTEGER PRIMARY KEY AUTOINCREMENT, title,'
                f'id_author INTEGER NOT NULL REFERENCES {AUTHORS_TABLE_NAME}(id) ON DELETE CASCADE)'
            )
            cursor.executemany(
                f'INSERT INTO `{BOOKS_TABLE_NAME}` '
                '(title, id_author) VALUES (?, ?)',
                [(item['title'], item['id']) for item in initial_records]
            )


def _get_book_obj_from_row(row: Tuple) -> Book:
    return Book(id=row[0], title=row[1], author=row[2])


def _get_id_author_or_add_author_if_not_exist(c: sqlite3.Cursor, name: str) -> int:
    c.execute(
        f"""
            SELECT id FROM {AUTHORS_TABLE_NAME}
            WHERE name = ?
            """, (name,)
    )
    author_id = c.fetchone()
    if author_id:
        return author_id[0]

    c.execute(
        f"""
            INSERT INTO `{AUTHORS_TABLE_NAME}` (name) VALUES (?)
            """, (name,)
    )
    return c.lastrowid


def get_all_books() -> List[Book]:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT books.id, books.title, author.name '
                       f'FROM `{BOOKS_TABLE_NAME}` books '
                       f'INNER JOIN {AUTHORS_TABLE_NAME} author ON books.id_author = author.id')
        all_books = cursor.fetchall()
        return [_get_book_obj_from_row(row) for row in all_books]


def add_book(book: Book) -> Book:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        author_id = _get_id_author_or_add_author_if_not_exist(cursor, book.author)

        cursor.execute(
            f"""
            INSERT INTO `{BOOKS_TABLE_NAME}`
            (title, id_author) VALUES (?, ?)
            """, (book.title, author_id)
        )
        book.id = cursor.lastrowid
        return book


def get_book_by_id(book_id: int) -> Optional[Book]:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT books.id, books.title, author.name '
                       f'FROM `{BOOKS_TABLE_NAME}` books '
                       f'LEFT JOIN {AUTHORS_TABLE_NAME} author ON books.id_author = author.id ' 
                       f'WHERE books.id = ?', (book_id,)
                       )
        book = cursor.fetchone()
        if book:
            return _get_book_obj_from_row(book)


def update_book_by_id(book: Book) -> None:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        author_id = _get_id_author_or_add_author_if_not_exist(cursor, book.author)
        cursor.execute(
            f"""
            UPDATE {BOOKS_TABLE_NAME}
            SET title = ? ,
                id_author = ?
            WHERE id = ?
            """, (book.title, author_id, book.id)
        )
        conn.commit()


def delete_book_by_id(book_id: int) -> None:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            DELETE FROM {BOOKS_TABLE_NAME}
            WHERE id = ?
            """, (book_id,)
        )
        conn.commit()


def get_all_authors() -> List[Author]:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {AUTHORS_TABLE_NAME}')
        all_authors = cursor.fetchall()
        return [Author(id=row[0], name=row[1]) for row in all_authors]


def add_author(author: Author) -> Author:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO `{AUTHORS_TABLE_NAME}`
            (name) VALUES (?)
            """, (author.name,)
        )
        author.id = cursor.lastrowid
        return author


def delete_author_by_id(author_id: int) -> None:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.executescript(ENABLE_FOREIGN_KEY)
        cursor.execute(
            f"""
            DELETE FROM {AUTHORS_TABLE_NAME}
            WHERE id = ?
            """, (author_id,)
        )
        conn.commit()


def get_author_by_name(author_name: str) -> Optional[Author]:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.execute(
            f'SELECT * FROM `{AUTHORS_TABLE_NAME}` WHERE name = ?', (author_name,)
        )
        author = cursor.fetchone()
        if author:
            return Author(id=author[0], name=author[1])


def get_author_by_id(author_id: int) -> Optional[Author]:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.execute(
            f'SELECT * FROM `{AUTHORS_TABLE_NAME}` WHERE id = ?', (author_id,)
        )
        author = cursor.fetchone()
        if author:
            return Author(id=author[0], name=author[1])


def get_books_by_id_author(author_id: int) -> List[Book]:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM `{BOOKS_TABLE_NAME}` '
                       f'WHERE id_author = ?', (author_id,)
                       )
        books = cursor.fetchall()
        return [_get_book_obj_from_row(row) for row in books]


def is_book_exists(book_title: str, author_name: str) -> bool:
    with sqlite3.connect('table_books.db') as conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT b.title, a.name '
                       f'FROM `{BOOKS_TABLE_NAME}` b '
                       f'JOIN `{AUTHORS_TABLE_NAME}` a ON b.id_author = a.id ' 
                       f'WHERE b.title = ? and a.name = ?', (book_title, author_name)
                       )
        book = cursor.fetchone()
        if book:
            return True
        return False
