from typing import Dict

from marshmallow import (
    Schema, fields, validates,
    ValidationError, post_load, validates_schema
)

from models import (
    get_author_by_name,
    is_book_exists, Book, Author
)


class BookSchema(Schema):

    id = fields.Int(dump_only=True)
    title = fields.Str(required=True)
    author = fields.Str(required=True)

    # проверка существования книги (по названию и автору).
    # используется при добавлении новой книги и обновлении существующей
    @validates_schema()
    def validate_exists_book(self, data, **kwargs):
        if is_book_exists(data['title'], data['author']):
            errors = dict()
            errors['error'] = 'Book with title "{title}" and author "{author}" already exists, ' \
                              'please use a different title or author.'.format(title=data['title'],
                                                                               author=data['author'])
            raise ValidationError(errors)

    @post_load
    def create_book(self, data: Dict, **kwargs) -> Book:
        return Book(**data)


class AuthorSchema(Schema):

    id = fields.Int(dump_only=True)
    name = fields.Str(required=True)

    # проверка существования автора (по имени)
    # используется при добавлении нового автора
    @validates('name')
    def validate_name(self, name: str) -> None:
        if get_author_by_name(name) is not None:
            raise ValidationError(
                'Author with name "{name}" already exists, '
                'please use a different name.'.format(name=name)
            )

    @post_load
    def create_author(self, data: Dict, **kwargs) -> Author:
        return Author(**data)
