from typing import Tuple, List, Dict

from flask import Flask, request, jsonify, make_response
from flask_restful import Api, Resource, abort
from marshmallow import ValidationError

from models import (
    DATA, get_all_books, init_db,
    add_book, get_book_by_id, update_book_by_id,
    delete_book_by_id, get_all_authors, add_author,
    delete_author_by_id, get_author_by_id, get_books_by_id_author
)
from schemas import BookSchema, AuthorSchema

app = Flask(__name__)
api = Api(app)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


def abort_if_book_doesnt_exist(book_id: int):
    if get_book_by_id(book_id) is None:
        abort(404, error="Book with id={} doesn't exist".format(book_id))


def abort_if_author_doesnt_exist(author_id: int):
    if get_author_by_id(author_id) is None:
        abort(404, error="Author with id={} doesn't exist".format(author_id))


class BookList(Resource):
    # получение списка книг
    def get(self) -> Tuple[List[Dict], int]:
        schema = BookSchema()
        return jsonify({'books': schema.dump(get_all_books(), many=True)})

    # добавление новой книги
    def post(self) -> Tuple[Dict, int]:

        data = request.json
        schema = BookSchema()

        try:
            book = schema.load(data)
        except ValidationError as exc:
            return exc.messages, 400

        book = add_book(book)
        return {'book': schema.dump(book)}, 201


class BookActions(Resource):
    # получение информации по книге
    def get(self, book_id: int):
        abort_if_book_doesnt_exist(book_id)

        schema = BookSchema()
        return {'book': schema.dump(get_book_by_id(book_id))}

    # обновление информации по книге
    def put(self, book_id: int):
        abort_if_book_doesnt_exist(book_id)

        data = request.json
        schema = BookSchema()

        try:
            book = schema.load(data)
        except ValidationError as exc:
            return exc.messages, 400

        book.id = book_id
        update_book_by_id(book)

        return schema.dump(data), 201

    # удаление книги
    def delete(self, book_id: int):
        abort_if_book_doesnt_exist(book_id)

        delete_book_by_id(book_id)

        return {"message": f"Book with id {book_id} is deleted."}, 200


class AuthorList(Resource):
    # получение списка авторов
    def get(self) -> Tuple[List[Dict], int]:
        schema = AuthorSchema()
        return jsonify({'authors': schema.dump(get_all_authors(), many=True)})

    # добавление нового автора
    def post(self) -> Tuple[Dict, int]:

        data = request.json
        schema = AuthorSchema()

        try:
            author = schema.load(data)
        except ValidationError as exc:
            return exc.messages, 400

        author = add_author(author)
        return {"author": schema.dump(author)}, 201


class AuthorActions(Resource):
    # получение информации о всех книгах автора
    def get(self, author_id: int):
        abort_if_author_doesnt_exist(author_id)

        schema = BookSchema(only=("id", "title"))
        return {'books': schema.dump(get_books_by_id_author(author_id), many=True)}

    # удаление автора со всеми его книгами
    def delete(self, author_id: int):
        abort_if_author_doesnt_exist(author_id)

        delete_author_by_id(author_id)

        return {"message": f"Author with id {author_id} is deleted."}, 200


api.add_resource(BookList, '/api/books')  # список всех книг, добавить книгу
api.add_resource(BookActions, '/api/books/<int:book_id>')  # получить инфу по книге, обновить, удалить книгу

api.add_resource(AuthorList, '/api/authors')  # список всех авторов, добавить автора
api.add_resource(AuthorActions, '/api/authors/<int:author_id>')  # все книги автора, удалить автора со всеми его книгами

if __name__ == '__main__':
    init_db(initial_records=DATA)
    app.run(debug=True)
