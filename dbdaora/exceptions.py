class DBDaoraError(Exception):
    ...


class EntityNotFoundError(DBDaoraError):
    ...


class InvalidQueryError(DBDaoraError):
    ...
