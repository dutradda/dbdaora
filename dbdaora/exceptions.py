class DBDaoraError(Exception):
    ...


class EntityNotFoundError(DBDaoraError):
    ...


class InvalidQueryError(DBDaoraError):
    ...


class InvalidHashAttribute(DBDaoraError):
    ...
