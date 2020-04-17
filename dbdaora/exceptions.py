class DBDaoraError(Exception):
    ...


class EntityNotFoundError(DBDaoraError):
    ...


class QueryClassRequiredError(DBDaoraError):
    ...


class InvalidQueryError(DBDaoraError):
    ...
