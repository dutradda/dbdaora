class DBDaoraError(Exception):
    ...


class EntityNotFoundError(DBDaoraError):
    ...


class InvalidQueryError(DBDaoraError):
    ...


class InvalidHashAttribute(DBDaoraError):
    ...


class InvalidEntityAnnotationError(DBDaoraError):
    ...


class RequiredKeyAttributeError(DBDaoraError):
    ...


class InvalidKeyAttributeError(DBDaoraError):
    ...


class InvalidEntityTypeError(DBDaoraError):
    ...


class RequiredClassAttributeError(DBDaoraError):
    ...
