"""Provider-neutral persistence errors."""


class StorageError(RuntimeError):
    pass


class EntityNotFound(StorageError):
    pass


class IllegalTransition(StorageError):
    pass


class LeaseLost(StorageError):
    pass
