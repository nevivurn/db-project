class DBError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
    def __str__(self) -> str:
        return self.message

class DBMessage:
    def __init__(self, message: str) -> None:
        self.message = message
    def __str__(self) -> str:
        return self.message

class SyntaxError(DBError):
    def __init__(self) -> None:
        super().__init__('Syntax error')

class DuplicateColumnDefError(DBError):
    def __init__(self) -> None:
        super().__init__('Create table has failed: column definition is duplicated')

class DuplicatePrimaryKeyDefError(DBError):
    def __init__(self) -> None:
        super().__init__('Create table has failed: primary key definition is duplicated')

class ReferenceTypeError(DBError):
    def __init__(self) -> None:
        super().__init__('Create table has failed: foreign key references wrong type')

class ReferenceNonPrimaryKeyError(DBError):
    def __init__(self) -> None:
        super().__init__('Create table has failed: foreign key references non primary key column')

class ReferenceColumnExistenceError(DBError):
    def __init__(self) -> None:
        super().__init__('Create table has failed: foreign key references non existing column')

class ReferenceTableExistenceError(DBError):
    def __init__(self) -> None:
        super().__init__('Create table has failed: foreign key references non existing table')

class NonExistingColumnDefError(DBError):
    def __init__(self, col_name: str) -> None:
        super().__init__(f"Create table has failed: '{col_name}' does not exist in column definition")

class TableExistenceError(DBError):
    def __init__(self) -> None:
        super().__init__('Create table has failed: table with the same name already exists')

class CharLengthError(DBError):
    def __init__(self) -> None:
        super().__init__('Char length should be over 0')

class NoSuchTableError(DBError):
    def __init__(self) -> None:
        super().__init__('No such table')

class DropReferencedTableError(DBError):
    def __init__(self, table_name: str) -> None:
        super().__init__(f"Drop table has failed: '{table_name}' is referenced by other table")

class CreateTableSuccess(DBMessage):
    def __init__(self, table_name: str) -> None:
        super().__init__(f"'{table_name}' table is created")

class DropSuccess(DBMessage):
    def __init__(self, table_name: str) -> None:
        super().__init__(f"'{table_name}' table is dropped")

class InsertResult(DBMessage):
    def __init__(self) -> None:
        super().__init__(f'1 row inserted')

class InsertTypeMismatchError(DBError):
    def __init__(self) -> None:
        super().__init__(f'Insertion has failed: Types are not matched')

class InsertColumnExistenceError(DBError):
    def __init__(self, col_name: str) -> None:
        super().__init__(f"Insertion has failed: '{col_name}' does not exist")

class InsertColumnNonNullableError(DBError):
    def __init__(self, col_name: str) -> None:
        super().__init__(f"Insertion has failed: '{col_name}' is not nullable")

class DeleteResult(DBMessage):
    def __init__(self, count: int) -> None:
        super().__init__(f"'{count}' row(s) deleted")

class SelectTableExistenceError(DBError):
    def __init__(self, table_name: str) -> None:
        super().__init__(f"Selection has failed: '{table_name}' does not exist")

class SelectColumnResolveError(DBError):
    def __init__(self, col_name: str) -> None:
        super().__init__(f"Selection has failed: fail to resolve '{col_name}'")

class WhereIncomparableError(DBError):
    def __init__(self) -> None:
        super().__init__(f'Where clause trying to compare incomparable values')

class WhereTableNotSpecified(DBError):
    def __init__(self) -> None:
        super().__init__(f'Where clause trying to reference tables which are not specified')

class WhereColumnNotExist(DBError):
    def __init__(self) -> None:
        super().__init__(f'Where clause trying to reference non existing column')

class WhereAmbiguousReference(DBError):
    def __init__(self) -> None:
        super().__init__(f'Where clause contains ambiguous reference')

class InsertDuplicatePrimaryKeyError(DBError):
    def __init__(self) -> None:
        super().__init__(f'Insertion has failed: Primary key duplication')

class InsertReferentialIntegrityError(DBError):
    def __init__(self) -> None:
        super().__init__(f'Insertion has failed: Referential integrity violation')

class DeleteReferentialIntegrityPassed(DBError):
    def __init__(self, count: int) -> None:
        super().__init__(f"'{count}' row(s) are not deleted due to referential integrity")
