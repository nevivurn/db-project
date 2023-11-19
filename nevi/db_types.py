from datetime import date
from dataclasses import dataclass, field
from enum import Enum

from db_messages import *

Attribute = int | str | date | None

class ColumnTypeClass(Enum):
    INT = 1
    CHAR = 2
    DATE = 3

    def __str__(self) -> str:
        match self:
            case ColumnTypeClass.INT:
                return 'int'
            case ColumnTypeClass.CHAR:
                return 'char'
            case ColumnTypeClass.DATE:
                return 'date'

@dataclass
class ColumnType:
    type_class: ColumnTypeClass
    type_attr: int | None = None # for char

    # validate type attributes
    def validate(self) -> None:
        if self.type_class != ColumnTypeClass.CHAR:
            if self.type_attr is not None:
                raise SyntaxError
        else:
            if self.type_attr is None:
                raise SyntaxError
            if self.type_attr <= 0:
                raise CharLengthError

    def __str__(self) -> str:
        s = str(self.type_class)
        if self.type_attr is not None:
            s += f'({self.type_attr})'
        return s

@dataclass
class Column:
    name: str
    col_type: ColumnType
    nullable: bool = True

    # validate column, in isolation
    def validate(self) -> None:
        if self.name == '':
            raise SyntaxError
        self.col_type.validate()

    def __str__(self) -> str:
        return '\t'.join([
            self.name,
            str(self.col_type),
            'Y' if self.nullable else 'N',
        ])

@dataclass
class ForeignKey:
    columns: list[str]
    ref_table: str

    # validate fkey, in isolation
    def validate(self) -> None:
        if self.ref_table == '':
            raise SyntaxError

        if len(self.columns) == 0:
            raise SyntaxError

        # ensure no duplicates in fkey
        col_names = set(col.lower() for col in self.columns)
        if len(self.columns) != len(col_names):
            raise DuplicateColumnDefError

@dataclass
class Table:
    name: str
    columns: list[Column]
    primary_keys: list[list[str]] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)

    # find the given Column, it must already exist
    def find_column(self, col_name: str) -> Column:
        col_name = col_name.lower()
        for col in self.columns:
            if col.name.lower() == col_name:
                return col
        raise KeyError

    # validate table
    def validate(self) -> None:
        if self.name == '':
            raise SyntaxError

        # validate each column
        if self.columns == []:
            raise SyntaxError
        for col in self.columns:
            col.validate()

        # ensure no duplicate columns
        col_names = set(col.name.lower() for col in self.columns)
        if len(col_names) != len(self.columns):
            raise DuplicateColumnDefError

        # validate pkeys
        if len(self.primary_keys) > 1:
            raise DuplicatePrimaryKeyDefError
        if len(self.primary_keys) == 1:
            pkey_names = set(col.lower() for col in self.primary_keys[0])
            # ensure no duplicates in pkey
            if len(self.primary_keys[0]) != len(pkey_names):
                raise DuplicatePrimaryKeyDefError
            # ensure pkeys are defined
            if diff := pkey_names - col_names:
                raise NonExistingColumnDefError(diff.pop())

            # set pkeys as non-null
            for col in self.columns:
                if col.name.lower() in pkey_names:
                    col.nullable = False

        # validate fkeys
        for fkey in self.foreign_keys:
            fkey.validate()

            # fkey cannot reference self
            if fkey.ref_table.lower() == self.name.lower():
                raise ReferenceTableExistenceError

            # ensure fkeys are defined
            fkey_names = set(col.lower() for col in fkey.columns)
            if diff := fkey_names - col_names:
                raise NonExistingColumnDefError(diff.pop())
