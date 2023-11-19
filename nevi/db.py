from datetime import date
from typing import Any, Self, Literal
import contextlib
import json
import os
import pickle
import struct

from berkeleydb import db as bdb

from db_types import Table, ColumnTypeClass, Attribute
from db_messages import *

class DB:
    def __init__(self, filename: str | None) -> None:
        self.filename = filename

    def open(self) -> None:
        self.schema_db = bdb.DB()
        self.schema_db.open(self.filename, dbname='SCHEMA', dbtype=bdb.DB_BTREE, flags=bdb.DB_CREATE)

    def open_table(self, table_name: str) -> contextlib.closing[bdb.DB]:
        table_db = bdb.DB()
        table_db.open(self.filename, dbname=f'ZZ_TABLE_{table_name.lower()}', dbtype=bdb.DB_BTREE, flags=bdb.DB_CREATE)
        return contextlib.closing(table_db)

    def __enter__(self) -> Self:
        self.open()
        return self

    def close(self) -> None:
        self.schema_db.close()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()

    def _get_table(self, table_name: str) -> Table:
        tbl_get = self.schema_db.get(table_name.lower().encode())
        if tbl_get is None:
            raise bdb.DBNotFoundError
        tbl = pickle.loads(tbl_get)
        assert isinstance(tbl, Table)
        return tbl

    def _get_refcnt(self, table_name: str) -> int:
        ref_name = f'ZZ_REFCOUNT_{table_name.lower()}'
        get = self.schema_db.get(ref_name.encode(), default=struct.pack('<i', 0))

        refcnt = struct.unpack('<i', get)[0]
        assert isinstance(refcnt, int)
        return refcnt

    def _add_refcnt(self, table_name: str, delta: int) -> None:
        refcnt = self._get_refcnt(table_name)
        refcnt += delta

        ref_name = f'ZZ_REFCOUNT_{table_name.lower()}'
        if refcnt != 0:
            self.schema_db.put(ref_name.encode(), struct.pack('<i', refcnt))
        else:
            self.schema_db.delete(ref_name.encode())

    def create_table(self, tbl: Table) -> DBMessage:
        tbl.validate()

        # validate fkeys
        for fkey in tbl.foreign_keys:
            col_names = set(col.lower() for col in fkey.columns)

            try:
                refd_table = self._get_table(fkey.ref_table)
            except bdb.DBNotFoundError:
                raise ReferenceTableExistenceError

            # ensure fkeys are defined
            refd_col_names = set(col.name.lower() for col in refd_table.columns)
            if col_names - refd_col_names:
                raise ReferenceColumnExistenceError

            # ensure fkeys are pkeys
            if len(refd_table.primary_keys) == 0:
                raise ReferenceNonPrimaryKeyError
            refd_pkey_names = set(col.lower() for col in refd_table.primary_keys[0])
            if col_names != refd_pkey_names:
                raise ReferenceNonPrimaryKeyError

            # ensure fkey types match
            for col_name in col_names:
                refr_col = tbl.find_column(col_name)
                refd_col = refd_table.find_column(col_name)
                if refr_col.col_type != refd_col.col_type:
                    raise ReferenceTypeError

        try:
            self.schema_db.put(tbl.name.lower().encode(), pickle.dumps(tbl), flags=bdb.DB_NOOVERWRITE)
        except bdb.DBKeyExistError:
            raise TableExistenceError

        # increment reference counts
        for fkey in tbl.foreign_keys:
            self._add_refcnt(fkey.ref_table, 1)

        return CreateTableSuccess(tbl.name)

    def drop_table(self, table_name: str) -> DBMessage:
        try:
            tbl = self._get_table(table_name)
        except bdb.DBNotFoundError:
            raise NoSuchTableError

        # ensure references to table
        refcnt = self._get_refcnt(table_name)
        if refcnt != 0:
            raise DropReferencedTableError(table_name)

        self.schema_db.delete(table_name.lower().encode())

        # decrement reference counts
        for fkey in tbl.foreign_keys:
            self._add_refcnt(fkey.ref_table, -1)

        return DropSuccess(table_name)

    def explain_table(self, table_name: str) -> DBMessage:
        try:
            tbl = self._get_table(table_name)
        except bdb.DBNotFoundError:
            raise NoSuchTableError

        pkey_names = set(col.lower() for pkey in tbl.primary_keys for col in pkey)
        fkey_names = set(col.lower() for fkey in tbl.foreign_keys for col in fkey.columns)

        message = f'table_name [{table_name}]\n'
        message += '\t'.join(['column_name', 'type', 'null', 'key'])

        for col in tbl.columns:
            cons = []
            if col.name.lower() in pkey_names:
                cons.append('PRI')
            if col.name.lower() in fkey_names:
                cons.append('FOR')
            message += f'\n{col}\t{"/".join(cons)}'

        line = '-----------------------------------------------------------------'
        return DBMessage('\n'.join([line, message, line]))

    def show_tables(self) -> DBMessage:
        tables = []
        with contextlib.closing(self.schema_db.cursor()) as c:
            while row := c.next():
                key = row[0].decode()
                if key.startswith('ZZ_'):
                    continue
                tables.append(key)
        message = '\n'.join(tables)

        line = '------------------------'
        return DBMessage('\n'.join([line, message, line]))

    # for a given record, return its pkey
    # record must be a superset of the pkey columns
    def _record_pkey(self, tbl: Table, record: list[tuple[str, Attribute]]) -> bytes:
        if len(tbl.primary_keys) == 0:
            # no primary key, return random
            # TODO(nevi): consider
            return os.urandom(16)

        record_map = {col.lower(): val for col, val in record}
        pkey_record = []
        for pkey_col in tbl.primary_keys[0]:
            pkey_record.append(record_map[pkey_col.lower()])
        return json.dumps(pkey_record).encode()

    # search the given table by its pkey
    def _find_by_pkey(self, tbl: Table, pkey: bytes) -> bool:
        with self.open_table(tbl.name) as table_db:
            return table_db.get(pkey) is not None

    def insert_values(self, table_name: str, columns: list[str] | None, values: list[Attribute]) -> DBMessage:
        try:
            tbl = self._get_table(table_name)
        except bdb.DBNotFoundError:
            raise NoSuchTableError

        if columns is None:
            # if columns are not specified, same as specifying all columns
            columns = [col.name for col in tbl.columns]

        # ensure tuple counts match
        if len(columns) != len(values):
            raise InsertTypeMismatchError

        # ensure no duplicates in column
        ins_col_names = set(col.lower() for col in columns)
        if len(ins_col_names) != len(columns):
            raise InsertTypeMismatchError

        # ensure columns exist
        tbl_col_names = set(col.name.lower() for col in tbl.columns)
        if diff := ins_col_names - tbl_col_names:
            raise InsertColumnExistenceError(diff.pop())

        # ensure unspecified columns are nullable
        for col in tbl_col_names - ins_col_names:
            column = tbl.find_column(col)
            if not column.nullable:
                raise InsertColumnNonNullableError(column.name)

        # ensure types match
        #insert_columns = {col.lower(): tbl.find_column(col) for col in columns}
        for col, val in zip(columns, values):
            column = tbl.find_column(col.lower())
            #column = insert_columns[col.lower()]

            if val is None:
                if not column.nullable:
                    raise InsertColumnNonNullableError(column.name)
                continue

            match column.col_type.type_class:
                case ColumnTypeClass.INT:
                    if isinstance(val, int):
                        continue
                case ColumnTypeClass.CHAR:
                    if isinstance(val, str):
                        continue
                case ColumnTypeClass.DATE:
                    if isinstance(val, date):
                        continue
            raise InsertTypeMismatchError

        # construct record
        record: list[tuple[str, Attribute]] = []
        for column in tbl.columns:
            if column.name.lower() not in ins_col_names:
                record.append((column.name.lower(), None))
                continue
            for k, v in zip(columns, values):
                # truncate chars
                if column.col_type.type_class == ColumnTypeClass.CHAR:
                    assert isinstance(v, str)
                    v = v[:column.col_type.type_attr]

                if k.lower() == column.name.lower():
                    record.append((column.name.lower(), v))

        # ensure foreign key constraints
        for fkey in tbl.foreign_keys:
            ref_tbl = self._get_table(fkey.ref_table)
            ref_pkey = self._record_pkey(ref_tbl, record)
            if not self._find_by_pkey(ref_tbl, ref_pkey):
                raise InsertReferentialIntegrityError

        record_pkey = self._record_pkey(tbl, record)
        record_values = json.dumps([v for _, v in record]).encode()

        with self.open_table(table_name) as table_db:
            try:
                table_db.put(record_pkey, record_values, flags=bdb.DB_NOOVERWRITE)
            except bdb.DBKeyExistError:
                # I am willing to bet my grades on randomness
                raise InsertDuplicatePrimaryKeyError

        return InsertResult()
