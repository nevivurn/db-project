from __future__ import annotations
from abc import ABC, abstractmethod
from binascii import hexlify
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from itertools import product
from typing import Union, no_type_check
import json
import os
import pickle
import struct

from berkeleydb import db as bdb

from db_messages import *

Attr = Union[int, str, date, None]

class CClass(Enum):
    INT = 1
    CHAR = 2
    DATE = 3

@dataclass
class CType:
    cclass: CClass
    cparam: int | None = None
    nullable: bool = True

    # check if the given attr can be set for this type
    def check_type(self, attr: Attr) -> bool:
        assert(attr is not None)
        if isinstance(attr, int):
            return self.cclass == CClass.INT
        elif isinstance(attr, str):
            return self.cclass == CClass.CHAR
        elif isinstance(attr, date):
            return self.cclass == CClass.DATE

    def check_null(self, attr: Attr) -> bool:
        return (attr is not None) or self.nullable

    # check if this type could match a foreign key for self
    def match_fkey(self, other: CType) -> bool:
        # same as == , but ignore nullable
        if self.cclass != other.cclass:
            return False
        if (self.cparam is None) != (other.cparam is None):
            return False
        return True

@dataclass
class Column:
    cname: str
    ctype: CType

@dataclass
class FKey:
    ref_tname: str
    cname_map: dict[str, str]

    def ref_record(self, ref_table: Table, record: Record) -> Record | None:
        assert record.cnames is not None

        ref_cnames: list[str] = []
        ref_vals: list[Attr] = []
        for cname, val in zip(record.cnames, record.vals):
            if cname not in self.cname_map:
                continue
            ref_cname = self.cname_map[cname]
            if ref_cname not in ref_table.pkeys:
                continue
            ref_cnames.append(ref_cname)
            ref_vals.append(val)

        # if the entire fkey is NULL, allow
        if all(val is None for val in ref_vals):
            return None
        return Record(ref_vals, ref_cnames)

@dataclass
class Table:
    tname: str
    cols: list[Column]
    pkeys: set[str] = field(default_factory=set)
    fkeys: list[FKey] = field(default_factory=list)

    def cnames(self) -> set[str]:
        return {col.cname for col in self.cols}

    def find_col(self, cname: str) -> Column:
        for col in self.cols:
            if col.cname == cname:
                return col
        raise KeyError

@dataclass
class Record:
    vals: list[Attr]
    cnames: list[str] | None = None

    # convert to given table's pkey, cnames must be set
    def pkey(self, table: Table) -> bytes:
        # if no pkey, ensure unique value
        if len(table.pkeys) == 0:
            return hexlify(os.urandom(16))

        assert(self.cnames is not None)
        pkey: list[Attr | list[int]] = []
        for cname, val in zip(self.cnames, self.vals):
            if cname not in table.pkeys:
                continue
            if isinstance(val, date):
                pkey.append([val.year, val.month, val.day])
            else:
                pkey.append(val)
        return json.dumps(pkey).encode()

@dataclass
class Ident:
    tname: str | None
    cname: str

    def match(self, other: Ident) -> bool:
        if (self.tname is not None) and (other.tname is not None):
            if other.tname != self.tname:
                return False
        return self.cname == other.cname

Operand = Union[Ident, int, str, date]

class CompOp(Enum):
    LESSTHAN = 1
    LESSEQUAL = 2
    GREATERTHAN = 3
    GREATEREQUAL = 4
    EQUAL = 5
    NOTEQUAL = 6

@dataclass
class View:
    idents: list[Ident]
    cols: list[Column]

    def find(self, ident: Ident) -> Column:
        # no need to match table if not set
        match_table = ident.tname is None
        match_ident = False
        match: Column | None = None

        for view_ident, col in zip(self.idents, self.cols):
            if ident.tname is not None:
                if view_ident.tname != ident.tname:
                    continue
                match_table = True

            if view_ident.cname != ident.cname:
                continue

            if match_ident:
                raise WhereAmbiguousReference
            match_ident = True
            match = col

        if not match_table:
            raise WhereTableNotSpecified
        if not match_ident:
            raise WhereColumnNotExist
        assert match is not None
        return match

@dataclass
class QualRecord:
    vals: list[Attr]
    idents: list[Ident]

    def unqual(self) -> Record:
        return Record(self.vals, [ident.cname for ident in self.idents])

    def find(self, ident: Ident) -> Attr:
        for view_ident, val in zip(self.idents, self.vals):
            if not view_ident.match(ident):
                continue
            return val
        raise Exception('unreachable code')

class Where(ABC):
    @abstractmethod
    def validate(self, view: View) -> None: ...
    @abstractmethod
    def evaluate(self, record: QualRecord) -> bool: ...

class WhereNOP(Where):
    def validate(self, view: View) -> None:
        pass
    def evaluate(self, record: QualRecord) -> bool:
        return True

class WhereAnd(Where):
    def __init__(self, *wheres: Where) -> None:
        self.wheres = wheres
    def validate(self, view: View) -> None:
        for wh in self.wheres:
            wh.validate(view)
    def evaluate(self, record: QualRecord) -> bool:
        return all(wh.evaluate(record) for wh in self.wheres)

class WhereOr(Where):
    def __init__(self, *wheres: Where) -> None:
        self.wheres = wheres
    def validate(self, view: View) -> None:
        for wh in self.wheres:
            wh.validate(view)
    def evaluate(self, record: QualRecord) -> bool:
        return any(wh.evaluate(record) for wh in self.wheres)

class WhereNot(Where):
    def __init__(self, where: Where) -> None:
        self.where = where
    def validate(self, view: View) -> None:
        self.where.validate(view)
    def evaluate(self, record: QualRecord) -> bool:
        return self.where.evaluate(record)

class WhereNull(Where):
    def __init__(self, ident: Ident) -> None:
        self.ident = ident
    def validate(self, view: View) -> None:
        view.find(self.ident)
    def evaluate(self, record: QualRecord) -> bool:
        attr = record.find(self.ident)
        return attr is None

class WhereComp(Where):
    def __init__(self, left: Operand, right: Operand, oper: CompOp) -> None:
        self.left = left
        self.right = right
        self.oper = oper

    def _get_type(self, view: View, op: Operand) -> type:
        if not isinstance(op, Ident):
            return type(op)
        cclass = view.find(op).ctype.cclass
        if cclass == CClass.INT:
            return int
        elif cclass == CClass.CHAR:
            return str
        elif cclass == CClass.DATE:
            return date

    def _get_value(self, record: QualRecord, op: Operand) -> Attr:
        if not isinstance(op, Ident):
            return op
        return record.find(op)

    def validate(self, view: View) -> None:
        left_type = self._get_type(view, self.left)
        right_type = self._get_type(view, self.right)
        if left_type != right_type:
            raise WhereIncomparableError
        elif self.oper == CompOp.LESSTHAN:
            if left_type == int or left_type == date:
                return
        elif self.oper == CompOp.LESSEQUAL:
            if left_type == int or left_type == date:
                return
        elif self.oper == CompOp.GREATERTHAN:
            if left_type == int or left_type == date:
                return
        elif self.oper == CompOp.GREATEREQUAL:
            if left_type == int or left_type == date:
                return
        elif self.oper == CompOp.EQUAL or self.oper == CompOp.NOTEQUAL:
            return
        raise WhereIncomparableError

    @no_type_check
    def evaluate(self, record: QualRecord) -> bool:
        left_val = self._get_value(record, self.left)
        right_val = self._get_value(record, self.right)
        if (left_val is None) or (right_val is None):
            return False
        if self.oper == CompOp.LESSTHAN:
            return left_val < right_val
        elif self.oper == CompOp.LESSEQUAL:
            return left_val <= right_val
        elif self.oper == CompOp.GREATERTHAN:
            return left_val > right_val
        elif self.oper == CompOp.GREATEREQUAL:
            return left_val >= right_val
        elif self.oper == CompOp.EQUAL:
            return left_val == right_val
        elif self.oper == CompOp.NOTEQUAL:
            return left_val != right_val

@dataclass
class TableView:
    tnames: list[str]
    alt_tnames: list[str]

@dataclass
class ColumnView:
    idents: list[Ident]
    alt_cnames: list[str]

    def project(self, record: QualRecord) -> QualRecord:
        p_record = QualRecord([], [])
        for c_ident, c_cname in zip(self.idents, self.alt_cnames):
            for r_ident, r_val in zip(record.idents, record.vals):
                if r_ident.match(c_ident):
                    break
            p_record.vals.append(r_val)
            p_record.idents.append(Ident(None, c_cname))
        return p_record

class DB:
    def __init__(self, filename: str) -> None:
        self.filename = filename

        schema_db = bdb.DB()
        schema_db.open(filename, dbname='SCHEMA', dbtype=bdb.DB_HASH, flags=bdb.DB_CREATE)
        self.schema_db = schema_db

    def close(self) -> None:
        self.schema_db.close()

    def _get_table(self, tname: str) -> Table:
        table_raw = self.schema_db.get(f"ZZ_table_{tname}".encode())
        if table_raw is None:
            raise KeyError
        table = pickle.loads(table_raw)
        assert isinstance(table, Table)
        return table

    def _put_table(self, table: Table) -> None:
        table_raw = pickle.dumps(table)
        self.schema_db.put(f"ZZ_table_{table.tname}".encode(), table_raw, flags=bdb.DB_NOOVERWRITE)

    def _open_table(self, tname: str) -> closing[bdb.DB]:
        tdb = bdb.DB()
        tdb.open(self.filename, dbname=f"ZZ_table_{tname}", dbtype=bdb.DB_HASH, flags=bdb.DB_CREATE)
        return closing(tdb)

    def _put_record(self, table: Table, record: Record) -> None:
        with self._open_table(table.tname) as tdb:
            pkey = record.pkey(table)
            vals_raw = json.dumps(record.vals).encode()
            tdb.put(pkey, vals_raw, flags=bdb.DB_NOOVERWRITE)

    def _decode_record(self, table: Table, raw: bytes) -> QualRecord:
        record_dec = json.loads(raw)
        vals: list[Attr] = []
        for elem in record_dec:
            if elem is None:
                vals.append(None)
                continue
            if isinstance(elem, int):
                vals.append(elem)
            elif isinstance(elem, str):
                vals.append(elem)
            elif isinstance(elem, list):
                vals.append(date(*elem))
        return QualRecord(
            vals=vals,
            idents=[Ident(table.tname, col.cname) for col in table.cols],
        )

    def _get_record(self, table: Table, pkey: bytes) -> QualRecord:
        with self._open_table(table.tname) as tdb:
            record_raw = tdb.get(pkey)
            if record_raw is None:
                raise KeyError
            return self._decode_record(table, record_raw)

    def _delete_records(self, table: Table, where: Where) -> int:
        count = 0
        fkey_failed = False
        with self._open_table(table.tname) as tdb:
            # first pass, count and check fkeys
            with closing(tdb.cursor()) as c:
                while row := c.next():
                    record = self._decode_record(table, row[1])
                    if not where.evaluate(record):
                        continue
                    count += 1

                    pkey = record.unqual().pkey(table)
                    refcnt = self._get_refcnt_record(table.tname, pkey)
                    if refcnt > 0:
                        fkey_failed = True

            if fkey_failed:
                raise DeleteReferentialIntegrityPassed(count)

            # second pass, delete and decrement
            with closing(tdb.cursor()) as c:
                while row := c.next():
                    record = self._decode_record(table, row[1])
                    if not where.evaluate(record):
                        continue

                    c.delete()

                    # decrement fkey refcnt
                    for fkey in table.fkeys:
                        ref_table = self._get_table(fkey.ref_tname)
                        ref_record = fkey.ref_record(ref_table, record.unqual())
                        if ref_record is None:
                            continue
                        ref_pkey = ref_record.pkey(ref_table)
                        self._add_refcnt_record(ref_table.tname, ref_pkey, -1)
        return count

    def _generate_records(self, table: Table, alt_tname: str) -> Iterable[QualRecord]:
        with self._open_table(table.tname) as tdb:
            with closing(tdb.cursor()) as c:
                while row := c.next():
                    record = self._decode_record(table, row[1])
                    record = QualRecord(
                        record.vals,
                        [Ident(alt_tname, ident.cname) for ident in record.idents],
                    )
                    yield record

    def _get_refcnt_table(self, tname: str) -> int:
        refcnt_raw = self.schema_db.get(f"ZZ_refcnt_table_{tname}".encode())
        if refcnt_raw is None:
            return 0
        refcnt = struct.unpack('<i', refcnt_raw)[0]
        assert isinstance(refcnt, int)
        return refcnt

    def _add_refcnt_table(self, tname: str, delta: int) -> None:
        refcnt = self._get_refcnt_table(tname)
        refcnt += delta
        self.schema_db.put(f"ZZ_refcnt_table_{tname}".encode(), struct.pack('<i', refcnt))

    def _get_refcnt_record(self, tname: str, pkey: bytes) -> int:
        name = json.dumps([tname, pkey.decode()]).encode()
        refcnt_raw = self.schema_db.get(b'ZZ_refcnt_record_' + name)
        if refcnt_raw is None:
            return 0
        refcnt = struct.unpack('<i', refcnt_raw)[0]
        assert isinstance(refcnt, int)
        return refcnt

    def _add_refcnt_record(self, tname: str, pkey: bytes, delta: int) -> None:
        name = json.dumps([tname, pkey.decode()]).encode()
        refcnt = self._get_refcnt_record(tname, pkey)
        refcnt += delta
        self.schema_db.put(b'ZZ_refcnt_record_' + name, struct.pack('<i', refcnt))

    def _render_select(self, headers: list[str], rows: list[list[Attr]]) -> str:
        msgs: list[list[str]] = []
        for row in rows:
            msg: list[str] = []
            for val in row:
                if val is None:
                    msg.append('NULL')
                elif isinstance(val, date):
                    msg.append(val.isoformat())
                else:
                    msg.append(str(val))
            msgs.append(msg)

        widths: list[int] = []
        for head in headers:
            widths.append(len(head))
        for msg in msgs:
            for i, elem in enumerate(msg):
                widths[i] = max(widths[i], len(elem))

        message = ''
        sep = '|'
        lsep = '+'

        message += lsep + sep.join('-'*(w+2) for w in widths) + lsep
        message += '\n'
        message += sep + sep.join(' ' + h.ljust(w+1) for h, w in zip(headers, widths)) + sep
        message += '\n'
        message += lsep + sep.join('-'*(w+2) for w in widths) + lsep

        for msg in msgs:
            message += '\n'
            message += sep + sep.join(' ' + e.ljust(w+1) for e, w in zip(msg, widths)) + sep
        message += '\n'
        message += lsep + sep.join('-'*(w+2) for w in widths) + lsep

        return message

    def create_table(self, table: Table) -> DBMessage:
        for fkey in table.fkeys:
            # table ref
            try:
                ref_table = self._get_table(fkey.ref_tname)
            except KeyError:
                raise ReferenceTableExistenceError

            # col ref
            if set(fkey.cname_map.values()) >= ref_table.cnames():
                raise ReferenceColumnExistenceError

            # col type
            for from_cname, to_cname in fkey.cname_map.items():
                from_ctype = table.find_col(from_cname).ctype
                to_ctype = ref_table.find_col(to_cname).ctype

                if not from_ctype.match_fkey(to_ctype):
                    raise ReferenceTypeError

            # only entire pkeys
            if set(fkey.cname_map.values()) != ref_table.pkeys:
                raise ReferenceNonPrimaryKeyError

        try:
            self._put_table(table)
        except bdb.DBKeyExistError:
            raise TableExistenceError

        for fkey in table.fkeys:
            self._add_refcnt_table(fkey.ref_tname, 1)

        return CreateTableSuccess(table.tname)

    #def create_table():
    #    # check fkeys
    #    # - get table (exists)
    #    #   ref.cnames >= fkey.cnames
    #    #   ref.col.ctype == fkey.ctype
    #    #   ref.pkey == fkey.cnames
    #    # for fkeys, ref.refcount++

    #    # insert, check if exists

    #def drop_table():
    #    # get table (exists)
    #    # table.refcount == 0
    #    # for fkeys, ref.refcount--
    #    pass

    def insert_values(self, tname: str, record: Record) -> DBMessage:
        try:
            table = self._get_table(tname)
        except KeyError:
            raise NoSuchTableError

        # cols unspecified
        if record.cnames is None:
            if len(table.cols) != len(record.vals):
                raise InsertTypeMismatchError
            record.cnames = [col.cname for col in table.cols]

        # unspecified cols
        if diff := set(record.cnames) - table.cnames():
            raise InsertColumnExistenceError(diff.pop())

        # construct record to insert
        full_vals: list[Attr] = []
        for col in table.cols:
            if col.cname in record.cnames:
                full_vals.append(record.vals[record.cnames.index(col.cname)])
            else:
                full_vals.append(None)

            # check types
            if not col.ctype.check_null(full_vals[-1]):
                raise InsertColumnNonNullableError(col.cname)
            if full_vals[-1] is not None and not col.ctype.check_type(full_vals[-1]):
                raise InsertTypeMismatchError
            # truncate strings
            if isinstance(full_vals[-1], str):
                full_vals[-1] = full_vals[-1][:col.ctype.cparam]
        full_record = Record(full_vals, [col.cname for col in table.cols])

        # check fkeys
        fkey_rows: list[tuple[Table, bytes]] = []
        for fkey in table.fkeys:
            ref_table = self._get_table(fkey.ref_tname)
            ref_record = fkey.ref_record(ref_table, full_record)
            if ref_record is None:
                continue

            ref_pkey = ref_record.pkey(ref_table)
            try:
                self._get_record(ref_table, ref_pkey)
            except KeyError:
                raise InsertReferentialIntegrityError
            fkey_rows.append((ref_table, ref_pkey))

        try:
            self._put_record(table, full_record)
        except bdb.DBKeyExistError:
            raise InsertDuplicatePrimaryKeyError

        for ref_table, ref_pkey in fkey_rows:
            self._add_refcnt_record(ref_table.tname, ref_pkey, 1)

        return InsertResult()

    #def insert_values():
    #    # get table
    #    # self.cnames >= ins.cnames
    #    # create record, type check
    #    #   - zip(record, column).typecheck
    #    # for fkey:
    #    # - get table
    #    #   ref.get_by_pkey()
    #    #   record.refcount++
    #    # insert, check if exists
    #    pass

    def delete_values(self, tname: str, where: Where | None) -> DBMessage:
        try:
            table = self._get_table(tname)
        except KeyError:
            raise NoSuchTableError

        view = View(
            idents=[Ident(tname, col.cname) for col in table.cols],
            cols=table.cols,
        )

        if where is None:
            where = WhereNOP()
        where.validate(view)

        counter = self._delete_records(table, where)
        return DeleteResult(counter)

    #def delete_values():
    #    # get table (exists)
    #    # check where validity
    #    #   where.validate(col)
    #    # for each value:
    #    #   where.eval(value)
    #    #   refcnt?
    #    pass

    def select_values(self, cview: ColumnView | None, tview: TableView, where: Where | None) -> DBMessage:
        # first pass for validation
        view = View([], [])
        tables: list[Table] = []
        for tname, alt_tname in zip(tview.tnames, tview.alt_tnames):
            try:
                table = self._get_table(tname)
            except KeyError:
                raise SelectTableExistenceError(tname)

            tables.append(table)
            for col in table.cols:
                view.idents.append(Ident(alt_tname, col.cname))
                view.cols.append(col)

        if cview is None:
            # if *, all cnames must be unique
            cnames: set[str] = set()
            for col in view.cols:
                if col.cname in cnames:
                    raise SelectColumnResolveError(col.cname)
                cnames.add(col.cname)
        else:
            # otherwise, every selected column must be unique
            for ident in cview.idents:
                try:
                    view.find(ident)
                except DBError:
                    raise SelectColumnResolveError(ident.cname)

        # ensure where is valid
        if where is None:
            where = WhereNOP()
        where.validate(view)

        # second pass for actual fetch
        gens: list[Iterable[QualRecord]] = []
        for table, alt_tname in zip(tables, tview.alt_tnames):
            gens.append(self._generate_records(table, alt_tname))

        # prepare header
        if cview is None:
            headers = [col.cname for col in view.cols]
        else:
            headers = cview.alt_cnames

        # prepare records
        rows: list[list[Attr]] = []
        for records in product(*gens):
            # generate full record by union
            full_record = QualRecord([], [])
            for record in records:
                full_record.vals += record.vals
                full_record.idents += record.idents
            if not where.evaluate(full_record):
                continue

            if cview is not None:
                full_record = cview.project(full_record)

            rows.append(full_record.vals)

        return DBMessage(self._render_select(headers, rows))

    #def select_values():
    #    # get tables (exists)
    #    # check where validity
    #    #   where.validate(col)
    #    # check project validity
    #    # for each value*:
    #    #   where.eval(value)
    #    # project
    #    pass
