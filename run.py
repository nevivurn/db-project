from datetime import date
from typing import Any, Union, no_type_check
import sys

from lark import Lark, Transformer, UnexpectedInput, Token, Tree
from lark.exceptions import VisitError

from db_messages import *
import db

Node = Union[Token, Tree]

PROMPT = 'DB_2017-19937>'

def println_prompt(msg: Any) -> None:
    print(PROMPT, msg)
def print_prompt() -> None:
    print(PROMPT, end=' ')

class PrintTransformer(Transformer[Any, Any]):
    def __init__(self, db: db.DB) -> None:
        super().__init__()
        self.db = db

    def create_table_query(self, args: Any) -> None:
        tname = args[2].children[0].lower()

        cols = []

        column_defs = args[3].find_data('column_definition')
        for column_def in column_defs:
            cname = column_def.children[0].children[0].lower()

            data_type = column_def.children[1].children
            data_class = data_type[0].upper()
            if data_class == 'INT':
                ctype = db.CType(cclass=db.CClass.INT)
            elif data_class == 'CHAR':
                ctype = db.CType(
                    cclass=db.CClass.CHAR,
                    cparam=int(data_type[2]),
                )
                if ctype.cparam is None or ctype.cparam < 0:
                    raise SyntaxError
            elif data_class == 'DATE':
                ctype = db.CType(cclass=db.CClass.DATE)

            ctype.nullable = column_def.children[2] is None
            cols.append(db.Column(cname, ctype))

        pkey_defs = args[3].find_data('primary_key_constraint')
        pkeys = set()
        for pkey_def in pkey_defs:
            column_names = pkey_def.children[2].find_data('column_name')
            for column_name in column_names:
                pkeys.add(column_name.children[0].lower())

        fkey_defs = args[3].find_data('referential_constraint')
        fkeys = []
        for fkey_def in fkey_defs:
            ref_tname = fkey_def.children[4].children[0].lower()

            cnames = []
            for col_def in fkey_def.children[2].find_data('column_name'):
                cnames.append(col_def.children[0].lower())
            ref_cnames = []
            for col_def in fkey_def.children[5].find_data('column_name'):
                ref_cnames.append(col_def.children[0].lower())

            cname_map = {c: r for c, r in zip(cnames, ref_cnames)}
            fkeys.append(db.FKey(ref_tname, cname_map))

        # don't do any validation on create, not graded
        table = db.Table(tname, cols, pkeys, fkeys)

        println_prompt(self.db.create_table(table))

    def drop_table_query(self, args: Any) -> None:
        println_prompt("'DROP TABLE' requested")
    def explain_query(self, args: Any) -> None:
        println_prompt("'EXPLAIN' requested")
    def describe_query(self, args: Any) -> None:
        println_prompt("'DESCRIBE' requested")
    def desc_query(self, args: Any) -> None:
        println_prompt("'DESC' requested")
    def show_tables_query(self, args: Any) -> None:
        println_prompt("'SHOW TABLES' requested")

    # pre-handle WHEREs
    def boolean_expr(self, args: list[db.Where]) -> db.Where:
        return db.WhereOr(*args[::2])

    def boolean_term(self, args: list[db.Where]) -> db.Where:
        return db.WhereAnd(*args[::2])

    def boolean_factor(self, args: tuple[Any, db.Where]) -> db.Where:
        if args[0] is not None:
            return db.WhereNot(args[1])
        return args[1]

    def boolean_test(self, args: tuple[db.Where]) -> db.Where:
        return args[0]

    def parenthesized_boolean_expr(self, args: tuple[Any, db.Where]) -> db.Where:
        return args[1]

    def predicate(self, args: tuple[db.Where]) -> db.Where:
        return args[0]

    @no_type_check
    def comparison_predicate(self, args) -> db.Where:
        oper = args[1].children[0].value
        if oper == '<':
            oper = db.CompOp.LESSTHAN
        elif oper == '<=':
            oper = db.CompOp.LESSEQUAL
        elif oper == '>':
            oper = db.CompOp.GREATERTHAN
        elif oper == '>=':
            oper = db.CompOp.GREATERTHANEQUAL
        elif oper == '=':
            oper = db.CompOp.EQUAL
        elif oper == '!=':
            oper = db.CompOp.NOTEQUAL
        else:
            raise SyntaxError
        return db.WhereComp(args[0], args[2], oper)

    @no_type_check
    def null_predicate(self, args) -> db.Where:
        tname = args[0]
        if tname is not None:
            tname = tname.children[0].lower()
        cname = args[1].children[0].lower()

        where = db.WhereNull(db.Ident(tname, cname))
        if args[2].children[1]:
            where = db.WhereNot(where)
        return where

    def comparable_value(self, args: list[Token]) -> db.Attr:
        if args[0].type == 'INT':
            return int(args[0])
        elif args[0].type == 'STR':
            return args[0][1:-1]
        elif args[0].type == 'DATE':
            return date.fromisoformat(args[0])
        else:
            raise SyntaxError

    @no_type_check
    def comp_operand(self, args) -> db.Operand:
        # literal
        if len(args) == 1:
            return args[0]

        # ident
        tname = args[0]
        if tname is not None:
            tname = tname.children[0].lower()
        cname = args[1].children[0].lower()

        return db.Ident(tname, cname)

    @no_type_check
    def select_query(self, args):

        if len(args[1].children) == 0:
            cview = None
        else:
            cview = db.ColumnView([], [])

            select_defs = args[1].find_data('selected_column')
            for select_def in select_defs:
                tname = select_def.children[0]
                if tname is not None:
                    tname = tname.children[0].lower()
                cname = select_def.children[1].children[0].lower()

                alt_cname = select_def.children[3]
                if alt_cname is not None:
                    alt_cname = alt_cname.children[0].lower()
                else:
                    alt_cname = cname

                cview.idents.append(db.Ident(tname, cname))
                cview.alt_cnames.append(alt_cname)

            for cname in cview.alt_cnames:
                if cview.alt_cnames.count(cname) > 1:
                    raise SelectColumnResolveError(cname)

        table_defs = args[2].find_data('referred_table')
        tview = db.TableView([], [])
        for table_def in table_defs:
            tname = table_def.children[0].children[0].lower()
            alt_tname = table_def.children[2]
            if alt_tname is not None:
                alt_tname = alt_tname.children[0].lower()
            else:
                alt_tname = tname

            tview.tnames.append(tname)
            tview.alt_tnames.append(alt_tname)

        if cview is not None:
            c_tnames = set(ident.tname for ident in cview.idents if ident.tname is not None)
            if diff := c_tnames - set(tview.alt_tnames):
                raise SelectTableExistenceError(diff.pop())

        where_def = args[2].children[1]
        if where_def is None:
            where = None
        else:
            where = where_def.children[1]

        print(self.db.select_values(cview, tview, where))

    @no_type_check
    def insert_query(self, args):
        tname = args[2].children[0].lower()
        if args[3] is not None:
            cnames = []
            for col_def in args[3].find_data('column_name'):
                cnames.append(col_def.children[0].lower())
            if len(cnames) != len(set(cnames)):
                raise InsertTypeMismatchError
        else:
            cnames = None

        vals = args[4].children[2:-1]

        if cnames is not None and len(cnames) != len(vals):
            raise InsertTypeMismatchError

        println_prompt(self.db.insert_values(tname, db.Record(vals, cnames)))

    @no_type_check
    def delete_query(self, args):
        tname = args[2].children[0].lower()
        if args[3] is None:
            where = None
        else:
            where = args[3].children[1]
        println_prompt(self.db.delete_values(tname, where))

    def update_query(self, args: Any) -> None:
        println_prompt("'UPDATE' requested")

    def EXIT(self, args: Any) -> None:
        sys.exit(0)
        print('exit')

def run() -> None:
    with open('grammar.lark', 'r') as f:
        sql_parser = Lark(f.read(), start='command', lexer='basic')

    while True:
        query = ''
        print_prompt()
        while not (query and query[-1] == ';'):
            query += input()

        for q in query.split(';')[:-1]:
            try:
                output = sql_parser.parse(q + ';')
            except UnexpectedInput:
                println_prompt('Syntax error')
                break

            ndb = db.DB('_test.db')
            try:
                PrintTransformer(ndb).transform(output)
            except VisitError as e:
                if isinstance(e.orig_exc, DBError):
                    println_prompt(e.orig_exc)
                else:
                    raise

if __name__ == '__main__':
    run()
