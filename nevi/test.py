from db import DB
from db_types import *

with DB('test.db') as db:
    db.create_table(Table(
        name='test_table_1',
        columns=[
            Column(
                name='column',
                col_type=ColumnType(ColumnTypeClass.INT),
            ),
        ],
        primary_keys=[[
            'column',
        ]],
    ))
    db.create_table(Table(
        name='test_table_2',
        columns=[
            Column(
                name='column',
                col_type=ColumnType(ColumnTypeClass.INT),
                nullable=False,
            ),
        ],
        foreign_keys=[ForeignKey(
            columns=['column'],
            ref_table='test_TABLE_1',
        )]
    ))
    db.create_table(Table(
        name='test_table_3',
        columns=[
            Column(
                name='column',
                col_type=ColumnType(ColumnTypeClass.CHAR, 3),
                nullable=False,
            ),
        ],
        primary_keys=[['column']],
    ))

    print(db.show_tables())
    print(db.explain_table('test_table_1'))
    print(db.explain_table('test_table_2'))
    print(db.explain_table('test_table_3'))

    print(db.insert_values('test_table_3', None, ['ab']))
    print(db.insert_values('test_table_3', None, ['a']))
    print(db.insert_values('test_table_3', None, ['abc']))
    #print(db.insert_values('test_table_1', None, [1]))
    #print(db.insert_values('test_table_2', None, [[1]]))
    #print(db.insert_values('test_table_2', ['CoLuMn'], [[1]]))
    #print(db.insert_values('test_table_3', ['CoLuMnas'], [[None]]))
