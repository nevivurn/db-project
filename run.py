import sys
from lark import Lark, Transformer, UnexpectedInput

PROMPT = 'DB_2017-19937>'

def println_prompt(msg):
    print(PROMPT, msg)
def print_prompt():
    print(PROMPT, end=' ')

class PrintTransformer(Transformer):
    def create_table_query(self, args):
        println_prompt("'CREATE TABLE' requested")
    def drop_table_query(self, args):
        println_prompt("'DROP TABLE' requested")
    def explain_query(self, args):
        println_prompt("'EXPLAIN' requested")
    def describe_query(self, args):
        println_prompt("'DESCRIBE' requested")
    def desc_query(self, args):
        println_prompt("'DESC' requested")
    def show_tables_query(self, args):
        println_prompt("'SHOW TABLES' requested")
    def select_query(self, args):
        println_prompt("'SELECT' requested")
    def insert_query(self, args):
        println_prompt("'INSERT' requested")
    def delete_query(self, args):
        println_prompt("'DELETE' requested")
    def update_query(self, args):
        println_prompt("'UPDATE' requested")

    def EXIT(self, args):
        sys.exit(0)
        print('exit')

def run():
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
            PrintTransformer().transform(output)

if __name__ == '__main__':
    run()
