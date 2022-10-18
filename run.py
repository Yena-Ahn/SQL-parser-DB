from bsddb3 import db
#from berkeleydb import db
from lark import Lark, Transformer


class MyTransformer(Transformer):
    """
    1. join items of parent nodes of each query - command, query_list and query
    2. get information of query (parse only the first item of each query for this assignment)
    """
    EXIT = str
    PRIMARY = str
    KEY = str
    FOREIGN = str
    REFERENCES = str
    IDENTIFIER = str
    LP = str
    RP = str
    TYPE_CHAR = str
    NOT = str
    NULL = str
    CREATE = str
    TABLE = str
    TYPE_INT = str
    TYPE_CHAR = str
    INT = int
    #parent nodes of queries
    # command = lambda self, items: "".join(map(str,items))
    # query_list = lambda self, items: "".join(map(str,items))
    #query = lambda self, items: "".join(map(str, items))
    def command(self, items):
        return items
    def query_list(self, items):
        return items
    
    def query(self, items):
        return items
    
    def create_table_query(self, items):
        return items
    
    def data_type(self, items):
        return items
    
    def table_name(self, table_name):
        return table_name
    
    def table_element_list(self, table_element):
        result = [i for i in table_element if i != '(' and i != ')']
        return result
    
    def table_element(self, items):
        return items[0]
    
    def table_constraint_definition(self, items):
        return items[0]
        
    def column_definition(self, column_definition):
        if column_definition[-2:] != ['not', 'null']:
            return column_definition[:2]
        return column_definition
    
    def column_name(self, column_name):
        return column_name[0]
    
    def column_name_list(self, column_name_list):
        result = [i for i in column_name_list if i != '(' and i !=')']
        return result
    
    def primary_key_constraint(self, primary_key_constraint):
        return primary_key_constraint
    
    def referential_constraint(self, items):
        return items
    
    ###########################################
    
    def drop_table_query(self, items):
        return sum(items, [])
    
    def desc_table_query(self, items):
        return items
    
    def insert_query(self, items):
        return f"'{items[0].upper()}' requested"
    
    def delete_query(self, items):
        return f"'{items[0].upper()}' requested"
    
    def select_query(self, items):
        return f"'{items[0].upper()}' requested"
    
    def show_table_query(self, items):
        return " ".join(map(str, items))
    
    def update_tables_query(self, items):
        return f"'{items[0].upper()}' requested"


with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

def parser(query):
    try:
        result = sql_parser.parse(query)
    except Exception:
        raise SyntaxError("Syntax error") # rasie SyntaxError when exception occurs e.g. UnexpectedInput
    else:
        transformed = MyTransformer().transform(result)
        if transformed == 'exit':
            global endloop
            endloop = False
        else:
            print("DB_2022-81863>",transformed)
        
        
        
def query_sequence():
    query = input("DB_2022-81863> ")
    while True:
        if query[-1] != ";":
            query += " " + input() # concate all query until semi-colon appears
            #program does not change indentation automatically so the user needs to put indent e.g. tab
        else:
            query = query.replace("\\n", "")
            query = query.replace("\\r", "") #remove line breaks
            break
    query_list = query.split("; ")
    for i in range(len(query_list)):
        if query_list[i][-1] != ";":
            query_list[i] = query_list[i] + ";"
    return query_list

endloop = True

while endloop:
    query_list = query_sequence()
    for i in query_list:
        try:
            transformed = parser(i)
            
        except SyntaxError as e:
            print("DB_2022-81863>", e) #if error occurs, prints error
            break





