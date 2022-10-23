
from lark import Lark, Transformer

class MyTransformer(Transformer):
    
    """
    Project 1-1
    1. join items of parent nodes of each query - command, query_list and query
    2. get information of query (parse only the first item of each query for this assignment)
    
    Project 1-2 UPDATE
    1. create dictionary object to store parsed information when creation of table
    2. recognise what command the user inputs (drop, desc, show)
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
    
    def __init__(self):
        self.table_dict = {}
        
    """
    *PRJ 1-1* parent nodes of queries 
    # command = lambda self, items: "".join(map(str,items))
    # query_list = lambda self, items: "".join(map(str,items))
    # query = lambda self, items: "".join(map(str, items))
    """
    
    def command(self, items):
        return self.table_dict
    
    def query_list(self, items):
        return items
    
    def query(self, items):
        return items
    
    def create_table_query(self, items):
        self.table_dict["table_name"] = items[2][0]
        return items
    
    def data_type(self, items):
        if items[0] == 'char':
            return [items[0], items[2]]
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
        if "column_list" not in self.table_dict.keys():
            self.table_dict["column_list"] = []
        if column_definition[-2:] != ['not', 'null']:
            self.table_dict["column_list"].append(column_definition[:2] + [False])
            return column_definition[:2]
        self.table_dict["column_list"].append(column_definition[:2] + [True])
        return column_definition
    
    def column_name(self, column_name):
        return column_name[0]
    
    def column_name_list(self, column_name_list):
        result = [i for i in column_name_list if i != '(' and i !=')']
        return result
    
    def primary_key_constraint(self, primary_key_constraint):
        primary_key_constraint[0] = primary_key_constraint[0] +  primary_key_constraint[1]
        if "primary_key" not in self.table_dict.keys():
            self.table_dict["primary_key"] = []
        self.table_dict["primary_key"] += sum(primary_key_constraint[2:], [])
        return [primary_key_constraint[0]] + primary_key_constraint[2:]
    
    def referential_constraint(self, items):
        items[0] = items[0] + items[1]
        if "foreign_key" not in self.table_dict.keys():
            self.table_dict["foreign_key"] = []
        self.table_dict["foreign_key"].append([items[2]] + items[4:])
        return [items[0]] + items[2] + items[4:]
    
    ###########################################
    
    def drop_table_query(self, items):
        return sum(items, [])
    
    def desc_table_query(self, items):
        return items
    
    def show_table_query(self, items):
        return " ".join(map(str, items))
    
    ##########################################
    
    #No need for PRJ 1-2
    def insert_query(self, items):
        return f"'{items[0].upper()}' requested"
    
    def delete_query(self, items):
        return f"'{items[0].upper()}' requested"
    
    def select_query(self, items):
        return f"'{items[0].upper()}' requested"
    
    
    def update_tables_query(self, items):
        return f"'{items[0].upper()}' requested"