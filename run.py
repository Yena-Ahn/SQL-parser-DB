from berkeleydb import db
from lark import Lark
from table import *
from parser import MyTransformer

"""
Project 1-1 UPDATE
1. parse information and recognise what query has been received
2. Syntax error when query is not under grammar rule

Project 1-2 UPDATE
1. get parsed information with dictionary object or command query
2. create table object to store data
3. store in DB using berkeley db
"""



with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="basic")

def parser(query):
    try:
        result = sql_parser.parse(query)
    except Exception:
        raise SyntaxError("Syntax error") # raise SyntaxError when exception occurs e.g. UnexpectedInput
    else:
        transformed = MyTransformer().transform(result)
        if transformed == 'exit':
            global endloop
            endloop = False
        else:
            #return transformed
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




def database(parsed_dict):
    if parsed_dict["query"] == "create":
        #DuplicateColumnDefError
        if DuplicateColumnDefError(parsed_dict):
            print("Create table has failed: column definition is duplicated")
            return False
        #DuplicatePrimaryKeyDefError
        if len(parsed_dict["primary_key"]) > 1:
            print("Create table has failed: primary key definition is duplicated")
            return False
        #if 
        
                
def DuplicateColumnDefError(parsed_dict):
    col_list = [col[0] for col in parsed_dict["column_list"]]
    no_duplicate = [col[0] for col in parsed_dict["column_list"] if col[0] not in no_duplicate]
    if len(col_list) != len(no_duplicate):
        return True
    return False
"""
def ReferenceTypeError(parsed_dict):
    for i in parsed_dict["foreign_key"]:
        if len(i[0]) != len(i[2]) or 
"""
endloop = True

while endloop:
    query_list = query_sequence()
    for i in query_list:
        try:
            transformed = parser(i)
            
        except SyntaxError as e:
            print("DB_2022-81863>", e) #if error occurs, prints error
            break





