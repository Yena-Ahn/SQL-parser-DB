
from berkeleydb import db
from lark import Lark
from table import *
from parser import MyTransformer
import os
import pickle
import datetime

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
            return "exit"
        else:
            return transformed
            #print("DB_2022-81863>",transformed)
        
        
        
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


def validate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        return False
    return True


def HandlingError(parsed_dict, record):
    if parsed_dict["query"] == "create":
        table = parsed_dict["table_name"]
        
        #TableExistenceError
        for i in record.getTableList():
            if table.getTableName() == i.getTableName():
                parsed_dict["error"].append("TableExistenceError")
                return parsed_dict
        
        #DuplicateColumnDefError 
        if DuplicateColumnDefError(parsed_dict):
            parsed_dict["error"].append("DuplicateColumnDefError")
            return parsed_dict
        
        if "foreign_key" in parsed_dict.keys():
            for group in parsed_dict["foreign_key"]:
                #ReferenceTypeError
                #number of referenced columns are diff
                if len(group[0]) != len(group[2]):
                    parsed_dict["error"].append("ReferenceTypeError")
                    return parsed_dict
                
                ref_table = record.findTable(group[1][0])
                
                #ReferenceTableExistenceError
                if ref_table == None:
                    parsed_dict["error"].append("ReferenceTableExistenceError")
                    return parsed_dict
                
                #ReferenceTypeError: datatype is diff
                for i in range(len(group[0])):
                    if table.findCol(group[0][i]).getDataType() != ref_table.findCol(group[2][i]).getDataType():
                        parsed_dict["error"].append("ReferenceTypeError")
                        return parsed_dict
                    if table.findCol(group[0][i]).getLengthLimit() != ref_table.findCol(group[2][i]).getLengthLimit():
                        parsed_dict["error"].append("ReferenceTypeError")
                        return parsed_dict
                
                # ReferenceNonPrimaryKeyError
                for item in group[2]:
                    if item in ref_table.getColNameList() and item not in ref_table.getPKname():
                        parsed_dict["error"].append("ReferenceNonPrimaryKeyError")
                        return parsed_dict
                        
                # ReferenceColumnExistenceError
                for item in group[2]:
                    if item not in ref_table.getColNameList():
                        parsed_dict["error"].append("ReferenceColumnExistenceError")
                        return parsed_dict
                
                # NonExistingColumnDefError
                for key in group[0]:
                    if key not in table.getColNameList():
                        parsed_dict["error"].append(f"NonExistingColumnDefError({key})")
                        return parsed_dict
                
                # add to referencedby table if no error occurs
                ref_table.addReferencedBy(table)
                for item in group[0]:
                    col = table.findCol(item)
                    col.setFK()
                    
                
        
         # NonExistingColumnDefError(#colName)   
        for key in parsed_dict["primary_key"]:
            if key not in table.getColNameList():
                parsed_dict["error"].append(f"NonExistingColumnDefError({key})")
                return parsed_dict

        #add to record if no error
        for key in parsed_dict["primary_key"]:
            col = table.findCol(key)
            col.setPK()
        record.addTable(table)
            
        
        
        return parsed_dict
    
    if parsed_dict["query"] == "drop":
        table = record.findTable(parsed_dict["table_name"])
        
        #NoSuchTable
        if table not in record.getTableList():
            parsed_dict["error"].append("NoSuchTable")
            return parsed_dict
        
        #DropReferencedTableError(#tableName)
        if len(table.getReferencedBY()) > 0:
             parsed_dict["error"].append(f"DropReferencedTableError({table.getTableName()})")
             return parsed_dict
         
         #remove from record if no error
        record.removeTable(table)
        return None

    if parsed_dict["query"] == "desc":
        table = record.findTable(parsed_dict["table_name"])
        
        #NoSuchTable
        if table not in record.getTableList():
            parsed_dict["error"].append("NoSuchTable")
            return parsed_dict
        
        print("-------------------------------------------------")
        print(f"table_name [{table.getTableName()}]")
        space = " " * 8
        print("column_name" + space + "type" + space + "null" + space + "key")
        
        for c in table.getColumns():
            name = c.getColName()
            datatype = c.getDataType()
            length = c.getLengthLimit()
            null = "Y"
            if c.not_null:
                null = "N"
            string = ""
            if c.isPK():
                string += "PRI"
                if c.isFK():
                    string += "/FOR"
            elif c.isFK():
                string += "FOR"
                
            foreign = c.isFK()
            if datatype == "char":
                print(name + " " * (19-len(name)) + datatype + f"({length})" + " " * (12-6-len(f"{length}")) + null + " " * 11 + string)
            else:
                print(name + " " * (19-len(name)) + datatype + " " * (12-len(datatype)) + null + " " * 11 + string)
        print("-------------------------------------------------")
        return None
    
    if parsed_dict["query"] == "show":
        print("----------------")
        for item in record.getTableList():
            print(item.getTableName())            
        print("----------------")
        return None
    
    if parsed_dict["query"] == "insert":
        if parsed_dict["table_name"] not in record.getTableList():
            parsed_dict["error"].append("NoSuchTable")
            return parsed_dict
        table = record.findTable(parsed_dict["table_name"])
        col = parsed_dict["column_list"]
        val = parsed_dict["value_list"]
        if col != None:
            col_list = table.getColumns()
            if len(col_list) != len(col):
                parsed_dict["error"].append("InsertTypeMismathError")
                return parsed_dict
            for i in range(len(col_list)):
                if col_list[i].getDataType() == "char":
                    if isinstance(val[i], str) == False:
                        parsed_dict["error"].append("InsertTypeMismathError")
                        return parsed_dict
                if col_list[i].getDataType() == "int":
                    if isinstance(val[i], int) == False:
                        parsed_dict["error"].append("InsertTypeMismathError")
                        return parsed_dict
                if col_list[i].getDataType() == "date":
                    if validate(val[i]) == False:
                        parsed_dict["error"].append("InsertTypeMismathError")
                        return parsed_dict
                    
            
        else:
            col_list = table.getColumns()
            
            

       
        
        
        
        
            
        
                
def DuplicateColumnDefError(parsed_dict):
    col_list = [col for col in parsed_dict["column_list"]]
    no_duplicate = []
    for col in parsed_dict["column_list"]:
        if col not in no_duplicate:
            no_duplicate.append(col)
    if len(col_list) != len(no_duplicate):
        return True
    return False

def error(error_type):
    if error_type == "DuplicateColumnDefError":
        print("DB_2022-81863>", "Create table has failed: column definition is duplicated")
    elif error_type == "DuplicatePrimaryKeyDefError":
        print("DB_2022-81863>", "Create table has failed: primary key definition is duplicated")
    elif error_type == "ReferenceTypeError":
        print("DB_2022-81863>", "Create table has failed: foreign key references wrong type")
    elif error_type == "ReferenceNonPrimaryKeyError":
        print("DB_2022-81863>", "Create table has failed: foreign key references non primary key column")
    elif error_type == "ReferenceColumnExistenceError":
        print("DB_2022-81863>", "Create table has failed: foreign key references non existing column")
    elif error_type == "ReferenceTableExistenceError":
        print("DB_2022-81863>", "Create table has failed: foreign key references non existing table")
    elif error_type[:len("NonExistingColumnDefError")] == "NonExistingColumnDefError":
        name = error_type[len("NonExistingColumnDefError")+1:-1]
        print("DB_2022-81863>", f"Create table has failed: '{name}' does not exists in column definition")
    elif error_type == "TableExistenceError":
        print("DB_2022-81863>", "Create table has failed: table with the same name already exists")
    elif error_type[:len("DropReferencedTableError")] == "DropReferencedTableError":
        name = error_type[len("DropReferencedTableError")+1:-1]
        print("DB_2022-81863>", f"Drop table has failed: '{name}' is referenced by other table")
    elif error_type == "NoSuchTable":
        print("DB_2022-81863>", "NoSuchTable")
    elif error_type == "CharLengthError":
        print("DB_2022-81863>", "Char length should be over 0")        


def database(dict, allData):
    if dict["query"] == "create":
        table = dict["table_name"]
        table_name = table.getTableName()
        myDB = db.DB()
        dir = f"db/{table_name}.db"
        myDB.open(dir, dbtype=db.DB_HASH, flags=db.DB_CREATE)
        # myDB.put(table.getPK(), table.get)
        myDB.close()
        print("DB_2022-81863>", f"'{table_name}' table is created")
    if dict["query"] == "drop":
        table = dict["table_name"]
        table_name = table.getTableName()
        file = f"db/{table_name}.db"
        if os.path.isfile(file):
            os.remove(file)
            print("DB_2022-81863>", f"'{table_name}' table is dropped")

        
            

def loadData(record):
    allData = [f for f in os.listdir("db") if os.path.isfile(os.path.join("db", f))]
    return allData

def save_object(obj, filename):
    with open(filename, 'wb') as output:
        pickle.dump(obj, output, -1)

def load_object(filename):
    with open(filename, 'rb') as input:
        return pickle.load(input)





        
        
        





endloop = True
record = Record()

while endloop:
    allData = loadData(record)
    print(allData)
    query_list = query_sequence()
    for i in query_list:
        try:
            transformed = parser(i)
            print(transformed)
            if transformed == "exit":
                endloop = False
                break
            dict = HandlingError(transformed, record)
            save_object(record, "record.pkl")
            if dict != None:
                if len(dict["error"]) != 0:
                    error(dict["error"][0])
                    break
                if dict["query"] == "create" or dict["query"] == "drop":
                    database(dict)
                 
        except SyntaxError as e:
            print("DB_2022-81863>", e) #if error occurs, prints error
            break





