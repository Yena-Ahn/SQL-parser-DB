
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

Project 1-3 UPDATE
1. load previous object using pickle
2. load dbs and insert, delete, select, update rows
3. errors are handled
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
        tableNameList = [table.getTableName() for table in record.getTableList()]
        
        #NoSuchTable
        if parsed_dict["table_name"] not in tableNameList:
            parsed_dict["error"].append("NoSuchTable")
            return parsed_dict
        
        table = record.findTable(parsed_dict["table_name"])
        col = parsed_dict["column_list"]
        val = parsed_dict["value_list"]
        col_list = table.getColumns()
        if col != None:
            #InsertTypeMismatchError
            if len(col_list) != len(col):
                parsed_dict["error"].append("InsertTypeMismatchError")
                return parsed_dict    
            for i in range(len(col_list)):
                #InsertColumnExistenceError
                if col[i] != col_list[i].getColName():
                    parsed_dict["error"].append(f"InsertColumnExistenceError{col[i]}")
                    return parsed_dict
        if col == None:
            if len(val) != len(col_list):
                parsed_dict["error"].append("InsertTypeMismatchError")
                return parsed_dict
            
                
        for i in range(len(col_list)):
            #InsertTypeMismathError
            if col_list[i].getDataType() == "char":
                if isinstance(val[i], str) == False:
                    parsed_dict["error"].append("InsertTypeMismatchError")
                    return parsed_dict
                if len(val[i]) > col_list[i].getLengthLimit():
                    val[i] = val[i][:col_list[i].getLengthLimit()]
            if col_list[i].getDataType() == "int":
                if isinstance(val[i], int) == False:
                    parsed_dict["error"].append("InsertTypeMismatchError")
                    return parsed_dict
            if col_list[i].getDataType() == "date":
                if validate(val[i]) == False:
                    parsed_dict["error"].append("InsertTypeMismatchError")
                    return parsed_dict
                
            #InsertColumnNonNullableError
            if val[i] == "null" and col_list[i].not_null == True:
                parsed_dict["error"].append(f"InsertColumnNonNullableError{col_list[i].getColName()}")
                return parsed_dict
        return parsed_dict
    
    if parsed_dict["query"] == "delete":
        tableNameList = [table.getTableName() for table in record.getTableList()]
        
        for tables in parsed_dict["from_clause"]:
            
            #NoSuchTable
            if tables[0] not in tableNameList:
                parsed_dict["error"].append("NoSuchTable")
                return parsed_dict
        
        return whereParse(parsed_dict, record)
    
    if parsed_dict["query"] == "select":
        tableNameList = [table.getTableName() for table in record.getTableList()]
        tableList = record.getTableList()
        #SelectTableExistenceError(#tableName)
        for tables in parsed_dict["from_clause"]:
            if tables[0] not in tableNameList:
                parsed_dict["error"].append(f"SelectTableExistenceError({tables})")
                return parsed_dict
        return whereParse(parsed_dict, record)
    
    if parsed_dict["query"] == "update":
        tableNameList = [table.getTableName() for table in record.getTableList()]
        
        #NoSuchTable
        if parsed_dict["table_name"] not in tableNameList:
            parsed_dict["error"].append("NoSuchTable")
            return parsed_dict
        
        table = record.findTable(parsed_dict["table_name"])
        col = table.findCol(parsed_dict["set"][0])
        if col == None:
            name = parsed_dict["set"][0]
            parsed_dict["error"].append(f"UpdateColumnExistenceError({name})")
            return parsed_dict
        if col.getDataType() == "char":
            if isinstance(parsed_dict["set"][2], str) == False:
                parsed_dict["error"].append("UpdateTypeMismatchError")
                return parsed_dict
        if col.getDataType() == "int":
            if isinstance(parsed_dict["set"][2], int) == False:
                parsed_dict["error"].append("UpdateTypeMismatchError")
                return parsed_dict
        if col.getDataType() == "date":
            if validate(parsed_dict["set"][2]) == False:
                parsed_dict["error"].append("UpdateTypeMismatchError")
                return parsed_dict
        if col.not_null == True and parsed_dict["set"][2] == "null":
            parsed_dict["error"].append(f"UpdateColumnNonNullableError({col.getColName()})")
            return parsed_dict
        if col.isPK():
            parsed_dict["error"].append("UpdateDuplicatePrimaryKeyError")
            return parsed_dict
        if col.isFK():
            parsed_dict["error"].append("UpdateReferentialIntegrityError")
            return parsed_dict
        return parsed_dict
            



def whereParse(parsed_dict, record):
    for where in parsed_dict["where_clause"]:
            table_bool = False
            
            # if columns have reference table (tablename.colname)
            if isinstance(where[0], list):
                for table in parsed_dict["from_clause"]:
                    
                    #if table has been renamed (as)
                    if len(table) > 1:
                        if table[0] == where[0][0] or table[1] == where[0][0]:
                            table_bool = True
                            table_object = record.findTable(table[0])
                            col = table_object.findCol(where[0][1])

                            #WhereColumnNotExist
                            if col == None:
                                parsed_dict["error"].append("WhereColumnNotExist")
                                return parsed_dict
                            
                            #WhereIncomparableError
                            if col.getDataType() == "char":
                                if isinstance(where[2], str) == False:
                                    parsed_dict["error"].append("WhereIncomparableError")
                                    return parsed_dict
                            if col.getDataType() == "int":
                                if isinstance(where[2], int) == False:
                                    parsed_dict["error"].append("WhereIncomparableError")
                                    return parsed_dict
                            if col.getDataType() == "date":
                                if validate(where[2]) == False:
                                    parsed_dict["error"].append("WhereIncomparableError")
                                    return parsed_dict
                    
                    # not renamed
                    else:
                        table_object = record.findTable(table[0])
                        if table_object != None:
                            if table[0] == where[0][0]:
                                table_bool = True
                                col = table_object.findCol(where[0][1])
                                if col == None:
                                    parsed_dict["error"].append("WhereColumnNotExist")
                                    return parsed_dict
                                if col.getDataType() == "char":
                                    if isinstance(where[2], str) == False:
                                        parsed_dict["error"].append("WhereIncomparableError")
                                        return parsed_dict
                                if col.getDataType() == "int":
                                    if isinstance(where[2], int) == False:
                                        parsed_dict["error"].append("WhereIncomparableError")
                                        return parsed_dict
                                if col.getDataType() == "date":
                                    if validate(where[2]) == False:
                                        parsed_dict["error"].append("WhereIncomparableError")
                                        return parsed_dict
                            else:
                                table_bool = True
                                col = table_object.findCol(where[0][0])
                                if col == None:
                                    parsed_dict["error"].append("WhereColumnNotExist")
                                    return parsed_dict
                                if col.getDataType() == "char":
                                    if isinstance(where[2], str) == False:
                                        parsed_dict["error"].append("WhereIncomparableError")
                                        return parsed_dict
                                if col.getDataType() == "int":
                                    if isinstance(where[2], int) == False:
                                        parsed_dict["error"].append("WhereIncomparableError")
                                        return parsed_dict
                                if col.getDataType() == "date":
                                    if validate(where[2]) == False:
                                        parsed_dict["error"].append("WhereIncomparableError")
                                        return parsed_dict
                                
            # WhereTableNotSpecified                    
                if table_bool == False:
                    parsed_dict["error"].append("WhereTableNotSpecified") 
                    return parsed_dict
            # columns that have no reference table (just columns)               
            elif where != "and" and where != "or" and isinstance(where[0], list) == False:
                wanted_col = where[0]
                col_boolean = False
                count = 0
                for tables in parsed_dict["from_clause"]:
                    table = record.findTable(tables[0])
                    if table != None:
                        col = table.findCol(wanted_col)
                        if col != None:
                            col_boolean = True
                            count += 1
                            if count > 1:
                                parsed_dict["error"].append("WhereAmbiguousReference")
                                return parsed_dict
                            if col.getDataType() == "char":
                                if isinstance(where[2], str) == False:
                                    parsed_dict["error"].append("WhereIncomparableError")
                                    return parsed_dict
                            if col.getDataType() == "int":
                                if isinstance(where[2], int) == False:
                                    parsed_dict["error"].append("WhereIncomparableError")
                                    return parsed_dict
                            if col.getDataType() == "date":
                                if validate(where[2]) == False:
                                    parsed_dict["error"].append("WhereIncomparableError")
                                    return parsed_dict
                                
                
                if col_boolean == False:
                    parsed_dict["error"].append("WhereColumnNotExist")
                    return parsed_dict 
    return parsed_dict   

                
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
    elif error_type == "InsertDuplicatePrimaryKeyError":
        print("DB_2022-81863>", "Insertion has failed: Primary key duplication")
    elif error_type == "InsertReferentialIntegrityError":
        print("DB_2022-81863>", "Insertion has failed: Referential integrity violation")
    elif error_type == "InsertTypeMismatchError":
        print("DB_2022-81863>", "Insertion has failed: Types are not matched")
    elif error_type[:len("InsertColumnExistenceError")] == "InsertColumnExistenceError":
        name = error_type[len("InsertColumnExistenceError")+1:-1]
        print("DB_2022-81863>", f"Insertion has failed: '{name}' does not exist")
    elif error_type[:len("InsertColumnNonNullableError")] == "InsertColumnNonNullableError":
        name = error_type[len("InsertColumnNonNullableError")+1:-1]
        print("DB_2022-81863>", f"Insertion has failed: '{name}' is not nullable")
    elif error_type == "WhereIncomparableError":
        print("DB_2022-81863>", "Where clause try to compare incomparable values")
    elif error_type == "WhereTableNotSpecified":
        print("DB_2022-81863>", "Where clause try to reference tables which are not specified")
    elif error_type == "WhereColumnNotExist":
        print("DB_2022-81863>", "Where clause try to reference non existing column")
    elif error_type == "WhereAmbiguousReference":
        print("DB_2022-81863>","Where clause contains ambiguous reference")
    elif error_type[:len("SelectTableExistenceError")] == "SelectTableExistenceError":
        name = error_type[len("SelectTableExistenceError")+1:-1]
        print("DB_2022-81863>", f"Selection has failed: '{name}' does not exist")
    elif error_type == "UpdateTypeMismatchError":
        print("DB_2022-81863>", "Update has failed: Types are not matched")
    elif error_type[:len("UpdateColumnNonNullableError")] == "UpdateColumnNonNullableError":
        name = error_type[len("UpdateColumnNonNullableError")+1:-1]
        print("DB_2022-81863>", f"Update has failed: '{name}' is not nullable")
    elif error_type[:len("UpdateColumnExistenceError")] == "UpdateColumnExistenceError":
        name = error_type[len("UpdateColumnExistenceError")+1:-1]
        print("DB_2022-81863>", f"Update has failed: '{name}' does not exist")
    elif error_type == "UpdateDuplicatePrimaryKeyError":
        print("DB_2022-81863>", "Update has failed: Primary key duplication")
    elif error_type == "UpdateReferentialIntegrityError":
        print("DB_2022-81863>", "Update has failed: Referential integrity violation")
 


def database(dict, record):
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
            
    if dict["query"] == "insert":
        table = dict["table_name"] + ".db"
        myDB = db.DB()
        dir = "db/" + table
        myDB.open(dir, dbtype=db.DB_HASH)
        value_dict = {} #stores values of a row
        table = record.findTable(dict["table_name"])
        column = table.getColumns()
        if dict["column_list"] != None:
            print("YES")
            # put values in dict object
            for i in range(len(dict["column_list"])):
                value_dict[dict["column_list"][i]] = dict["value_list"][i]
            for col in column:
                if col.getColName() not in value_dict.keys():
                    value_dict[col.getColName()] = "null"
            pk = table.getPKname()
            if value_dict[pk] == None:
                print("DB_2022-81863>", f"Insertion has failed: '{pk}' is not nullable")
            bytePk = pickle.dumps(value_dict[pk])
            byteValueDict = pickle.dumps(value_dict) #change dict object to bytes
            myDB.put(bytePk, byteValueDict) #put with pk as a key and dict as a value
            print("DB_2022-81863> The row is inserted")
        else:
            for i in range(len(dict["value_list"])):
                value_dict[column[i].getColName()] = dict["value_list"][i]
            pk = table.getPKname()
            bytePk = pickle.dumps(value_dict[pk])
            byteValueDict = pickle.dumps(value_dict) #change dict object to bytes
            myDB.put(bytePk, byteValueDict) #put with pk as a key and dict as a value
            print("DB_2022-81863> The row is inserted")
        
        myDB.close()
    
    if dict["query"] == "delete":
        count = 0
        for tables in dict["from_clause"]:
            table = tables[0]
            myDB = db.DB()
            table_dir = "db/" + table + ".db"
            myDB.open(table_dir, dbtype=db.DB_HASH)
            cursor = myDB.cursor()
            
            while x:=cursor.next():
                row = pickle.loads(x[1])
                for where in dict["where_clause"]:
                    col = where[0]
                    if where[1] == "=":
                        if row[col] == where[2]:
                            cursor.delete()
                            count += 1
                    if where[1] == "<=":
                        if row[col] <= where[2]:
                            cursor.delete()
                            count += 1
                    if where[1] == ">=":
                        if row[col] >= where[2]:
                            cursor.delete()
                            count += 1
                    if where[1] == ">":
                        if row[col] > where[2]:
                            cursor.delete()
                            count += 1
                    if where[1] == "<":
                        if row[col] < where[2]:
                            cursor.delete()
                            count += 1
            myDB.close()
        print("DB_2022-81863>", f"{count} row(s) are deleted")
                        
    
    if dict["query"] == "select":
        if dict["where_clause"] == []:
            
            #select *
            if dict["column_list"][0] == "*":
                table = dict["from_clause"][0][0]
                table_obj = record.findTable(table)
                cols = table_obj.getColNameList()
                printCols(cols)
                myDB = db.DB()
                myDB.open("db/" + table + ".db", dbtype=db.DB_HASH)
                cursor = myDB.cursor()
                while x:=cursor.next():
                    selectPrint(cols, pickle.loads(x[1]))
                printFirstLast(cols)
            
            #select few columns
            else:
                table = dict["from_clause"][0][0]
                cols = []
                for col_list in dict["column_list"]:
                    cols.append(col_list[1])
                myDB = db.DB()
                myDB.open("db/" + table + ".db", dbtype=db.DB_HASH)
                cursor = myDB.cursor()
                while x:=cursor.next():
                    if selectPrint(cols, pickle.loads(x[1])) == "Error":
                        return
                printCols(cols)
                while x:=cursor.next():
                    selectPrint(cols, pickle.loads(x[1]))
                printFirstLast(cols)
        
        else:
            pass  
        
    if dict["query"] == "update":
        table = dict["table_name"]
        myDB = db.DB()
        myDB.open("db/" + table + ".db", dbtype=db.DB_HASH)
        cursor = myDB.cursor()
        count = 0
        while x:=cursor.next():
            key = pickle.loads(x[0])
            row = pickle.loads(x[1])
            if row[dict["where_clause"][0][0]] == dict["where_clause"][0][2]:
                row[dict["set"][0]] = dict["set"][2]
                myDB.put(pickle.dumps(key), pickle.dumps(row))
                count += 1
        myDB.close()
        print("DB_2022-81863>", f"{count} row(s) are updated")
        

           
def printFirstLast(col_name):
    start1 = "+"
    horizontal = start1 + ""
    length_list = [0 for col in col_name]
    for i in range(len(col_name)):
        length_list[i] = len(col_name[i])
        horizontal += "-" * (length_list[i] + 8) + start1
    print(horizontal)    
        
def printCols(col_name):
    start2 = "|"
    vals = start2 + ""
    printFirstLast(col_name)
    for i in range(len(col_name)):
        vals += " " * 4 + col_name[i].upper() + " " * 4 + "|"
    print(vals)
    printFirstLast(col_name)
    
def selectPrint(col_name, row_dict):
    if isinstance(col_name, str):
        col_name = [col_name]
    
    for i in range(len(col_name)):  
        if col_name[i] not in row_dict.keys():
            print("DB_2022-81863>", f"Selection has failed: fail to resolve '{col_name[i]}'")
            return "Error"
    vals = "|"
    print(vals, end="")
    for i in range(len(col_name)):
        total = len(col_name[i]) + 8
        space = (total - len(str(row_dict[col_name[i]]))) // 2    
        print(" " * space + str(row_dict[col_name[i]]) + " " * (total - space - len(str(row_dict[col_name[i]]))) + "|", end="")
    print()
    
                        

def save_object(obj, filename):
    with open(filename, 'wb') as output:
        pickle.dump(obj, output, -1)

def load_object(filename):
    with open(filename, 'rb') as input:
        return pickle.load(input)


def loadPkl():
    pklData = [f for f in os.listdir(os.curdir) if os.path.isfile(f) and f.endswith(".pkl")]
    return pklData
        
        


endloop = True
record = Record()

while endloop:
    # allData = loadData(record)
    # print(allData)
    query_list = query_sequence()
    for i in query_list:
        try:
            transformed = parser(i)
            print(transformed)
            if transformed == "exit":
                endloop = False
                break
            if len(loadPkl()) > 0: #load objects if they exist
                record_obj = load_object("Record.pkl")
                record = load_object("record.pkl")
            dict = HandlingError(transformed, record) #handle error first
            #save record object so can access to information when program runs again
            save_object(Record, "record_class.pkl")
            save_object(record, "record.pkl")
            
            if dict != None:     
                if len(dict["error"]) != 0:
                    error(dict["error"][0])
                    break
                if dict["query"] == "create" or dict["query"] == "drop" or dict["query"] == "insert" or dict["query"] == "select" or dict["query"] == "delete" or dict["query"] == "update":
                    database(dict, record) 

        except SyntaxError as e:
            print("DB_2022-81863>", e) #if error occurs, prints error
            break





