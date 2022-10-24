"""
Store data information in table class
"""
class Record:
    def __init__(self):
        self.__table_list = []
    def getTableList(self):
        return self.__table_list
    def addTable(self, table):
        self.__table_list.append(table)
    def findTable(self, table_name):
        for t in self.__table_list:
            if t.getTableName() == table_name:
                return t
        return None
    def removeTable(self, table):
        for i in range(len(self.__table_list)):
            if self.__table_list[i] == table:
                self.__table_list = self.__table_list[:i] + self.__table_list[i+1:]
                return True
        return False

class Table:
    def __init__(self, table_name):
        self.__table_name = table_name.lower()
        self.__columns = []
        self.__referencedBY = []
    def getTableName(self):
        return self.__table_name
    def getColumns(self):
        return self.__columns
    def getColNameList(self):
        return [col.getColName() for col in self.__columns]
    def getReferencedBY(self):
        return self.__referencedBY
    def addCol(self, column):
        self.__columns.append(column)
    def addColList(self, col_list):
        self.__columns += col_list
    def findCol(self, col):
        for c in self.__columns:
            if c.getColName() == col:
                return c
    def addReferencedBy(self, table):
        self.__referencedBY.append(table)
    def isReferenced(self):
        if len(self.getReferencedBY) == 0:
            return False
        return True
    def getPKname(self):
        return [col.getColName for col in self.__columns if col.isPK()]
    def getPK(self):
        pkList = [col for col in self.__columns if col.isPK()]
        return pkList


class Column:
    def __init__(self, col_name, data_type, length_limit, not_null):
        self.__col_name = col_name.lower()
        self.__data_type = data_type
        self.__length_limit = length_limit
        self.not_null = not_null
        self.__is_pk = False
        self.__is_fk = False
    def getColName(self):
        return self.__col_name
    def getDataType(self):
        return self.__data_type
    def getLengthLimit(self):
        return self.__length_limit
    def isPK(self):
        return self.__is_pk
    def isFK(self):
        return self.__is_fk
    def setPK(self):
        self.__is_pk = True
        self.not_null = True
    def setFK(self):
        self.__is_fk = True




