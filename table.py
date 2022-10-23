"""
Store data information in table class
"""

class Table:
    def __init__(self, table_name):
        self.__table_name = table_name.lower()
        self.__columns = []
        self.__referencedBY = []
    def getTableName(self):
        return self.__table_name
    def getColumns(self):
        return self.__columns
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
    def getPK(self):
        pkList = [col for col in self.__columns if col.isPrimary()]
        return pkList

class Column:
    def __init__(self, col_name, data_type, length_limit = None, not_null=False):
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


        


class ForeignKey:
    def __init__(self, col_name, reference_table, reference_col):
        self.__fk = col_name
        self.__reference_table = reference_table
        self.__reference_col = reference_col
    def getFK(self):
        return self.__fk
    def getReferenceTable(self):
        return self.__reference_table
    def getRC(self):
        return self.__reference_col
    



