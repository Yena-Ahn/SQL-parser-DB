from bsddb3 import db
#from berkeleydb import db

class DataBase:
    def __init__(self, parsed_item):
        self.DB = db.DB()
    