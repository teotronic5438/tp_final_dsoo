from peewee import *

sqlite_db = SqliteDatabase('./obras_urbanas.db', pragmas={'journal_mode': 'wal'})

try:
    sqlite_db.connect()
except OperationalError as e:
    print("Error al conectar con la BD.", e)
    exit()
else:
    print("Connexion realizada exitosamente")

class BaseModel(Model):
    class Meta:
        database = sqlite_db

class User(BaseModel):
    username = CharField(unique=True)
    email = CharField()
    joined_date = DateTimeField()
    class Meta:
        db_table = 'usuarios'

sqlite_db.create_tables([User])