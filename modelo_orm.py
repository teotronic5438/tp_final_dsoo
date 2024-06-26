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

class Obra:

    def nuevo_proyecto():
        pass

    def iniciar_contratacion():
        pass

    def adjudicar_obra():
        pass

    def iniciar_obra():
        pass

    def actualizar_porcentaje_avance():
        pass

    def incrementar_plazo():
        pass

    def incrementar_mano_obra():
        pass

    def finalizar_obra():
        pass

    def rescindir_obra():
        pass

    