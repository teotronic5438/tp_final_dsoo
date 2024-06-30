from peewee import *

sqlite_db = SqliteDatabase('obras_urbanas.db')

'''
# conecto a la base de datos
try:
    sqlite_db.connect()
except OperationalError as e:
    print("Error al conectar con la BD.", e)
    exit()
'''

# Definición de BaseModel
class BaseModel(Model):
    class Meta:
        database = sqlite_db

'''
    Primero defino las tablas lookup
'''

# Tabla para la etapa de la obra (relacionada por foreign key)
class Etapa(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'etapas'

# Tabla para el tipo de obra (relacionada por foreign key)
class Tipo(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'tipos'

# Tabla para el área responsable (relacionada por foreign key)
class AreaResponsable(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'areas_responsables'

# Tabla para la comuna (relacionada por foreign key)
class Comuna(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'comunas'

# Tabla para el barrio (relacionada por foreign key)
class Barrio(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'barrios'

# Tabla para la empresa de licitación (relacionada por foreign key)
class LicitacionEmpresa(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'empresas'

# Tabla para el tipo de contratación (relacionada por foreign key)
class ContratacionTipo(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'contrataciones_tipos'

# Tabla para financiamiento de obra (relacionada por foreign key)
class Financiamiento(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'financiamientos'
'''
    FIN DE LAS TABLAS LOOKUP
'''

# Modelo principal para los datos de la obra
class Obra(BaseModel):
    id = AutoField()
    entorno = CharField()
    nombre = CharField()
    etapa = ForeignKeyField(Etapa, backref='etapa')
    tipo = ForeignKeyField(Tipo, backref='tipo')
    area_responsable = ForeignKeyField(AreaResponsable, backref='area_responsable')
    descripcion = TextField()
    monto_contrato = DecimalField(max_digits=10, decimal_places=2)
    comuna = ForeignKeyField(Comuna, backref='comuna')
    barrio = ForeignKeyField(Barrio, backref='barrio')
    direccion = CharField()
    lat = FloatField()
    lng = FloatField()
    fecha_inicio = DateField()
    fecha_fin_inicial = DateField()
    plazo_meses = IntegerField()
    porcentaje_avance = FloatField()
    imagen_1 = CharField()
    imagen_2 = CharField()
    imagen_3 = CharField()
    imagen_4 = CharField()
    licitacion_oferta_empresa = ForeignKeyField(LicitacionEmpresa, backref='licitacion_oferta_empresa')
    licitacion_anio = IntegerField()
    contratacion_tipo = ForeignKeyField(ContratacionTipo, backref='contratacion_tipo')
    nro_contratacion = CharField()
    cuit_contratista = CharField()
    beneficiarios = IntegerField()
    mano_obra = IntegerField()
    compromiso = TextField()
    destacada = BooleanField()
    ba_elige = BooleanField()
    link_interno = CharField()
    pliego_descarga = CharField()
    expediente_numero = CharField()
    estudio_ambiental_descarga = CharField()
    financiamiento = ForeignKeyField(Financiamiento, backref='financiamiento')
    def __str__(self):
        pass
    class Meta:
        db_table = 'obras'
'''
#Creamos las tablas correspondientes a las clases del modelo
try:
    sqlite_db.create_tables([Etapa, Tipo, AreaResponsable, Comuna, Barrio, LicitacionEmpresa, ContratacionTipo,Financiamiento, Obra])
except OperationalError as e:
    print("Error al crear tablas:", e)
    sqlite_db.close()
    exit()
else:
    print("tablas creadas exitosamente")
    
# Cerrar la conexión a la base de datos cuando hayamos terminado
sqlite_db.close()
'''


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

    