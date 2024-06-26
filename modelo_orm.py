from peewee import *

sqlite_db = SqliteDatabase('obras_urbanas.db')

# Definición de BaseModel
class BaseModel(Model):
    class Meta:
        database = sqlite_db

'''
    Primero defino las tablas lookup
'''

# Tabla para la etapa de la obra (ver relacion con foreign key)
class Etapa(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'etapas'

# Tabla para el tipo de obra (ver relacion con foreign key)
class Tipo(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'tipos'

# Tabla para el área responsable (ver relacion con foreign key)
class AreaResponsable(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'areas_responsables'

# Tabla para la comuna (ver relacion con foreign key)
class Comuna(BaseModel):
    nombre = IntegerField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'comunas'

# Tabla para el barrio (ver relacion con foreign key)
class Barrio(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'barrios'

# Tabla para la empresa de licitación (ver relacion con foreign key)
class LicitacionEmpresa(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'empresas'

# Tabla para el tipo de contratación (ver relacion con foreign key)
class ContratacionTipo(BaseModel):
    nombre = CharField(unique=True)
    def __str__(self):
        return self.nombre
    class Meta:
        db_table = 'contrataciones_tipos'

# Tabla para financiamiento de obra (ver relacion con foreign key)
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
    id = AutoField(primary_key=True)
    entorno = CharField()
    nombre = CharField()
    etapa = ForeignKeyField(Etapa, backref='etapa')
    tipo = ForeignKeyField(Tipo, backref='tipo')
    area_responsable = ForeignKeyField(AreaResponsable, backref='area_responsable')
    descripcion = TextField()
    monto_contrato = FloatField()
    comuna = ForeignKeyField(Comuna, backref='comuna')
    barrio = ForeignKeyField(Barrio, backref='barrio')
    direccion = CharField()
    lat = CharField()
    lng = CharField()
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
    beneficiarios = CharField()
    mano_obra = IntegerField()
    compromiso = TextField()
    destacada = TextField()
    ba_elige = TextField()
    link_interno = CharField()
    pliego_descarga = CharField()
    expediente_numero = CharField()
    estudio_ambiental_descarga = CharField()
    financiamiento = ForeignKeyField(Financiamiento, backref='financiamiento')
    def __str__(self):
        pass
    class Meta:
        db_table = 'obras'

    def nuevo_proyecto(self, registro):
        # self.etapa = "Proyecto"
        try:
            # Intenta obtener la instancia de Etapa con nombre "Proyecto"
            self.etapa = Etapa.get(Etapa.nombre == "Proyecto")
        except DoesNotExist:
            # Si no existe, crea una nueva instancia de Etapa y guárdala
            self.etapa = Etapa.create(nombre="Proyecto")
            print(f"Etapa 'Proyecto' creada.")
            
        print(f"Obra creada exitosamente.")
        registro.save()
        # print("Elija uno de loas barrios existentes")
        # self.tipo = input("Ingrese el barrio")
        # registro.save()

    def iniciar_contratacion(self):
        pass

    def adjudicar_obra(self):
        pass

    def iniciar_obra(self):
        pass

    def actualizar_porcentaje_avance(self):
        pass

    def incrementar_plazo(self):
        pass

    def incrementar_mano_obra(self):
        pass

    def finalizar_obra(self):
        pass

    def rescindir_obra(self):
        pass