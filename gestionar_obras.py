# Cargo la info del modelo de BBDD
from modelo_orm import *
# Ahora cargo los modulos que preciso para este modulo
from abc import ABC, abstractmethod
import pandas as pd
from unidecode import unidecode

# Clase abstracta para gestionar obras
class GestionarObra(ABC):
    archivo_csv = "./observatorio-de-obras-urbanas.csv"

    @classmethod
    @abstractmethod
    def extraer_datos(cls):
        # Usamos una excepcion en caso de no poder leer el csv con pandas
        try:
            columnas_validas = [
                "id", "entorno", "nombre", "etapa", "tipo", "area_responsable", "descripcion", "monto_contrato",
                "comuna", "barrio", "direccion", "lat", "lng", "fecha_inicio", "fecha_fin_inicial", "plazo_meses",
                "porcentaje_avance", "imagen_1", "imagen_2", "imagen_3", "imagen_4", "licitacion_oferta_empresa",
                "licitacion_anio", "contratacion_tipo", "nro_contratacion", "cuit_contratista", "beneficiarios",
                "mano_obra", "compromiso", "destacada", "ba_elige", "link_interno", "pliego_descarga", "expediente-numero",
                "estudio_ambiental_descarga", "financiamiento"
            ]
            # Nota a memoria, el archivo trae una columna vacia, se restringe a las 36 de estructura
            df = pd.read_csv(cls.archivo_csv, delimiter=';', usecols=columnas_validas, encoding='ISO-8859-1')
            # df = pd.read_csv(cls.archivo_csv, sep=";", encoding='ISO-8859-1')
            # nota a memoria:     campo='Urbanización'.encode('ISO-8859-1').decode('utf-8'), antes de cargar bbdd

        except FileNotFoundError as e:
            print("Error al conectar con el dataset.", e)
            return False
        else:
            print("csv extraido exito")
            # print(df.shape[1])
            # print(df)
            return df

    @classmethod
    @abstractmethod
    def conectar_db(cls):
        # conecto a la base de datos
        if sqlite_db.is_closed():
            try:
                sqlite_db.connect()
            except OperationalError as e:
                print("Error al conectar con la BD.", e)
                exit()
        else:
            print("La conexión a la BD ya está abierta.")

    @classmethod
    @abstractmethod
    def mapear_orm(cls):
        # Me aseguro que la conexión a la bbdd está abierta
        cls.conectar_db()

        # Método para mapear la estructura de la base de datos utilizando peewee
        # Creamos las tablas correspondientes a las clases del modelo
        try:
            sqlite_db.create_tables([Etapa, Tipo, AreaResponsable, Comuna, Barrio, LicitacionEmpresa, ContratacionTipo,Financiamiento, Obra])
        except OperationalError as e:
            print("Error al crear las tablas:", e)
            sqlite_db.close()
            exit()
        else:
            print("tablas creadas exitosamente")
        finally:
            if not sqlite_db.is_closed():
                # Cerrar la conexión a la base de datos cuando hayamos terminado
                sqlite_db.close()



    @classmethod
    @abstractmethod
    def limpiar_datos(cls, df):
        # 1) Primero vamos a limpiar los campos vacios de las columnas necesarias para los indicadores

        # NOTA A PROFESOR: mano_obra me baja de 1325 a 335 registros y elimina una cantidad importante de datos
        columnas_a_verificar = ['etapa', 'tipo', 'area_responsable', 'monto_contrato', 'comuna', 'barrio', 'plazo_meses', 'porcentaje_avance', 'fecha_inicio', 'fecha_fin_inicial', 'mano_obra']

        #Eliminar valores NA o NaN (nulos o no disponibles) de las columnas usadas para los indicadores
        df.dropna(subset=columnas_a_verificar, axis = 0, inplace = True)

        # Faltaria agregar limpieza de campos con acento, sin acento, mal escritos

        # Corrección de errores comunes
        correcciones = {
            'Licicitación pública': 'Licitacion publica',
            'Licitacion pública': 'Licitacion publica',
            'Licicitacion publica' : 'Licitacion publica',
            'Licicitacion publica ' : 'Licitacion publica',
            'Licitación pública': 'Licitacion publica',
            'Contrataciín directa': 'Contratacion Directa',
            'Licitacion privada obra menor' : 'Licitacion privada de obra menor',
            'Ad mantenimiento': 'Ad. mantenimiento',
            'Licitación pública nacional': 'Licitacion publica nacional',
            'Licitación privada': 'Licitacion privada',
            'Licitación privada de obra menor': 'Licitacion privada de obra menor',
            'Contratación menor': 'Contratacion menor',
            'Licitación pública abreviada.': 'Licitacion publica abreviada'
        }


        # Columnas a normalizar
        columnas_a_normalizar = ['etapa', 'tipo', 'area_responsable', 'comuna', 'barrio', 'licitacion_oferta_empresa', 'contratacion_tipo', 'financiamiento']

        # Función para limpiar y corregir texto
        def limpiar_texto(texto):
            if pd.isna(texto):
                return texto
            # Convertir a minúsculas y eliminar acentos
            texto = unidecode(texto.lower())
            return correcciones.get(texto, texto).capitalize()
            # Corregir y capitalizar

        # Aplicar la función a las columnas de interés
        for columna in columnas_a_normalizar:
            df[columna] = df[columna].apply(limpiar_texto)


        # 2) Hago la conversion de las columnas numéricas segun tipo y relleno
        conversiones = {
            "id": "integer",
            "monto_contrato": "float",
            "comuna": "integer",
            "plazo_meses": "integer",
            "porcentaje_avance": "integer",
            "licitacion_anio": "integer",
            "mano_obra": "integer"
        }


        # 2.1) Itero las columnas del DataFrame para el rellenado
        for columna in df:
            # Convierto las columnas numéricas según el tipo especificado en conversiones
            if columna in conversiones:
                df[columna] = pd.to_numeric(df[columna], errors='coerce', downcast=conversiones[columna])

            # Relleno los valores nulos (NaN) con el contenido N/A o no disponible (consultado con el profesor)
            for columna in df.columns:
                if columna in conversiones.keys():
                    df[columna] = df[columna].fillna(0)
                elif columna not in columnas_a_verificar:
                    if df[columna].dtype == "object":
                        df[columna] = df[columna].fillna("no disponible")

        # Forzar la conversión de plazo_meses y mano de obra a tipo entero (pandas los hace float por tener textos)
        df["plazo_meses"] = df["plazo_meses"].astype(int)
        df["mano_obra"] = df["mano_obra"].astype(int)
        df["comuna"] = df["comuna"].astype(int)
        df["licitacion_anio"] = df["licitacion_anio"].astype(int)

        # Aplico capitalize a todo el DataFrame si es de tipo string
        for columna in df:
            if df[columna].dtype == 'object':
                df[columna] = df[columna].str.capitalize()

        # Muestra el dataframe limpio
        # print(df['financiamiento'].unique())  # solo pruebas unitarias
        # print(df['comuna'].unique())
        df.to_csv('./csv_limpiado.csv', index=False, sep=';')
        return df

    @classmethod
    @abstractmethod
    def cargar_datos(cls, df):

        # Me aseguro que la conexión a la bbdd está abierta
        cls.conectar_db()

        # Valido si hay datos en la tabla Obra antes de proceder
        if Obra.select().exists():
            print("La tabla Obra ya contiene datos.")
            # Me aseguro que la conexion a la bbdd quede cerrada
            sqlite_db.close()
            return


        #Obtener los valores únicos (no repetidos) de una columna
        # Diccionario de valores únicos por columna de interés
        valores_unicos = {
            'etapa': df['etapa'].unique(),
            'tipo': df['tipo'].unique(),
            'area_responsable': df['area_responsable'].unique(),
            'comuna': df['comuna'].unique(),
            'barrio': df['barrio'].unique(),
            'licitacion_oferta_empresa': df['licitacion_oferta_empresa'].unique(),
            'contratacion_tipo': df['contratacion_tipo'].unique(),
            'financiamiento': df['financiamiento'].unique()
        }

        # Diccionario de clases ORM correspondientes a cada campo
        clases_orm = {
            'etapa': Etapa,
            'tipo': Tipo,
            'area_responsable': AreaResponsable,
            'comuna': Comuna,
            'barrio': Barrio,
            'licitacion_oferta_empresa': LicitacionEmpresa,
            'contratacion_tipo': ContratacionTipo,
            'financiamiento': Financiamiento
        }

        # Iterar sobre el diccionario y crear instancias en las tablas lookup
        for campo, valores in valores_unicos.items():
            for valor in valores:
                if pd.notna(valor):  # Verificar que el valor no sea NaN
                    clase_orm = clases_orm[campo]
                    clase_orm.create(nombre=valor)

        # LLeno ahora la tabla principal recorriendo las filas del csv
        for elem in df.values:
            tipo_etapa = Etapa.get(Etapa.nombre == elem[3])
            tipo_tipo = Tipo.get(Tipo.nombre == elem[4])
            tipo_area = AreaResponsable.get(AreaResponsable.nombre == elem[5])
            tipo_comuna = Comuna.get(Comuna.nombre == elem[8])
            tipo_barrio = Barrio.get(Barrio.nombre == elem[9])
            tipo_licitacion = LicitacionEmpresa.get(LicitacionEmpresa.nombre == elem[21])
            tipo_contratacion = ContratacionTipo.get(ContratacionTipo.nombre == elem[23])
            tipo_financiamiento = Financiamiento.get(Financiamiento.nombre == elem[35])
            try:
                Obra.create(
                    id=elem[0],
                    entorno=elem[1],
                    nombre=elem[2],
                    etapa=tipo_etapa,
                    tipo=tipo_tipo,
                    area_responsable=tipo_area,
                    descripcion=elem[6],
                    monto_contrato=elem[7],
                    comuna=tipo_comuna,
                    barrio=tipo_barrio,
                    direccion=elem[10],
                    lat=elem[11],
                    lng=elem[12],
                    fecha_inicio=elem[13],
                    fecha_fin_inicial=elem[14],
                    plazo_meses=elem[15],
                    porcentaje_avance=elem[16],
                    imagen_1=elem[17],
                    imagen_2=elem[18],
                    imagen_3=elem[19],
                    imagen_4=elem[20],
                    licitacion_oferta_empresa=tipo_licitacion,
                    licitacion_anio=elem[22],
                    contratacion_tipo=tipo_contratacion,
                    nro_contratacion=elem[24],
                    cuit_contratista=elem[25],
                    beneficiarios=elem[26],
                    mano_obra=elem[27],
                    compromiso=elem[28],
                    destacada=elem[29],
                    ba_elige=elem[30],
                    link_interno=elem[31],
                    pliego_descarga=elem[32],
                    expediente_numero=elem[33],
                    estudio_ambiental_descarga=elem[34],
                    financiamiento=tipo_financiamiento
                )
            except IntegrityError as e:
                print("Error al insertar un nuevo registro en la tabla viajes.", e)

        print("Datos cargados en las tablas exitosamente.")
        # Me aseguro de cerrar la conexion a la BBDD
        if not sqlite_db.is_closed():
            sqlite_db.close()

    @classmethod
    @abstractmethod
    def nueva_obra(cls):
        # Método para crear una nueva instancia de Obra con valores ingresados por teclado
        while True:
            try:
                new_entorno = input("Ingrese el entorno de la obra: ")
                new_nombre = input("Ingrese el nombre de la obra: ")
                new_descripcion = input("Ingrese la descripción de la obra: ")
                new_monto_contrato = float(input("Ingrese el monto del contrato: "))
                new_direccion = input("Ingrese la dirección de la obra: ")
                new_lat = input("Ingrese la latitud: ")
                new_lng = input("Ingrese la longitud: ")
                new_fecha_inicio = input("Ingrese la fecha de inicio (dd-mm-yyyy): ")
                new_fecha_fin_inicial = input("Ingrese la fecha de fin inicial (dd-mm-yyyy): ")
                new_plazo_meses = int(input("Ingrese el plazo en meses (entero): "))
                new_porcentaje_avance = float(input("Ingrese el porcentaje de avance de la obra: "))
                new_imagen_1 = "N/a"
                new_imagen_2 = "N/a"
                new_imagen_3 = "N/a"
                new_imagen_4 = "N/a"
                new_licitacion_anio = int(input("Ingrese el año de licitacion(ejemplo: 2024): "))
                new_nro_contratacion = input("Ingrese el número de contratación: ")
                new_cuit_contratista = input("Ingrese el CUIT del contratista: ")
                new_beneficiarios = input("Ingrese quienes seran los beneficiarios: ")
                new_mano_obra = int(input("Ingrese la cantidad de mano de obra (entero): "))
                new_compromiso = input("Ingrese el compromiso: ")
                new_destacada = input("Es una obra destacada? (si/no): ").lower()
                new_ba_elige = input("Es una obra de BA Elige? (si/no): ").lower()
                new_link_interno = "link no disponible"
                new_pliego_descarga = "link de pliego no disponible"
                new_expediente_numero = input("Ingrese el número del expediente: ")
                new_estudio_ambiental_descarga = "link de estudio ambiental no disponible"

                # Ingresar otros valores necesarios para crear una obra
                new_etapa_nombre = input("Ingrese la etapa de la obra: ").capitalize()
                try:
                    etapa = Etapa.get(Etapa.nombre == new_etapa_nombre)
                except DoesNotExist:
                    new_etapa_nombre = input("ETAPA NUEVA. Confirme la etapa de la obra: ").capitalize()
                    Etapa.create(nombre=new_etapa_nombre)
                    etapa = Etapa.get(Etapa.nombre == new_etapa_nombre)

                new_tipo_nombre = input("Ingrese el tipo de la obra: ").capitalize()
                try:
                    tipo = Tipo.get(Tipo.nombre == new_tipo_nombre)
                except DoesNotExist:
                    new_tipo_nombre = input("TIPO DE OBRA NUEVA. Confirme el tipo de la obra: ").capitalize()
                    Tipo.create(nombre=new_tipo_nombre)
                    tipo = Tipo.get(Tipo.nombre == new_tipo_nombre)

                new_area_responsable_nombre = input("Ingrese el área responsable de la obra: ").capitalize()
                try:
                    area_responsable = AreaResponsable.get(AreaResponsable.nombre == new_area_responsable_nombre)
                except DoesNotExist:
                    new_area_responsable_nombre = input("AREA RESPONSABLE NUEVO. Confirme el area de la obra: ").capitalize()
                    AreaResponsable.create(nombre=new_area_responsable_nombre)
                    area_responsable = AreaResponsable.get(AreaResponsable.nombre == new_area_responsable_nombre)

                new_comuna_numero = int(input("Ingrese la comuna de la obra: "))
                try:
                    comuna = Comuna.get(Comuna.nombre == new_comuna_numero)
                except DoesNotExist:
                    # En este caso como las comunas estan precargadas las 15, solo debe seleccionar la correcta
                    new_comuna_numero = input("Verifique la comuna ingresada (del 1 al 15)")
                    comuna = Comuna.get(Comuna.nombre == new_comuna_numero)

                new_barrio_nombre = input("Ingrese el barrio de la obra: ").capitalize()
                try:
                    barrio = Barrio.get(Barrio.nombre == new_barrio_nombre)
                except DoesNotExist:
                    new_barrio_nombre = input("BARRIO NUEVO INGRESADO. Confirme el barrio de la obra: ").capitalize()
                    Barrio.create(nombre = new_barrio_nombre)
                    barrio = Barrio.get(Barrio.nombre == new_barrio_nombre)

                new_licitacion_empresa_nombre = input("Ingrese la empresa de licitación: ").capitalize()
                try:
                    licitacion_empresa = LicitacionEmpresa.get(LicitacionEmpresa.nombre == new_licitacion_empresa_nombre)
                except DoesNotExist:
                    new_licitacion_empresa_nombre = input("LICITACION NUEVA INGRESADA. Confirme la empresa de licitación: ").capitalize()
                    LicitacionEmpresa.create(nombre = new_licitacion_empresa_nombre)
                    licitacion_empresa = LicitacionEmpresa.get(LicitacionEmpresa.nombre == new_licitacion_empresa_nombre)

                new_contratacion_tipo_nombre = input("Ingrese el tipo de contratación: ").capitalize()
                try:
                    contratacion_tipo = ContratacionTipo.get(ContratacionTipo.nombre == new_contratacion_tipo_nombre)
                except DoesNotExist:
                    new_contratacion_tipo_nombre = input("TIPO CONTRATACION NUEVO INGRESADO. Confirme el tipo de contratacion: ").capitalize()
                    ContratacionTipo.create(nombre = new_contratacion_tipo_nombre)
                    contratacion_tipo = ContratacionTipo.get(ContratacionTipo.nombre == new_contratacion_tipo_nombre)

                new_financiamiento_nombre = input("Ingrese el financiamiento de la obra: ").capitalize()
                try:
                    financiamiento = Financiamiento.get(Financiamiento.nombre == new_financiamiento_nombre)
                except DoesNotExist:
                    new_financiamiento_nombre = input("TIPO CONTRATACION NUEVO INGRESADO. Confirme el tipo de contratacion: ").capitalize()
                    Financiamiento.create(nombre = new_financiamiento_nombre)
                    Financiamiento.get(Financiamiento.nombre == new_financiamiento_nombre)
                    financiamiento = Financiamiento.get(Financiamiento.nombre == new_financiamiento_nombre)

                # Crear nueva instancia de Obra
                nueva_obra = Obra.create(
                    entorno=new_entorno,
                    nombre=new_nombre,
                    descripcion=new_descripcion,
                    monto_contrato=new_monto_contrato,
                    direccion=new_direccion,
                    lat=new_lat,
                    lng=new_lng,
                    fecha_inicio=new_fecha_inicio,
                    fecha_fin_inicial=new_fecha_fin_inicial,
                    plazo_meses=new_plazo_meses,
                    porcentaje_avance=new_porcentaje_avance,
                    imagen_1=new_imagen_1,
                    imagen_2=new_imagen_2,
                    imagen_3=new_imagen_3,
                    imagen_4=new_imagen_4,
                    licitacion_anio = new_licitacion_anio,
                    nro_contratacion=new_nro_contratacion,
                    cuit_contratista=new_cuit_contratista,
                    beneficiarios=new_beneficiarios,
                    mano_obra=new_mano_obra,
                    compromiso=new_compromiso,
                    destacada=new_destacada,
                    ba_elige=new_ba_elige,
                    link_interno=new_link_interno,
                    pliego_descarga=new_pliego_descarga,
                    expediente_numero=new_expediente_numero,
                    estudio_ambiental_descarga=new_estudio_ambiental_descarga,
                    etapa=etapa,
                    tipo=tipo,
                    area_responsable=area_responsable,
                    comuna=comuna,
                    barrio=barrio,
                    licitacion_oferta_empresa=licitacion_empresa,
                    contratacion_tipo=contratacion_tipo,
                    financiamiento=financiamiento
                )
                nueva_obra.save()
                return nueva_obra
            except DoesNotExist as e:
                print(f"Error: {e}. Intente nuevamente.")
            except Exception as e:
                print(f"Ocurrió un error: {e}. Intente nuevamente.")
            finally:
                if not sqlite_db.is_closed():
                    sqlite_db.close()

    @classmethod
    @abstractmethod
    def obtener_indicadores(cls):
        # Me aseguro que la conexión a la bbdd está abierta
        cls.conectar_db()
        try:
            # a) Listado de todas las areas responsables
            query = AreaResponsable.select(AreaResponsable.nombre).distinct().order_by(AreaResponsable.nombre)
            resultado_a = query.execute()
            print("Muestro las areas responsables: ")
            for area in resultado_a:
                print(f"* Nombre: {area.nombre}")

            # b) Listado de todos los tipos de Obra
            query = Tipo.select(Tipo.nombre).distinct().order_by(Tipo.nombre)
            resultado_b = query.execute()
            print("\nMuestro los Tipos de Obra: ")
            for tipo in resultado_b:
                print(f"* Nombre: {tipo.nombre}")

            # c) Cantidad de obras que se encuentran en cada etapa
            query = (Etapa.select(Etapa.nombre, fn.Count(Obra.id).alias('cantidad_obras'))
                     .join(Obra, on=(Obra.etapa == Etapa.id))
                     .group_by(Etapa.id))

            resultado_c = [(etapa.nombre, etapa.cantidad_obras) for etapa in query]

            print("\nMuestro la cantidad de obras por etapa: ")
            for etapa, cantidad in resultado_c:
                print(f'Etapa: {etapa} | Cantidad de obras: {cantidad}')

            # d) Cantidad de obras y monto total de inversion por tipo de Obra
            query_tipo_obra = (Tipo
                   .select(Tipo.nombre,
                           fn.Count(Obra.id).alias('cantidad_obras'),
                           fn.Sum(Obra.monto_contrato).alias('monto_total_inversion'))
                   .join(Obra, on=(Obra.tipo == Tipo.id))
                   .group_by(Tipo.id))

            resultado_d = [(tipo.nombre, tipo.cantidad_obras, tipo.monto_total_inversion) for tipo in query_tipo_obra]

            print("\nMuestro la cantidad de obras y monto total de inversion por tipo de obra: ")
            for tipo in resultado_d:
                print(f"Tipo de Obra: {tipo[0]}")
                print(f"Cantidad de Obras: {tipo[1]}")
                print(f"Monto Total de Inversión: ${tipo[2]}")
                print("----------------------")

            # e) Listado de todos los barrios pertencientes a las comunas 1, 2, 3
            comunas_seleccionadas = [1, 2, 3]
            query = (Barrio
                     .select(Barrio.nombre, Comuna.nombre)
                     .join(Obra)
                     .join(Comuna, on=(Obra.comuna == Comuna.id))
                     .where(Comuna.nombre.in_(comunas_seleccionadas))
                    .distinct())
            # print(query)
        
            resultado_e = [(barrio.nombre) for barrio in query]
            print("\nListado de barrios sin repetir en comunas 1, 2, 3:")
            for barrio_nombre in resultado_e:
                print(f"Barrio: {barrio_nombre}")

            # f) Cantidad de Obras finalizadas y su monto de inversion en la comunan 1
            etapa_finalizada = "Finalizada"

            query = (Obra
                    .select(fn.COUNT(Obra.id).alias('cantidad_obras'), fn.SUM(Obra.monto_contrato).alias('monto_inversion'))
                    .join(Comuna, on=(Obra.comuna == Comuna.id))
                    .join(Etapa, on=(Obra.etapa == Etapa.id))
                    .where((Comuna.nombre == "1") & (Etapa.nombre == etapa_finalizada))
                    .group_by(Comuna.id))
            # print(query)

            resultado_f = query.dicts().get()
            print("\nCantidad de Obras finalizadas y su monto de inversion en la comunan 1")
            print(f"* Cantidad de obras finalizadas en la comuna 1: {resultado_f['cantidad_obras']}")
            print(f"* Monto total de inversión en la comuna 1: {resultado_f['monto_inversion']}")

            # g) Cantidad de obras finalizadas en un plazo menor a 24 meses
            etapa_finalizada = "Finalizada"

            # Consulta en Peewee ORM
            query = (Obra
                    .select(fn.COUNT(Obra.id).alias('cantidad_obras'))
                    .join(Etapa, on=(Obra.etapa == Etapa.id))
                    .where((Etapa.nombre == etapa_finalizada) & (Obra.plazo_meses < 24)
                    ))
            # print(query)

            resultado_g = query.dicts().get()

            print(f"\n* Cantidad de obras finalizadas en un plazo menor a 24 meses: {resultado_g['cantidad_obras']}")

            # h) Porcentaje total de obras finalizadas
            total_obras = Obra.select().count()
            obras_finalizadas = Obra.select().where(Obra.porcentaje_avance == 100).count()

            porcentaje_finalizadas = (obras_finalizadas / total_obras) * 100 if total_obras > 0 else 0

            print(f"\n* Porcentaje total de obras finalizadas: {porcentaje_finalizadas:.2f}%")

            # i) Cantidad total de mano de obra empleada
            total_mano_obra = (Obra
                   .select(fn.SUM(Obra.mano_obra).alias('total_mano_obra'))
                   .where(Obra.mano_obra > 0)
                   .scalar())

            print(f"\n* Cantidad total de mano de obra empleada: {total_mano_obra}")

            # j) Monto total de inversion
            total_inversion = (Obra
                   .select(fn.SUM(Obra.monto_contrato).alias('total_inversion'))
                   .where(Obra.monto_contrato > 0)
                   .scalar())

            print(f"\n* Monto total de inversión en obras: {total_inversion:.2f}")

                
        except OperationalError as e:
            print("Error al obtener datos:", e)
        except AttributeError as e:
            print(f"Error de atributo: {e}")
        finally:
            if not sqlite_db.is_closed():
                sqlite_db.close()


def menu():
    print("Bienvenido al Sistema de Gestion de obra")
    print("1) Empezar un nuevo proyecto")
    print("2)")


if __name__ == "__main__":
    print("estoy iniciando el programa")
    class Implementacion(GestionarObra):
        pass

    data_set = Implementacion.extraer_datos()
    Implementacion.conectar_db()
    Implementacion.mapear_orm()
    data_set = Implementacion.limpiar_datos(data_set)
    Implementacion.cargar_datos(data_set)
    Implementacion.nueva_obra()
    Implementacion.obtener_indicadores()
    # proyecto_nuevo = Obra()
    # # proyecto_nuevo.nuevo_proyecto()
    # nueva_obra = GestionarObra.nueva_obra()
    # nueva_obra.save()