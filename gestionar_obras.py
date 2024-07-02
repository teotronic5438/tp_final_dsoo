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
    def verificando_texto(cls, texto):
        while True:
            # Verifico si el texto ingresado no está vacío
            if texto.strip():  
                return texto.capitalize()
            else:
                print("El texto no puede estar vacio")
                texto = input("Ingreselo nuevamente: ")
    @classmethod
    @abstractmethod
    def verificando_entero(cls, numero):
        while True:
            try:
                opcion = int(numero)
                return opcion
            except ValueError:
                print("El valor ingresado no es un numero.")
                numero = input("Ingreselo nuevamente: ")
    @classmethod
    @abstractmethod
    def verificando_flotante(cls, numero):
        while True:
            try:
                opcion = float(numero)
                return opcion
            except ValueError:
                print("El valor ingresado no es un numero.")
                numero = input("Ingreselo nuevamente: ")

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

        # NOTA A PROFESOR: mano_obra me baja de 1325 aprox 301 registros y elimina una cantidad importante de datos
        # datos nulos, textos, 0 y vacios
        # Columnas a verificar y limpiar
        columnas_a_verificar = ['etapa', 'tipo', 'area_responsable', 'monto_contrato', 'comuna', 'barrio', 'plazo_meses',
                                'porcentaje_avance', 'licitacion_anio', 'fecha_inicio', 'fecha_fin_inicial', 'mano_obra']
        # print(df)
        # Eliminar filas con NaN en columnas a verificar
        df.dropna(subset=columnas_a_verificar, inplace=True)


        # Columnas numéricas que deben limpiarse de texto y valores cero
        columnas_numericas = ['monto_contrato', 'comuna', 'plazo_meses', 'porcentaje_avance', 'licitacion_anio', 'mano_obra']

        # Convertir columnas específicas a tipo numérico, forzando errores como NaN
        for col in columnas_numericas:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

        # Eliminar filas con NaN o valores cero en columnas numéricas
        df = df[~(df[columnas_numericas].isna() | (df[columnas_numericas] == 0)).any(axis=1)]



        # Conversion forzada, ya no es necesario texto en numeros limpiado
        df.loc[:, "plazo_meses"] = df["plazo_meses"].astype(int)  # NO USADO POR NO FUNCIONAR
        # df["plazo_meses"] = df["plazo_meses"].astype(int)
        df.loc[:, "mano_obra"] = df["mano_obra"].astype(int)
        # df["mano_obra"] = df["mano_obra"].astype(int)

        # 2) Hago el rellenado de datos que no quiero eliminar
        conversiones = {
            "id": "integer",
            "monto_contrato": "float",
            "comuna": "integer",
            "plazo_meses": "integer",
            "porcentaje_avance": "integer",
            "licitacion_anio": "integer",
            "mano_obra": "integer"
        }
        for columna in df.columns:
            if columna in conversiones.keys():
                df.loc[:, columna] = df[columna].fillna(0)
            elif columna not in columnas_a_verificar:
                if df[columna].dtype == "object":
                    df.loc[:, columna] = df[columna].fillna("no disponible")

        # print(df['plazo_meses'])
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
        entorno = input("Ingrese el entorno de la obra: ").capitalize()
        entorno = cls.verificando_texto(entorno)
        nombre = input("Ingrese el nombre de la obra: ").capitalize()
        nombre = cls.verificando_texto(nombre)
        etapa = input('Ingrese la etapa de la obra: ').capitalize()             #FUNCION # tabla
        etapa = cls.verificando_texto(etapa)
        tipo = input("Ingrese el tipo de obra: ").capitalize()                  #FUNCION # tabla
        tipo = cls.verificando_texto(tipo)
        area_responsable = input("Ingrese el área responsable: ").capitalize()  #FUNCION # tabla
        area_responsable = cls.verificando_texto(area_responsable)
        descripcion = input("Ingrese la descripción: ").capitalize()
        descripcion = cls.verificando_texto(descripcion)
        monto_contrato = input("Ingrese el monto del contrato: ")
        monto_contrato = cls.verificando_flotante(monto_contrato)
        # Validación de Comuna      # tabla
        while True:
            comuna_nombre = input("Ingrese la comuna: ").capitalize()
            comuna_nombre = cls.verificando_entero(comuna_nombre)
            comuna, created = Comuna.get_or_create(nombre=comuna_nombre)
            if created:
                print(f"Comuna '{comuna_nombre}' creada.")
            break
        # Validación de Barrio                                                  #FUNCION# tabla
        while True:
            barrio_nombre = input("Ingrese el barrio: ").capitalize()
            barrio_nombre = cls.verificando_texto(barrio_nombre)
            barrio, created = Barrio.get_or_create(nombre=barrio_nombre)
            if created:
                print(f"Barrio '{barrio_nombre}' creado.")
            break
        direccion = input("Ingrese la dirección: ").capitalize()
        direccion = cls.verificando_texto(direccion)
        lat = input("Ingrese la latitud: ").capitalize()
        lat = cls.verificando_texto(lat)
        lng = input("Ingrese la longitud: ").capitalize()
        lng = cls.verificando_texto(lng)
        fecha_inicio = input("Ingrese la fecha de inicio (dd-mm-yyyy): ")                #FUNCION
        fecha_inicio = cls.verificando_texto(fecha_inicio)  # nota ver fecha EXPREG
        fecha_fin_inicial = input("Ingrese la fecha de fin inicial (dd-mm-yyyy): ")     #FUNCION
        fecha_fin_inicial = cls.verificando_texto(fecha_fin_inicial)
        plazo_meses = input("Ingrese el plazo en meses (entero): ")                #FUNCION
        plazo_meses = cls.verificando_entero(plazo_meses)
        porcentaje_avance = input("Ingrese el porcentaje de avance de la obra: ")  #FUNCION
        porcentaje_avance = cls.verificando_flotante(porcentaje_avance)
        imagen_1 = "N/a"
        imagen_2 = "N/a"
        imagen_3 = "N/a"
        imagen_4 = "N/a"
        # Validación de LicitacionEmpresa                                               #FUNCION
        while True:
            empresa_nombre = input("Ingrese la empresa de la licitación: ").capitalize()
            empresa_nombre = cls.verificando_texto(empresa_nombre)
            empresa, created = LicitacionEmpresa.get_or_create(nombre=empresa_nombre)
            if created:
                print(f"Empresa '{empresa_nombre}' creada.")
            break
        licitacion_anio = input("Ingrese el año de licitacion(ejemplo: 2024): ")
        licitacion_anio = cls.verificando_entero(licitacion_anio)

        # Validación de ContratacionTipo                                                #FUNCION
        while True:
            contratacion_tipo_nombre = input("Ingrese el tipo de contratación: ").capitalize()
            contratacion_tipo_nombre = cls.verificando_texto(contratacion_tipo_nombre)
            contratacion_tipo, created = ContratacionTipo.get_or_create(nombre=contratacion_tipo_nombre)
            if created:
                print(f"Tipo de contratación '{contratacion_tipo_nombre}' creado.")
            break
        nro_contratacion = input("Ingrese el número de contratación: ").capitalize()
        nro_contratacion = cls.verificando_texto(nro_contratacion)
        cuit_contratista = input("Ingrese el CUIT del contratista: ").capitalize()
        cuit_contratista = cls.verificando_texto(cuit_contratista)      # pasar a numero
        beneficiarios = input("Ingrese quienes seran los beneficiarios: ").capitalize()
        beneficiarios = cls.verificando_texto(beneficiarios)    
        mano_obra = input("Ingrese la cantidad de mano de obra (entero): ")            #FUNCION
        mano_obra = cls.verificando_entero(mano_obra)
        compromiso = input("Ingrese el compromiso: ").capitalize()
        compromiso = cls.verificando_texto(compromiso)
        destacada = input("Es una obra destacada? (si/no): ").lower()                       #FUNCION
        destacada = cls.verificando_texto(destacada)
        ba_elige = input("Es una obra de BA Elige? (si/no): ").lower()
        ba_elige = cls.verificando_texto(ba_elige)
        link_interno = "link no disponible"
        pliego_descarga = "link de pliego no disponible"
        expediente_numero = input("Ingrese el número del expediente: ").capitalize()        #FUNCION
        expediente_numero = cls.verificando_texto(expediente_numero)
        estudio_ambiental_descarga = "link de estudio ambiental no disponible"
        financiamiento = input("Ingrese la empresa que financia: ").capitalize()            #FUNCION
        financiamiento = cls.verificando_texto(financiamiento)

        # Creación de la nueva obra
        nueva_obra = Obra.create(
            entorno=entorno,
            nombre=nombre,
            etapa=etapa,
            tipo=tipo,
            area_responsable=area_responsable,
            descripcion=descripcion,
            monto_contrato=monto_contrato,
            comuna=comuna,
            barrio=barrio,
            direccion=direccion,
            lat=lat,
            lng=lng,
            fecha_inicio=fecha_inicio,
            fecha_fin_inicial=fecha_fin_inicial,
            plazo_meses=plazo_meses,
            porcentaje_avance=porcentaje_avance,
            imagen_1=imagen_1,
            imagen_2=imagen_2,
            imagen_3=imagen_3,
            imagen_4=imagen_4,
            licitacion_oferta_empresa=empresa,
            licitacion_anio=licitacion_anio,
            contratacion_tipo=contratacion_tipo,
            nro_contratacion=nro_contratacion,
            cuit_contratista=cuit_contratista,
            beneficiarios=beneficiarios,
            mano_obra=mano_obra,
            compromiso=compromiso,
            destacada=destacada,
            ba_elige=ba_elige,
            link_interno=link_interno,
            pliego_descarga=pliego_descarga,
            expediente_numero=expediente_numero,
            estudio_ambiental_descarga=estudio_ambiental_descarga,
            financiamiento=financiamiento
        )
        nueva_obra.save()
        return nueva_obra

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
    Implementacion.obtener_indicadores()
    # Implementacion.nueva_obra()
    proyecto_nuevo = GestionarObra.nueva_obra()
    proyecto_nuevo.nuevo_proyecto(proyecto_nuevo)
