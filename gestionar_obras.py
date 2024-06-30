# Cargo la info del modelo de BBDD
from modelo_orm import *
# Ahora cargo los modulos que preciso para este modulo
from abc import ABC, abstractmethod
import pandas as pd

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
        try:
            sqlite_db.connect()
        except OperationalError as e:
            print("Error al conectar con la BD.", e)
            exit()
        
    @classmethod
    @abstractmethod
    def mapear_orm(cls):
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
            
        # Cerrar la conexión a la base de datos cuando hayamos terminado
        sqlite_db.close()
    
    @classmethod
    @abstractmethod
    def limpiar_datos(cls, df):
        # 1) Primero vamos a limpiar los campos vacios de las columnas necesarias para los indicadores
        # nota a memoria: mano_obra me baja de 1325 a 335 registros
        columnas_a_verificar = ['etapa', 'tipo', 'area_responsable', 'monto_contrato', 'comuna', 'barrio', 'plazo_meses', 'porcentaje_avance', 'mano_obra', 'fecha_inicio', 'fecha_fin_inicial']

        #Eliminar valores NA o NaN (nulos o no disponibles) de las columnas usadas para los indicadores
        df.dropna(subset=columnas_a_verificar, axis = 0, inplace = True)

        # 2) Hago la conversion de las columnas numéricas segun tipo y relleno las demas
        conversiones = {
            "id": "integer",
            "monto_contrato": "float",
            "comuna": "integer",
            "plazo_meses": "integer",
            "porcentaje_avance": "integer",
            "licitacion_anio": "integer",
            "mano_obra": "integer"
        }

        # 2.1) Itero las columnas del DataFrame
        for columna in df:
            # Convierto las columnas numéricas según el tipo especificado en conversiones
            if columna in conversiones:
                df[columna] = pd.to_numeric(df[columna], errors='coerce', downcast=conversiones[columna])

            # Relleno los valores nulos (NaN) con el contenido N/A o no disponible (consultado con el profesor)
            if columna in conversiones.keys():
                df[columna].fillna(-1, inplace=True)
            elif columna not in columnas_a_verificar:
                if df[columna].dtype == "object":
                    df[columna].fillna("N/A", inplace=True)

        # Forzar la conversión de plazo_meses y mano de obra a tipo entero (pandas los hace float por tener textos)
        df["plazo_meses"] = df["plazo_meses"].astype(int)
        df["mano_obra"] = df["mano_obra"].astype(int)

        # Muestra el dataframe limpio
        print(df['financiamiento'].unique())
        df.to_csv('./csv_limpiado.csv', index=False, sep=';')
        return df

'''
    nota a memoria:     campo='Urbanización'.encode('ISO-8859-1').decode('utf-8'), antes de cargar bbdd
    es decir al leer el csv lo leo como ISO-8859-1 pero al cargarlo a la base de datos lo paso a UTF-8
'''
'''

    @classmethod
    @abstractmethod
    def limpiar_datos(cls, df):
        # Método para limpiar los datos nulos y no accesibles del Dataframe
        df_clean = df.dropna()
        return df_clean

    @classmethod
    @abstractmethod
    def cargar_datos(cls, df):
        # Método para persistir los datos de las obras en la base de datos relacional SQLite
        for _, row in df.iterrows():
            # Ejemplo de creación de instancia de Obra y guardado en la base de datos
            obra = Obra.create(
                entorno=row['entorno'],
                nombre=row['nombre'],
                # Asignar otros campos según la estructura de tu dataframe y modelos ORM
            )
            obra.save()

    @classmethod
    @abstractmethod
    def nueva_obra(cls):
        # Método para crear una nueva instancia de Obra con valores ingresados por teclado
        while True:
            try:
                entorno = input("Ingrese el entorno de la obra: ")
                nombre = input("Ingrese el nombre de la obra: ")
                # Ingresar otros valores necesarios para crear una obra
                # Ejemplo de búsqueda de valores relacionados (foreign key)
                etapa_nombre = input("Ingrese la etapa de la obra: ")
                etapa = Etapa.get(Etapa.nombre == etapa_nombre)
                # Crear nueva instancia de Obra
                nueva_obra = Obra.create(
                    entorno=entorno,
                    nombre=nombre,
                    etapa=etapa,
                    # Asignar otros campos según la estructura de tu modelo ORM
                )
                nueva_obra.save()
                return nueva_obra
            except DoesNotExist:
                print(f"No existe una etapa con el nombre '{etapa_nombre}'. Intente nuevamente.")

    @classmethod
    @abstractmethod
    def obtener_indicadores(cls):
        # Método para obtener indicadores de las obras existentes en la base de datos SQLite
        obras = Obra.select()
        # Ejemplo de obtener indicadores
        total_obras = obras.count()
        # Puedes retornar cualquier indicador relevante
        return total_obras

'''

if __name__ == "__main__":
    print("estoy iniciando el programa")
    class Implementacion(GestionarObra):
        pass

    data_set = Implementacion.extraer_datos()
    Implementacion.conectar_db()
    Implementacion.mapear_orm()
    data_set = Implementacion.limpiar_datos(data_set)
