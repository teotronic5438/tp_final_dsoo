from abc import ABC, abstractmethod
from abc import ABCMeta
#import pandas as pd
import sqlite3

class GestionarObra(metaclass=ABCMeta):
    @abstractmethod
    def extraer_datos(self):
        #df = pd.read_csv('observatorio-de-obras-urbanas.csv', header= None)
        pass

    @abstractmethod
    def conectar_db(self):
        #una idea de como creo que se conecta a la base de datos
        try:
            conn = sqlite3.connect("obras_urbanas.db")
        except:
            print("No se pudo conectar a la base")
        else:
            print("Se conecto correctamente a la base")
        return conn

    @abstractmethod
    def mapear_orm(self):
        pass

    @abstractmethod
    def limpiar_datos(self):
        pass

    @abstractmethod
    def cargar_datos(self):
        pass

    @abstractmethod
    def nueva_obra(self):
        pass

    @abstractmethod
    def obtener_indicadores(self):
        pass