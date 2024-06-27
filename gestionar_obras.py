from abc import ABC, abstractmethod
from abc import ABCMeta
import pandas as pd

df = pd.read_csv('observatorio-de-obras-urbanas.csv')

class GestionarObra(metaclass=ABCMeta):
    @abstractmethod
    def extraer_datos(self):
        pass

    @abstractmethod
    def conectar_db(self):
        pass

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
