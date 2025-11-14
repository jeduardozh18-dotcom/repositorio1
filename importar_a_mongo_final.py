import pandas as pd
from pymongo import MongoClient

class MongoDBHandler:
    def __init__(self, uri="mongodb://localhost:27017/", db_name="mi_exel"):
        """Inicializa la conexión con MongoDB"""
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def get_collection(self, collection_name):
        """Devuelve una colección de la base de datos"""
        return self.db[collection_name]


class ExcelToMongo:
    def __init__(self, mongo_handler, collection_name="diccionario_de_datos"):
        """Inicializa con el manejador de Mongo y la colección destino"""
        self.collection = mongo_handler.get_collection(collection_name)

    def leer_excel(self, ruta, nombre_hoja=None):
        """Lee un archivo Excel y conserva los valores exactamente como están"""
        df = pd.read_excel(
            ruta,
            sheet_name=nombre_hoja,
            keep_default_na=False  # conserva las celdas vacías como ""
        )
        return df

    def exportar_excel(self, ruta, nombre_hoja=None):
        """Lee un Excel y exporta su contenido a MongoDB tal cual"""
        df = self.leer_excel(ruta, nombre_hoja)

        if isinstance(df, dict):  # Varias hojas
            for hoja, df_hoja in df.items():
                datos = df_hoja.to_dict(orient="records")
                if datos:
                    self.collection.insert_many(datos)
                    print(f"Archivo {ruta} - Hoja '{hoja}' exportado con {len(datos)} filas.")
        else:  # Solo una hoja
            datos = df.to_dict(orient="records")
            if datos:
                self.collection.insert_many(datos)
                print(f"Archivo {ruta} exportado con {len(datos)} filas.")


if __name__ == "__main__":
    mongo_handler = MongoDBHandler(db_name="Construccion")
    exportador = ExcelToMongo(mongo_handler, collection_name="diccionario_de_datos")

    rutas = [
        r"C:\Users\LENOVO PREMIUM\Documents\Inegi\Construccion\diccionario_de_datos\denue_diccionario_de_datos.csv"
    ]

    for ruta in rutas:
        exportador.exportar_excel(ruta)