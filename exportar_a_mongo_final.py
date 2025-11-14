import pandas as pd
import numpy as np
from pymongo import MongoClient
from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Any, List


# -------------------------------
#  Conexión a MongoDB
# -------------------------------
class MongoDBHandler:
    def __init__(self, uri="mongodb://localhost:27017/", db_name="exel3"):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def get_collection(self, collection_name):
        return self.db[collection_name]


# ------------------------------------------
#  Validación de tipos con Pydantic
# ------------------------------------------
class ValidadorCampo(BaseModel):
    valor: Any

    @field_validator("valor", mode="before")
    @classmethod
    def detectar_tipo(cls, v):
        if pd.isna(v) or v == "":
            return None

        try:
            float(v)
            return float(v)
        except:
            pass

        try:
            if isinstance(v, datetime):
                return v
            pd.to_datetime(v)
            return pd.to_datetime(v)
        except:
            pass

        return str(v)


# --------------------------------------------------------
#  Clase principal para validación y exportación
# --------------------------------------------------------
class MongoToExcelValidator:
    def __init__(self, mongo_handler, collection_name):
        self.collection = mongo_handler.get_collection(collection_name)

    def obtener_datos(self):
        datos = list(self.collection.find({}, {"_id": 0}))
        return pd.DataFrame(datos)

    def detectar_tipo_predominante(self, serie):
        tipos = {"numero": 0, "fecha": 0, "texto": 0}

        for valor in serie:
            validado = ValidadorCampo(valor=valor).valor
            if isinstance(validado, (int, float)):
                tipos["numero"] += 1
            elif isinstance(validado, (datetime, pd.Timestamp)):
                tipos["fecha"] += 1
            elif isinstance(validado, str):
                tipos["texto"] += 1

        total = len(serie)
        if total == 0:
            return "texto"

        for tipo, cuenta in tipos.items():
            if cuenta / total >= 0.7:
                return tipo

        return "texto"

    def convertir_y_rellenar(self, df):
        df_resultado = df.copy()

        for columna in df.columns:
            tipo = self.detectar_tipo_predominante(df[columna])
            print(f"→ Columna '{columna}' detectada como tipo {tipo}")

            if tipo == "numero":
                df_resultado[columna] = pd.to_numeric(df[columna], errors="coerce")
                df_resultado[columna] = df_resultado[columna].fillna(0)

            elif tipo == "fecha":
                df_resultado[columna] = pd.to_datetime(df[columna], errors="coerce")
                df_resultado[columna] = df_resultado[columna].fillna("")

            else:  # texto
                df_resultado[columna] = df[columna].astype(str)
                df_resultado[columna].replace(["nan", "NaT", "None"], "", inplace=True)
                df_resultado[columna] = df_resultado[columna].replace("", "sin datos")

        return df_resultado

    # --------------------------------------------------------
    #  Tabla dinámica configurable
    # --------------------------------------------------------
    def crear_tabla_dinamica(self, df, columnas_indices: List[str], columnas_valores: List[str], funciones_agregacion: List[str]):
        # Validar que existan las columnas
        for col in columnas_indices + columnas_valores:
            if col not in df.columns:
                raise ValueError(f" Falta la columna requerida: {col}")

        # Reemplazar vacíos por "sin datos" en los índices
        for col in columnas_indices:
            df[col] = df[col].replace("", "sin datos")

        # Crear tabla dinámica con parámetros
        tabla_pivot = pd.pivot_table(
            df,
            index=columnas_indices,
            values=columnas_valores,
            aggfunc=funciones_agregacion,
            fill_value=0,
            margins=True,
            margins_name="Total General"
        )

        tabla_pivot = tabla_pivot.sort_index()
        return tabla_pivot

    # --------------------------------------------------------
    #  Exportar ambas hojas
    # --------------------------------------------------------
    def exportar_excel(self, ruta_salida, columnas_indices, columnas_valores, funciones_agregacion):
        df = self.obtener_datos()
        print(f"\nDatos obtenidos desde MongoDB: {len(df)} filas, {len(df.columns)} columnas.")

        df_validado = self.convertir_y_rellenar(df)

        try:
            tabla_pivot = self.crear_tabla_dinamica(df_validado, columnas_indices, columnas_valores, funciones_agregacion)
        except ValueError as e:
            print(str(e))
            tabla_pivot = pd.DataFrame()

        with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
            df_validado.to_excel(writer, index=False, sheet_name="Datos_Validados")
            tabla_pivot.to_excel(writer, sheet_name="Tabla_Dinamica")

        print(f"\n Archivo Excel exportado correctamente a:\n{ruta_salida}")
        print("→ Hoja 1: Datos_Validados")
        print("→ Hoja 2: Tabla_Dinamica")


# -------------------------------
#  Ejecución del programa
# -------------------------------
if __name__ == "__main__":
    mongo_handler = MongoDBHandler(db_name="exel3")
    validador = MongoToExcelValidator(mongo_handler, collection_name="tablas_exel")

    #  Configuración dinámica de la tabla
    columnas_indices = ["Comprobante Metodo Pago", "Comprobante Moneda"]
    columnas_valores = ["Comprobante Subtotal Descuento Mxn"]
    funciones_agregacion = ["count", "sum"]  # Puedes usar ["sum"], ["max"], ["mean"], etc.

    ruta_salida = r"C:\Users\LENOVO PREMIUM\Documents\exportado_validado_con_tabla_configurable.xlsx"

    validador.exportar_excel(
        ruta_salida,
        columnas_indices=columnas_indices,
        columnas_valores=columnas_valores,
        funciones_agregacion=funciones_agregacion
    )