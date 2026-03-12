from dolar import obtener_y_guardar_dolar
from datetime import datetime

def api_get(event, context):
    print("Lambda ejecutada")
    result = obtener_y_guardar_dolar()
    return result
