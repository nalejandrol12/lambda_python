import pymysql
import json
import base64
import boto3
import decimal
from datetime import datetime
from zoneinfo import ZoneInfo
from pymysql.err import IntegrityError

def get_secret():
    secret_name = "proyect-prd"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager',region_name=region_name)

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    else:
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return json.loads(decoded_binary_secret)

def default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    raise TypeError(f'Type {type(obj)} not serializable')

def validar_etapa2(cursor, id_archivo_digital):
    sql_validation_etapa2 = """
        SELECT COUNT(*) AS cantidad FROM Documentos d, Colas_etapa2 ci
        WHERE d.id = ci.id_documento
        AND d.id_archivo_digital = %s
        AND (ci.id_estado = 1 OR ci.id_estado = 5)
    """
    cursor.execute(sql_validation_etapa2, (id_archivo_digital,))
    existe_etapa2 = cursor.fetchone()

    return bool(existe_etapa2['cantidad'] > 0)


def validar_etapa2_finalizado(cursor, id_archivo_digital):
    sql_validation_etapa2 = """
        SELECT COUNT(*) AS cantidad FROM Documentos d, Colas_etapa2 ci
        WHERE d.id = ci.id_documento
        AND d.id_archivo_digital = %s
        AND ci.id_estado = 4
    """
    cursor.execute(sql_validation_etapa2, (id_archivo_digital,))
    existe_etapa2 = cursor.fetchone()

    return bool(existe_etapa2['cantidad'] > 0)

def obtener_id_proceso(cursor, id_archivo_digital):
    sql_obtener_id_proceso = "SELECT ad.id_proceso FROM Archivos_digitales ad WHERE ad.id = %s"
    cursor.execute(sql_obtener_id_proceso, (id_archivo_digital,))
    obtener_id_proceso = cursor.fetchone()
    id_proceso = 0
    if obtener_id_proceso:
        id_proceso = obtener_id_proceso

    return id_proceso

def obtener_id_documentos_lote(cursor, id_archivo_digital):
    sql_obtener_id_documentos = "SELECT d.id FROM Documentos d WHERE d.id_archivo_digital = %s AND d.estado = 1"
    cursor.execute(sql_obtener_id_documentos, (id_archivo_digital,))
    lista_id_documento = cursor.fetchall()
    ids_documentos = [item["id"] for item in lista_id_documento]
    return ids_documentos

def cambio_estado_lote_documento(cursor, params):
    ids_documento = params["ids_documento"]
    id_archivo_digital = params["id_archivo_digital"]

    if not ids_documento:
        ids_documento = obtener_id_documentos_lote(cursor, id_archivo_digital)

    if ids_documento:
        timestamp_str = datetime.now(ZoneInfo("America/Bogota")).strftime('%Y-%m-%d %H:%M:%S')
        update_estado_archivo_sql = """
            UPDATE Archivos_digitales ad SET ad.id_estado = 4, ad.fecha_fin_proceso = %s WHERE ad.id = %s
        """
        cursor.execute(update_estado_archivo_sql, (timestamp_str, id_archivo_digital))

        if id_archivo_digital:
            for item in ids_documento:
                update_estado_documento = """
                    UPDATE Documentos d SET estado = 4, d.fecha_modifica = %s WHERE d.id = %s
                """
                cursor.execute(update_estado_documento, (timestamp_str, item))
    else:
        print(f"Fallo [cambio_estado_lote_documento] - id_lote {id_archivo_digital}")

def existe_cola_etapa4(cursor, id_documento):
    sql_select_documento = """
        SELECT COUNT(*) AS cantidad FROM Colas_etapa4 ct WHERE ct.id_documento = %s
    """
    cursor.execute(sql_select_documento, (id_documento,))
    cola_etapa4 = cursor.fetchone()
    return bool(cola_etapa4['cantidad'] > 0)

def insertar_colas_etapa4(cursor, params):
    id_archivo_digital = params["id_archivo_digital"]
    id_proceso = obtener_id_proceso(cursor, id_archivo_digital)

    if id_proceso["id_proceso"] > 0:
        sql_obtener_documentos = """
            SELECT d.id FROM Documentos d
            INNER JOIN Tipos_documental_procesos tdp ON d.id_tipo_documental = tdp.id_tipo_documental
            INNER JOIN Templates ON t.id_tipo_documental_proceso = tdp.id
            WHERE tdp.id_proceso = %s AND d.id_archivo_digital = %s
            AND tdp.id_tipo_documental = d.id_tipo_documental
            AND d.estado NOT IN (7, 8) GROUP BY d.id ORDER BY d.id
        """
        cursor.execute(sql_obtener_documentos, (id_proceso["id_proceso"],id_archivo_digital))
        documentos = cursor.fetchall()

        sw = 0

        if documentos:
            for documento in documentos:
                timestamp_str = datetime.now(ZoneInfo("America/Bogota")).strftime('%Y-%m-%d %H:%M:%S')
                existe = existe_cola_etapa4(cursor, documento["id"])
                if not existe:
                    sql_colas_etapa4 = """
                        INSERT INTO Colas_etapa4(id_usuario, id_documento, id_estado, fecha_inicio_proceso, 
                        fecha_fin_proceso, fecha_llegada, fecha_eliminacion, fecha_creacion)
                        VALUES (NULL, %s, 5, NULL, NULL, %s, NULL, %s)
                    """
                    cursor.execute(sql_colas_etapa4, (documento["id"], timestamp_str, timestamp_str))
                    sw = 1

            if sw == 0:
                cambio_estado_lote_documento(cursor, params)
        else:
            cambio_estado_lote_documento(cursor, params)
    else:
        raise Exception(f"[insertar_colas_etapa4] id lote {id_archivo_digital}")

def insertar_colas_etapa2(cursor, params):
    ids_documento = params["ids_documento"]
    id_archivo_digital = params["id_archivo_digital"]

    if not ids_documento:
        ids_documento = obtener_id_documentos_lote(cursor, id_archivo_digital)

    if ids_documento:
        for id_documento in ids_documento:
            timestamp_str = datetime.now(ZoneInfo("America/Bogota")).strftime('%Y-%m-%d %H:%M:%S')

            sql_colas_etapa2 = """
                INSERT INTO Colas_etapa2 (id_usuario, id_documento, id_estado, fecha_inicio_proceso, 
                fecha_fin_proceso, fecha_llegada, fecha_eliminacion) VALUES (NULL, %s, 5, NULL, NULL, %s, NULL)
            """

            cursor.execute(sql_colas_etapa2, (id_documento, timestamp_str))
    else:
        raise Exception(f"[insertar_colas_etapa2] id lote {id_archivo_digital}")

def validar_cantidad_etapa3(cursor, id_proceso):
    sql_obtener_documentos = """
        SELECT COUNT(*) AS cantidad FROM Actividades_procesos ap WHERE ap.id_actividad = 5 AND ap.id_proceso = %s
    """
    cursor.execute(sql_obtener_documentos, (id_proceso,))
    cantidad = cursor.fetchone()

    query = ""

    if cantidad["cantidad"] != 0:
        query = """
            SELECT d.id FROM Documentos d
            INNER JOIN Tipos_documental_procesos tdp ON d.id_tipo_documental = tdp.id_tipo_documental
            INNER JOIN Templates_mesas tm ON tm.id_tipo_documental_proceso = tdp.id
            WHERE tdp.id_proceso = %s AND d.id_archivo_digital = %s AND tdp.mesa = 1
            AND d.id_tipo_documental = tdp.id_tipo_documental
            AND d.estado NOT IN (7, 8) GROUP BY d.id
        """
    else:
        query = """
            SELECT d.id FROM Documentos d
            INNER JOIN Tipos_documental_procesos tdp ON d.id_tipo_documental = tdp.id_tipo_documental
            INNER JOIN Templates_mesas tm ON tm.id_tipo_documental_proceso = tdp.id
            WHERE tdp.id_proceso = %s AND d.id_archivo_digital = %s 
            AND d.id_tipo_documental = tdp.id_tipo_documental
            AND d.estado NOT IN (7, 8) GROUP BY d.id
        """
    return query

def existe_colas_etapa3(cursor, id_documento):
    sql_select_documento = """
        SELECT COUNT(*) AS cantidad FROM Colas_etapa3 cm WHERE cm.id_documento = %s
    """
    cursor.execute(sql_select_documento, (id_documento,))
    colas_etapa3 = cursor.fetchone()
    return bool(colas_etapa3['cantidad'] > 0)

def insertar_colas_etapa3(cursor, params):
    sql_obtener_documentos = validar_cantidad_etapa3(cursor, params["id_proceso"])
    cursor.execute(sql_obtener_documentos, (params["id_proceso"],params["id_archivo_digital"]))
    documentos = cursor.fetchall()

    sw = 0
    if documentos:
        for documento in documentos:
            timestamp_str = datetime.now(ZoneInfo("America/Bogota")).strftime('%Y-%m-%d %H:%M:%S')
            existe = existe_colas_etapa3(cursor, documento["id"])
            if not existe:
                sql_colas_etapa2 = """
                    INSERT INTO Colas_etapa3 (id_usuario, id_documento, id_estado, fecha_inicio_proceso, 
                    fecha_fin_proceso, fecha_llegada, fecha_eliminacion) VALUES (NULL, %s, 5, NULL, NULL, %s, NULL)
                """
                cursor.execute(sql_colas_etapa2, (documento["id"], timestamp_str))
                sw = 1

        if sw == 0:
            insertar_colas_etapa2(cursor, params)
    else:
        insertar_colas_etapa2(cursor, params)

def update_colas_etapa4(cursor, id_archivo_digital, id_documento):
    timestamp_str = datetime.now(ZoneInfo("America/Bogota")).strftime('%Y-%m-%d %H:%M:%S')

    update_colas_etapa4 = """
        UPDATE Colas_etapa4 ct SET ct.id_estado = 4, ct.fecha_fin_proceso = %s WHERE ct.id_documento = %s
    """
    cursor.execute(update_colas_etapa4, (timestamp_str, id_documento))

    sql_existe_colas_trabajo = """
        SELECT count(*) AS cantidad FROM Colas_etapa4 ct, Documentos d WHERE ct.id_documento = d.id
        AND (ct.id_estado = 1 OR ct.id_estado = 5)
        AND d.id_archivo_digital = %s
    """
    cursor.execute(sql_existe_colas_trabajo, (id_archivo_digital,))
    existe_cola_etapa4 = cursor.fetchone()

    if existe_cola_etapa4["cantidad"] == 0:
        params = build_params(id_archivo_digital, 0, [])
        cambio_estado_lote_documento(cursor, params)

def update_colas_etapa3(cursor, id_archivo_digital):
    sql_obtener_colas_etapa3 = """
        SELECT cm.id FROM Documentos d, Colas_etapa3 cm
        WHERE d.id = cm.id_documento
        AND d.id_archivo_digital = %s
        AND cm.id_estado = 1
    """
    cursor.execute(sql_obtener_colas_etapa3, (id_archivo_digital,))
    obtener_colas_etapa3 = cursor.fetchall()

    if obtener_colas_etapa3:
        for item in obtener_colas_etapa3:
            timestamp_str = datetime.now(ZoneInfo("America/Bogota")).strftime('%Y-%m-%d %H:%M:%S')

            update_colas_etapa3 = """
                    UPDATE Colas_etapa3 cm SET cm.id_estado = 4, cm.fecha_fin_proceso = %s WHERE cm.id = %s
                """
            cursor.execute(update_colas_etapa4, (timestamp_str, item["id"]))
    else:
        print(f"[update_colas_etapa3] No se encontraron documentos para actualizar id_lote {id_archivo_digital}")

def build_params(id_archivo_digital, id_proceso, ids_documentos):
    return {
        "id_proceso": id_proceso,
        "ids_documentos": ids_documentos,
        "id_archivo_digital": id_archivo_digital
    }

def lambda_handler(event, context):
    secret = get_secret()
    connection = pymysql.connect(
        host=secret.host,
        user=secret.username,
        password=secret.password,
        db=secret.database,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    body = event
    try:
        body = json.loads(event["body"])
    except:
        body = event

    try:
        id_actividad_estado = body["id_actividad"]
        id_archivo_digital = body["id_archivo_digital"]
        id_nombre_cola = body["id_nombre_cola"]
        lista_id_documento = body["lista_id_documento"]
        id_proceso = body["id_proceso"]
        id_usuario = body["id_usuario"]
        id_documento = body["id_documento"]
        sw = 0

        with connection.cursor() as cursor:
            if id_actividad_estado == 1:
                sql_check_activity = """
                    SELECT ap.id_actividad FROM Actividades_procesos ap
                    WHERE ap.id_proceso = %s AND ap.orden = 3
                """
                cursor.execute(sql_check_activity, (id_proceso,))
                next_actividad = cursor.fetchone()
                id_actividad = next_actividad["id_actividad"]

                if id_actividad:
                    if id_actividad == 3:
                        params = build_params(id_actividad, id_proceso, lista_id_documento)
                        params["id_usuario"] = id_usuario
                        insertar_colas_etapa2(cursor, params)
                    if id_actividad == 5:
                        params = build_params(id_actividad, id_proceso, lista_id_documento)
                        params["id_usuario"] = id_usuario
                        insertar_colas_etapa3(cursor, params)
                else:
                    params = build_params(id_actividad, id_proceso, lista_id_documento)
                    cambio_estado_lote_documento(cursor, params)

                sw = 1

            if id_actividad_estado == 3:
                etapa2_finalizado = validar_etapa2_finalizado(cursor, id_archivo_digital)
                params = build_params(id_archivo_digital, id_proceso, lista_id_documento)
                if etapa2_finalizado:
                    insertar_colas_etapa4(cursor, params)
                    update_colas_etapa3(cursor, id_archivo_digital)
                else:
                    existe_cola_etapa2 = validar_etapa2(cursor, id_archivo_digital)
                    if not existe_cola_etapa2:
                        insertar_colas_etapa2(cursor, params)
                        update_colas_etapa3(cursor, id_archivo_digital)
                sw = 1

            if id_actividad_estado == 4 and id_archivo_digital and id_documento:
                update_colas_etapa4(cursor, id_archivo_digital, id_documento)
                sw = 1

            if sw == 1:
                connection.commit()
            else:
                connection.rollback()
                return {
                    "statusCode": 500,
                    "body": json.dumps({'msg': "Error al procesar la solicitud"})
                }

        return {
                    "statusCode": 200,
                    "body": json.dumps({'msg': "Exito al procesar la solicitud"})
                }
    except IntegrityError as e:
        if e.args[0] == 1062:
            print("ya existe registro para este lote")
            response = {
                "msg": "Ya existe registro para este lote",
            }
            return {'statusCode': 200, 'body': json.dumps(response)}
        else:
            raise e
    except Exception as e:
        connection.rollback()
        return {
            "statusCode": 500,
            "body": json.dumps({'msg': str(e)})
        }
    finally:
        connection.close()