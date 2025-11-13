import boto3
import uuid
import os
import json
from datetime import datetime

# Clientes/recursos
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Variables de entorno
NOMBRE_TABLA = os.environ.get("TABLE_NAME")
BUCKET_INGEST = os.environ.get("INGEST_BUCKET")
STAGE = os.environ.get("STAGE", "dev")

def lambda_handler(event, context):
    # Logging simple
    print("Received event:", event)

    # Obtener body (API Gateway proxy -> event['body'] suele ser string JSON)
    try:
        if isinstance(event, dict) and 'body' in event:
            body = event['body']
            if isinstance(body, str):
                data = json.loads(body)
            else:
                data = body
        else:
            data = event
    except Exception as e:
        print("Error parsing body:", str(e))
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'body inválido', 'details': str(e)})
        }

    # Validar campos esperados
    tenant_id = data.get('tenant_id')
    texto = data.get('texto')

    if not tenant_id or not texto:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Faltan campos tenant_id o texto'})
        }

    # Generar UUID y item para DynamoDB
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        }
    }

    # Intentar escribir en DynamoDB
    try:
        table = dynamodb.Table(NOMBRE_TABLA)
        response_db = table.put_item(Item=comentario)
        print("DynamoDB put_item response:", response_db)
    except Exception as e:
        print("Error escribiendo en DynamoDB:", str(e))
        # No retornamos error inmediato, intentamos igualmente la ingest a S3
        response_db = {'error': str(e)}

    # Preparar objeto JSON para S3 (agrego metadatos)
    comentario_ingest = comentario.copy()
    comentario_ingest['_ingest_meta'] = {
        'uploaded_at': datetime.utcnow().isoformat() + "Z",
        'stage': STAGE
    }

    filename = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuidv1}.json"

    try:
        s3.put_object(
            Bucket=BUCKET_INGEST,
            Key=filename,
            Body=json.dumps(comentario_ingest, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json'
        )
        s3_result = {'bucket': BUCKET_INGEST, 'key': filename}
        print("S3 put_object OK:", s3_result)
    except Exception as e:
        print("Error subiendo a S3:", str(e))
        s3_result = {'error': str(e)}

    # Respuesta final
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Comentario procesado',
            'comentario': comentario,
            'dynamo_response': response_db,
            's3_result': s3_result
        }, ensure_ascii=False)
    }
