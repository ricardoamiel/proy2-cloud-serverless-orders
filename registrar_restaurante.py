import json
import boto3
import uuid
import os

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['RESTAURANTS_TABLE']
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    # Verificar si 'body' es un diccionario y convertirlo a diccionario si es necesario
    body = event['body']
    if isinstance(body, str):
        body = json.loads(body)
    
    # Generar un tenant_id único si no se proporciona
    tenant_id = body.get('tenant_id', str(uuid.uuid4()))
    restaurant_name = body['restaurant_name']
    contact_info = body['contact_info']
    address = body['address']
    config = body['config']
    
    # Guardar la información del restaurante en la tabla DynamoDB
    table.put_item(
        Item={
            'tenant_id': tenant_id,
            'restaurant_name': restaurant_name,
            'contact_info': contact_info,
            'address': address,
            'config': config
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'tenant_id': tenant_id, 'message': 'Restaurant registered successfully'})
    }
