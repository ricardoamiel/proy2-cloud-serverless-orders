import json
import boto3
import os
from decimal import Decimal

# Inicializar el recurso de DynamoDB
dynamodb = boto3.resource('dynamodb')
table_name = os.environ['ORDERS_TABLE']
orders_table = dynamodb.Table(table_name)

# Codificador personalizado para manejar Decimals
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    try:
        # Escanear la tabla para obtener todos los pedidos
        response = orders_table.scan()
        orders = response.get('Items', [])
        
        # Paginación para obtener todos los elementos si hay más de 1 MB de datos
        while 'LastEvaluatedKey' in response:
            response = orders_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            orders.extend(response.get('Items', []))
        
        return {
            'statusCode': 200,
            'body': json.dumps({'orders': orders}, cls=DecimalEncoder)
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal server error'})
        }