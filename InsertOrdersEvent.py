import json
import boto3
import os

dynamodb = boto3.resource('dynamodb')
table_name = os.environ['ORDERS_TABLE']
orders_table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    for record in event['Records']:
        # Obtener el mensaje del evento SNS
        message = json.loads(record['Sns']['Message'])
        
        # Insertar el pedido en la tabla DynamoDB
        orders_table.put_item(Item=message)
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Order processed successfully'})
    }
