import json
import boto3
import os

# Inicializar recursos de AWS
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

# Obtener el nombre de la tabla DynamoDB y el ARN del tema SNS desde variables de entorno
table_name = os.environ['ORDERS_TABLE']
sns_topic_arn = os.environ['SNS_TOPIC_ARN']
s3_bucket = os.environ['S3_BUCKET'].strip()  # Asegúrate de eliminar espacios en blanco
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    # Verificar si 'body' es una cadena y convertirla a diccionario si es necesario
    body = event['body']
    if isinstance(body, str):
        body = json.loads(body)
    
    # Obtener el order_id desde el cuerpo de la solicitud y el tenant_id desde el encabezado
    order_id = body['order_id']
    tenant_id = event['headers']['tenant_id']
    
    # Definir la clave del pedido
    order_key = {
        'tenant_id': tenant_id,
        'order_id': order_id
    }
    
    # Obtener la URL del texto asociado
    response = table.get_item(Key=order_key)
    if 'Item' in response:
        order = response['Item']
        if 'text_url' in order:
            text_url = order['text_url']
            text_key = text_url.split(f"s3://{s3_bucket}/")[1]
            
            # Eliminar el archivo de texto de S3
            s3 = boto3.client('s3')
            s3.delete_object(Bucket=s3_bucket, Key=text_key)
    
    # Eliminar el pedido de la tabla DynamoDB
    table.delete_item(Key=order_key)
    
    # Publicar un mensaje en el tema SNS
    sns_message = {
        'tenant_id': tenant_id,
        'order_id': order_id,
        'message': 'Order deleted'
    }
    
    response = sns.publish(
        TopicArn=sns_topic_arn,
        Message=json.dumps(sns_message),
        Subject='Order Deleted'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Order deleted successfully', 'sns_response': response})
    }