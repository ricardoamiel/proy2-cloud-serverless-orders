import json
import boto3
import os

dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

table_name = os.environ['ORDERS_TABLE']
restaurants_table_name = os.environ['RESTAURANTS_TABLE']
sns_topic_arn = os.environ['SNS_TOPIC_ARN']
s3_bucket = os.environ['S3_BUCKET'].strip()

table = dynamodb.Table(table_name)
restaurants_table = dynamodb.Table(restaurants_table_name)

def lambda_handler(event, context):
    body = event['body']
    if isinstance(body, str):
        body = json.loads(body)
    
    tenant_id = event['headers']['tenant_id']
    
    # Verificar la existencia del tenant_id en la tabla de restaurantes
    response = restaurants_table.get_item(Key={'tenant_id': tenant_id})
    if 'Item' not in response:
        return {
            'statusCode': 404,
            'body': json.dumps({'message': 'Restaurant not found'})
        }
    
    # Verificar si hay un texto adjunto
    if 'text_content' in body:
        text_content = body.pop('text_content')
        text_key = f"{tenant_id}/{body['order_id']}.txt"
        
        # Subir el texto a S3
        s3 = boto3.client('s3')
        s3.put_object(Bucket=s3_bucket, Key=text_key, Body=text_content)
        
        # Guardar la URL del archivo de texto en el pedido
        body['text_url'] = f"s3://{s3_bucket}/{text_key}"
    
    # Agregar tenant_id al pedido
    body['tenant_id'] = tenant_id
    
    # Insertar el pedido en la tabla DynamoDB
    table.put_item(Item=body)
    
    # Publicar un mensaje en el tema SNS
    sns_message = {
        'tenant_id': tenant_id,
        'order_id': body['order_id'],
        'order_status': body['order_status'],
        'text_url': body.get('text_url', None)
    }
    
    response = sns.publish(
        TopicArn=sns_topic_arn,
        Message=json.dumps(sns_message),
        Subject='New Order Created'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Order created successfully', 'sns_response': response})
    }
