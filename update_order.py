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
    
    # Obtener el tenant_id desde los encabezados
    tenant_id = event['headers']['tenant_id']
    
    # Definir la clave del pedido
    order_key = {
        'tenant_id': tenant_id,
        'order_id': body['order_id']
    }
    
    # Actualizar el texto en S3 si se proporciona
    if 'text_content' in body:
        text_content = body.pop('text_content')
        text_key = f"{tenant_id}/{body['order_id']}.txt"
        
        # Subir el texto actualizado a S3
        s3 = boto3.client('s3')
        s3.put_object(Bucket=s3_bucket, Key=text_key, Body=text_content)
        
        # Guardar la URL del archivo de texto en el pedido
        body['text_url'] = f"s3://{s3_bucket}/{text_key}"
    
    # Crear la expresión de actualización para DynamoDB
    update_expression = "set order_status = :s"
    expression_attribute_values = {':s': body['order_status']}
    
    if 'text_url' in body:
        update_expression += ", text_url = :u"
        expression_attribute_values[':u'] = body['text_url']
    
    table.update_item(
        Key=order_key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values
    )
    
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
        Subject='Order Updated'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Order updated successfully', 'sns_response': response})
    }
