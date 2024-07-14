import json
import random
import boto3
import os
from datetime import datetime

sns = boto3.client('sns')
sns_topic_arn = os.environ['SNS_TOPIC_ARN']

# Listado de artículos de menú para generar pedidos aleatorios
menu_items = ["Pizza", "Burger", "Pasta", "Salad", "Sushi", "Taco"]
restaurants = ["restaurant1", "restaurant2", "restaurant3"]

def lambda_handler(event, context):
    # Generar un pedido aleatorio
    order_id = str(random.randint(1000, 9999))
    tenant_id = random.choice(restaurants)
    item = random.choice(menu_items)
    quantity = random.randint(1, 5)
    timestamp = datetime.utcnow().isoformat()
    
    # Crear el mensaje de evento
    event_message = {
        'order_id': order_id,
        'tenant_id': tenant_id,
        'item': item,
        'quantity': quantity,
        'timestamp': timestamp
    }
    
    # Publicar el mensaje en SNS
    response = sns.publish(
        TopicArn=sns_topic_arn,
        Message=json.dumps(event_message),
        Subject='Simulated Order'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Simulated order generated successfully', 'event': event_message})
    }
