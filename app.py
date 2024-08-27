from flask import Flask, request, jsonify
import json
from confluent_kafka import Producer

app = Flask(__name__)

# Configuración del productor de Kafka
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto por la dirección de tu servidor Kafka
}

producer = Producer(kafka_conf)


@app.route('/payments', methods=['POST'])
def send_to_kafka():
    data = request.json
    topic = 'payments-topic'

    # Convertir el mensaje JSON a string para enviar a Kafka
    message = json.dumps(data)
    producer.produce(topic, value=message)
    producer.flush()  # Asegúrate de que el mensaje se ha enviado

    return jsonify({'status': 'success'}), 200


@app.route('/transactions', methods=['POST'])
def send_transactions_to_kafka():
    data = request.json
    topic = 'transactions-topic'

    # Convertir el mensaje JSON a string para enviar a Kafka
    message = json.dumps(data)
    producer.produce(topic, value=message)
    producer.flush()  # Asegúrate de que el mensaje se ha enviado

    return jsonify({'status': 'success'}), 200


@app.route('/own-payments', methods=['POST'])
def send_own_payment_to_kafka():
    data = request.json
    topic = 'own-payments-topic'

    # Convertir el mensaje JSON a string para enviar a Kafka
    message = json.dumps(data)
    producer.produce(topic, value=message)
    producer.flush()  # Asegúrate de que el mensaje se ha enviado

    return jsonify({'status': 'success'}), 200


@app.route('/routing-orc-payments', methods=['POST'])
def send_routing_orc_payments_to_kafka():
    data = request.json
    topic = 'routing-orc-payments-topic'

    # Convertir el mensaje JSON a string para enviar a Kafka
    message = json.dumps(data)
    producer.produce(topic, value=message)
    producer.flush()  # Asegúrate de que el mensaje se ha enviado

    return jsonify({'status': 'success'}), 200


if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=5000)
