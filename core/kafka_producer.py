import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
)


def _connect_type(value):
    """Mapping type Python -> type de schema Kafka Connect."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        return "double"
    return "string"


def _build_schema(record: dict, schema_name="core.MalariaCase"):
    """Construit dynamiquement le schema Kafka Connect à partir du dict Python."""
    fields = []
    for key, value in record.items():
        if isinstance(value, list):
            fields.append({"field": key, "type": "string", "optional": True})
        else:
            fields.append({"field": key, "type": _connect_type(value), "optional": True})

    return {
        "type": "struct",
        "fields": fields,
        "optional": False,
        "name": schema_name,
    }


def _build_payload(record: dict):
    """Convertit les listes en chaîne JSON pour matcher le schema (type string)."""
    payload = {}
    for key, value in record.items():
        if isinstance(value, list):
            payload[key] = json.dumps(value)
        else:
            payload[key] = value
    return payload


def send_case_to_kafka(record: dict):
    """
    Envoie un enregistrement vers le topic malaria-cases, enveloppé au format
    Kafka Connect ({"schema": ..., "payload": ...}) pour que le JDBC Sink
    Connector puisse le convertir en Struct et l'écrire dans MySQL.

    Clé = region_id => garantit que tous les cas d'une même région
    arrivent dans l'ordre sur la même partition.
    """
    envelope = {
        "schema": _build_schema(record),
        "payload": _build_payload(record),
    }

    producer.send(
        topic='malaria-cases',
        key=record['region_id'],
        value=envelope
    )
    producer.flush()