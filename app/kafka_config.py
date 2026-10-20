# app/kafka_config.py
# ---------------------------------------------------
# Configuration centrale Kafka - Single Source of Truth
# Pourquoi ce fichier ? Pour éviter de répéter bootstrap_servers
# dans chaque service. En prod, on change une seule variable d'env.
# ---------------------------------------------------
import os
from kafka import KafkaProducer, KafkaConsumer
import json

# ---------------------------------------------------
# ADRESSE DU BROKER KAFKA
# - En local (VSCode): localhost:9092
# - En Docker: kafka:29092 (nom du service dans docker-compose)
# On utilise une variable d'environnement pour switcher sans toucher au code
# ---------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Pour l'UI Kafka qui tourne dans Docker, on a besoin de l'adresse interne
# Confluent expose 2 listeners: 29092 (interne) et 9092 (externe/host)
INTERNAL_BOOTSTRAP = "kafka:29092"

# ---------------------------------------------------
# LISTE DES TOPICS - Convention de nommage: domaine-evenement
# C'est une bonne pratique: on ne hardcode jamais un nom de topic
# dans les services, on importe depuis ici.
# - order-created: quand le client commande
# - order-accepted: quand le restaurant accepte
# - order-delivered: quand le livreur livre
# ---------------------------------------------------
TOPICS = {
    "ORDER_CREATED": "order-created",
    "ORDER_ACCEPTED": "order-accepted",
    "ORDER_DELIVERED": "order-delivered"
}

# ---------------------------------------------------
# FABRIQUE DE PRODUCER - Pattern Factory
# Expert Kafka: 3 paramètres critiques pour la prod
# - acks='all': le leader attend que tous les replicas aient reçu le message
#   -> garantit 0 perte de message, mais un peu plus lent. C'est le standard prod.
# - retries=3: si le broker est temporairement down, on réessaye auto
# - key_serializer: la clé (order_id) permet de garantir l'ordre des messages
#   pour une même commande (tous les events d'une commande vont dans même partition)
# ---------------------------------------------------
def get_producer():
    """
    Retourne un producer Kafka configuré en JSON
    Usage: producer = get_producer()
            producer.send(topic, key=order_id, value=order_dict)
    """
    return KafkaProducer(
        # Adresse du cluster Kafka
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Sérialisation de la valeur: dict Python -> JSON -> bytes
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Sérialisation de la clé: str -> bytes (pour le partitionnement)
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        # Garantie de durabilité - Prod grade
        acks='all',
        # Nombre de tentatives en cas d'échec réseau
        retries=3,
        # Temps max d'attente pour acks
        request_timeout_ms=10000
    )

# ---------------------------------------------------
# FABRIQUE DE CONSUMER - Pattern Factory
# Expert Kafka: group_id est LE concept le plus important
# - Même group_id: les consumers se partagent le travail (load balancing)
#   ex: 3 instances de restaurant-service avec même group_id = chaque commande
#   traitée par une seule instance
# - Group_id différent: chaque groupe reçoit TOUS les messages (fan-out)
#   ex: notification-service a un group_id différent, il reçoit tout même si
#   restaurant-service a déjà lu le message
# ---------------------------------------------------
def get_consumer(topic, group_id):
    """
    Retourne un consumer Kafka configuré
    :param topic: str ou list de topics à écouter
    :param group_id: str, identifiant du groupe de consommateurs
    """
    return KafkaConsumer(
        # Topic(s) à écouter - peut être une string ou plusieurs
        topic,
        # Adresse du cluster
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Désérialisation: bytes -> JSON -> dict Python
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        # Groupe de consommateurs - Critique pour la scalabilité
        group_id=group_id,
        # Si c'est la première fois que ce group_id se connecte, on lit depuis le début
        # En prod on met 'latest' pour ne pas rejouer tout l'historique
        auto_offset_reset='earliest',
        # Commit auto de l'offset - simplifie le code pour un mini projet
        # En prod avec exactly-once, on ferait commit manuel
        enable_auto_commit=True,
        # Timeout pour la session consumer
        session_timeout_ms=30000
    )