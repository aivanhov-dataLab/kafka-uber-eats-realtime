# app/consumers.py
# ---------------------------------------------------
# BACKGROUND CONSUMERS - Simule 3 microservices indépendants
# Dans une vraie prod Uber Eats, chaque fonction ci-dessous serait
# un repo Git, un Dockerfile et un déploiement Kubernetes séparé.
# Ici on les met dans un seul fichier pour simplifier le projet recruteur.
#
# 3 SERVICES:
# 1. restaurant-service: Consomme order-created -> Produit order-accepted
# 2. delivery-service: Consomme order-accepted -> Produit order-delivered
# 3. notification-service: Consomme TOUS les topics -> Envoie SMS/Email (Fan-out)
# ---------------------------------------------------
import time
import random
import sys
import json
from kafka import KafkaConsumer

# Import config centrale - même config que le producer
from app.kafka_config import get_consumer, get_producer, TOPICS, BOOTSTRAP_SERVERS

# ---------------------------------------------------
# FONCTION PRINCIPALE - Lance tout en parallèle
# Pattern: Multi-threading
# Chaque consumer tourne dans son propre thread Python (daemon)
# daemon=True = si FastAPI s'arrête, les threads s'arrêtent aussi
# ---------------------------------------------------
def start_all_consumers(ws_manager=None):
    """
    Lance les 3 consumers en parallèle
    :param ws_manager: ConnectionManager WebSocket pour push live au frontend
    """
    import threading

    print("[SYSTEM] Starting all Kafka consumers in background threads...", flush=True)

    # Thread 1: Restaurant
    threading.Thread(
        target=restaurant_consumer,
        args=(ws_manager,),
        daemon=True,
        name="restaurant-thread"
    ).start()

    # Thread 2: Delivery
    threading.Thread(
        target=delivery_consumer,
        args=(ws_manager,),
        daemon=True,
        name="delivery-thread"
    ).start()

    # Thread 3: Notification (Fan-out)
    threading.Thread(
        target=notification_consumer,
        args=(ws_manager,),
        daemon=True,
        name="notification-thread"
    ).start()

    print("[SYSTEM] All 3 consumers started successfully", flush=True)

# ---------------------------------------------------
# SERVICE 1: RESTAURANT SERVICE
# Rôle Métier: Le restaurant reçoit la commande, la prépare
# Pattern Kafka: Consumer + Producer (Processor)
# Consumer Group: restaurant-group
# - Si tu lances 2 instances de ce service avec même group_id,
#   Kafka répartit les commandes (load balancing)
# ---------------------------------------------------
def restaurant_consumer(ws_manager):
    """
    Écoute order-created, simule la préparation, publie order-accepted
    """
    # On crée le consumer avec son group_id dédié
    consumer = get_consumer(TOPICS["ORDER_CREATED"], group_id="restaurant-service-group")
    
    # On a besoin aussi d'un producer pour publier la suite
    producer = get_producer()

    print("[RESTAURANT-SERVICE] Ready - Waiting for order-created events...", flush=True)

    # Boucle infinie - C'est le cœur d'un consumer Kafka
    # for msg in consumer bloque tant qu'il n'y a pas de message
    for msg in consumer:
        order = msg.value
        partition = msg.partition
        offset = msg.offset

        print(f"[RESTAURANT] Received order {order['order_id'][:8]} (partition={partition} offset={offset}) Customer={order['customer']}", flush=True)

        # Simulation métier: temps de préparation aléatoire
        # En prod: appel à la base de données restaurant, vérif stock, etc.
        cooking_time = random.randint(2, 4)
        print(f"[RESTAURANT] Cooking {order['items']} for {cooking_time}s...", flush=True)
        time.sleep(cooking_time)

        # Transition d'état - State Machine
        # CREATED -> ACCEPTED
        order["status"] = "ACCEPTED"
        order["type"] = "ORDER_ACCEPTED"
        order["restaurant_timestamp"] = time.time()

        # Publication du nouvel événement dans le topic suivant
        producer.send(
            TOPICS["ORDER_ACCEPTED"],
            key=order["order_id"],  # Même clé = même partition = ordre garanti
            value=order
        )
        producer.flush()

        print(f"[RESTAURANT] Order {order['order_id'][:8]} -> ACCEPTED", flush=True)

        # Push temps réel au frontend si WebSocket disponible
        if ws_manager:
            import asyncio
            try:
                asyncio.run(ws_manager.broadcast(order))
            except Exception as e:
                print(f"[RESTAURANT] WS broadcast failed: {e}")

# ---------------------------------------------------
# SERVICE 2: DELIVERY SERVICE
# Rôle Métier: Le livreur récupère et livre
# Pattern: Identique à restaurant-service
# Consumer Group: delivery-group (différent de restaurant)
# ---------------------------------------------------
def delivery_consumer(ws_manager):
    """Écoute order-accepted, simule la livraison, publie order-delivered"""
    consumer = get_consumer(TOPICS["ORDER_ACCEPTED"], group_id="delivery-service-group")
    producer = get_producer()

    print("[DELIVERY-SERVICE] Ready - Waiting for order-accepted events...", flush=True)

    for msg in consumer:
        order = msg.value
        print(f"[DELIVERY] Picking up order {order['order_id'][:8]} for {order['customer']}", flush=True)

        # Simulation: temps de livraison
        delivery_time = random.randint(3, 6)
        print(f"[DELIVERY] Delivering... {delivery_time}s", flush=True)
        time.sleep(delivery_time)

        # Transition d'état: ACCEPTED -> DELIVERED
        order["status"] = "DELIVERED"
        order["type"] = "ORDER_DELIVERED"
        order["delivery_timestamp"] = time.time()

        producer.send(
            TOPICS["ORDER_DELIVERED"],
            key=order["order_id"],
            value=order
        )
        producer.flush()

        print(f"[DELIVERY] Order {order['order_id'][:8]} -> DELIVERED", flush=True)

        if ws_manager:
            import asyncio
            try:
                asyncio.run(ws_manager.broadcast(order))
            except:
                pass

# ---------------------------------------------------
# SERVICE 3: NOTIFICATION SERVICE
# Rôle Métier: Envoie SMS / Push Notification à chaque étape
# Pattern Kafka: Fan-out / Broadcast
# - Il s'abonne à TOUS les topics avec *TOPICS.values()
# - Il a un group_id DIFFÉRENT des autres
#   -> Il reçoit TOUS les messages même si restaurant et delivery les ont déjà lus
# C'est comme ça que fonctionnent les services d'analytics / notification en prod
# ---------------------------------------------------
def notification_consumer(ws_manager):
    """
    Écoute tous les topics pour envoyer des notifications
    Pattern Fan-out: ce service reçoit une copie de chaque message
    """
    # Consumer qui écoute plusieurs topics à la fois
    consumer = KafkaConsumer(
        *TOPICS.values(),  # order-created, order-accepted, order-delivered
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id="notification-service-group",  # Groupe différent = reçoit tout
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print("[NOTIFICATION-SERVICE] Ready - Listening to ALL order events (Fan-out)...", flush=True)

    for msg in consumer:
        order = msg.value
        topic = msg.topic  # On sait de quel topic vient le message
        order_short = order['order_id'][:8]

        # Logique métier: envoi de notification selon le statut
        # En prod: ici on appellerait Twilio, SendGrid, Firebase
        if topic == TOPICS["ORDER_CREATED"]:
            print(f"📱 [NOTIF] SMS to {order['customer']}: Order {order_short} confirmed! Restaurant {order['restaurant']} is preparing.", flush=True)
        
        elif topic == TOPICS["ORDER_ACCEPTED"]:
            print(f"📱 [NOTIF] SMS to {order['customer']}: Good news! Your order {order_short} is being cooked. 🍳", flush=True)
        
        elif topic == TOPICS["ORDER_DELIVERED"]:
            print(f"📱 [NOTIF] SMS to {order['customer']}: Order {order_short} delivered! Enjoy your meal! 🍔 Rate your delivery.", flush=True)

        # On pourrait aussi push au frontend une notif toast
        if ws_manager:
            # On ne rebroadcast pas pour éviter double push, juste log
            pass