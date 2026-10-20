# app/main.py
# ---------------------------------------------------
# ORDER SERVICE - Point d'entrée du système Uber Eats
# Rôle: C'est le seul service exposé en HTTP (API Gateway)
# - Il reçoit les commandes du navigateur (POST /api/order)
# - Il produit un événement dans Kafka (order-created)
# - Il pousse le live tracking au frontend via WebSocket
# Pattern utilisé: Event Sourcing + CQRS
# ---------------------------------------------------
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import uuid
import time
import asyncio
import json
from pathlib import Path

# Import de notre config centrale Kafka
from app.kafka_config import get_producer, TOPICS

# ---------------------------------------------------
# INITIALISATION FASTAPI
# title et description apparaissent sur /docs (Swagger)
# ---------------------------------------------------
app = FastAPI(
    title="Uber Eats Kafka - Order Service",
    description="Microservice de commande - Producer Kafka avec Confluent",
    version="1.0.0"
)

# ---------------------------------------------------
# PRODUCER KAFKA - Singleton
# On crée un seul producer pour toute l'app FastAPI
# Pourquoi ? Un producer est thread-safe et coûte cher à créer
# ---------------------------------------------------
producer = get_producer()

# ---------------------------------------------------
# WEBSOCKET MANAGER - Pour le temps réel
# C'est ce qui permet au navigateur de voir la commande passer
# de CREATED -> ACCEPTED -> DELIVERED sans refresh
# Pattern: Observer Pattern
# ---------------------------------------------------
class ConnectionManager:
    """Gère les connexions WebSocket actives pour le push temps réel"""
    def __init__(self):
        # Liste des clients connectés (navigateurs ouverts)
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        # Accepte la connexion WebSocket du navigateur
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"[WS] Nouveau client connecté. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        # Retire le client quand il ferme l'onglet
        self.active_connections.remove(websocket)
        print(f"[WS] Client déconnecté. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        # Envoie un message à TOUS les navigateurs connectés
        # C'est comme ça que tout le monde voit le statut changer en live
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(message))
            except Exception as e:
                print(f"[WS] Erreur broadcast: {e}")

# Instance unique du manager
manager = ConnectionManager()

# ---------------------------------------------------
# MONTAGE DES FICHIERS STATIQUES (Frontend)
# FastAPI va servir le dossier app/static sur /static
# ---------------------------------------------------
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# ---------------------------------------------------
# ROUTE 1: PAGE D'ACCUEIL - Sert l'interface web
# ---------------------------------------------------
@app.get("/", tags=["Frontend"])
async def get_home():
    """
    Sert le fichier index.html au navigateur
    Quand tu vas sur http://localhost:8000, c'est cette fonction qui s'exécute
    """
    html_path = Path("app/static/index.html")
    # Si le fichier n'existe pas, on renvoie une erreur claire
    if not html_path.exists():
        return HTMLResponse(content="<h1>index.html not found in app/static/</h1>", status_code=404)
    
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"), status_code=200)

# ---------------------------------------------------
# ROUTE 2: API DE CREATION DE COMMANDE - Le Producer Kafka
# C'est le cœur métier: HTTP -> Kafka
# ---------------------------------------------------
@app.post("/api/order", tags=["Orders"])
async def create_order(order_data: dict):
    """
    Endpoint appelé par le bouton 'Commander' sur le site
    1. Reçoit le JSON du frontend
    2. Crée un événement métier (Event Sourcing)
    3. Publie dans Kafka topic order-created
    4. Notifie le frontend en WebSocket
    """
    # 1. Construction de l'événement métier - Pattern Event Sourcing
    # On ne stocke pas l'état final, on stocke l'événement qui s'est produit
    order_event = {
        "order_id": str(uuid.uuid4()),  # ID unique pour le tracing distribué
        "customer": order_data.get("customer", "Anonyme"),
        "restaurant": order_data.get("restaurant", "Restaurant"),
        "items": order_data.get("items", "Menu"),
        "total": order_data.get("total", 0),
        "status": "CREATED",  # État initial de la state machine
        "timestamp": time.time(),  # Pour l'ordonnancement
        "type": "ORDER_CREATED"  # Type d'événement pour le routage
    }

    # 2. Publication dans Kafka - Opération asynchrone et non bloquante
    # key=order_id est CRITIQUE: garantit que tous les événements de cette
    # commande iront dans la même partition -> garantie d'ordre
    # Sans clé, Kafka répartit aléatoirement et tu peux recevoir DELIVERED avant ACCEPTED
    producer.send(
        TOPICS["ORDER_CREATED"],
        key=order_event["order_id"],
        value=order_event
    )
    
    # 3. Flush: force l'envoi immédiat (sinon Kafka batch par défaut)
    # En prod on ne flush pas à chaque message pour la perf, mais ici pour le live
    producer.flush()

    print(f"[ORDER-SERVICE] Order produced: {order_event['order_id']}")

    # 4. Push temps réel au frontend via WebSocket pour feedback instantané
    # Le client voit sa commande apparaître sans attendre le consumer
    await manager.broadcast(order_event)

    # 5. Réponse HTTP au client
    return {
        "message": "Order created and published to Kafka",
        "order_id": order_event["order_id"],
        "order": order_event,
        "kafka_topic": TOPICS["ORDER_CREATED"]
    }

# ---------------------------------------------------
# ROUTE 3: WEBSOCKET - Pour le tracking live
# ---------------------------------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Connexion WebSocket persistante
    Le frontend se connecte ici au chargement de la page
    et reste connecté pour recevoir les mises à jour
    """
    await manager.connect(websocket)
    try:
        # Boucle infinie pour garder la connexion ouverte
        # On attend des messages du client (même si on n'en envoie pas, il faut écouter)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        # Le client a fermé son navigateur
        manager.disconnect(websocket)

# ---------------------------------------------------
# ROUTE 4: HEALTH CHECK - Indispensable en prod
# Utilisé par Docker et Kubernetes pour savoir si le service est vivant
# ---------------------------------------------------
@app.get("/health", tags=["System"])
async def health_check():
    """Endpoint de santé pour Docker HEALTHCHECK et monitoring"""
    return {
        "status": "ok",
        "service": "order-service",
        "kafka_bootstrap": get_producer().config['bootstrap_servers']
    }

# ---------------------------------------------------
# STARTUP EVENT - Lance les consumers en arrière-plan
# En architecture microservices pure, chaque consumer serait un service séparé
# Ici pour simplifier le mini projet, on les lance en threads depuis FastAPI
# ---------------------------------------------------
@app.on_event("startup")
async def startup_event():
    """
    Au démarrage de FastAPI, on lance les 3 consumers en background
    Pattern: Background Tasks
    """
    from app.consumers import start_all_consumers
    # On lance dans un thread séparé pour ne pas bloquer FastAPI
    # asyncio.to_thread permet de lancer du code synchrone (Kafka) depuis de l'async
    asyncio.create_task(asyncio.to_thread(start_all_consumers, manager))
    print("[SYSTEM] FastAPI started, background consumers launching...")