# Kafka Expliqué Simplement – Application de Stream Processing pour Paiements, Commandes & Inventaire
![Architecture de kafka](https://github.com/olaiamechal123/formations/blob/main/kafka-architecture.png)

Ce projet démontre comment **Apache Kafka** résout les problèmes classiques d'une architecture microservices synchrone "naïve" en passant à une architecture **événementielle** (event-driven).

## Problème avec une architecture microservices synchrone (tight coupling)

Dans une implémentation classique :

- Les microservices communiquent directement via **HTTP/REST** (synchrone)
- Exemple : Service **Order** appelle **Payment** → si Payment est lent ou down → Order attend → toute la commande gèle
- Si un service tombe, tous les services qui en dépendent sont bloqués
- Risque de perte de données (surtout analytics) car tout repose sur des appels en temps réel sans persistance

→ Système fragile, difficile à scaler, couplage fort

## Solution : Kafka comme middleware → Architecture événementielle (event-driven)

Au lieu que les services s'appellent directement, ils passent tous par **Kafka** (comme une boîte aux lettres asynchrone ultra-fiable).

Avantages majeurs :

- **Découplage total** (loose coupling) → les services ne se connaissent plus directement
- **Asynchrone** → un service continue à travailler même si un autre est lent ou down
- **Durabilité** → les événements sont stockés de manière persistante (pas de perte)
- **Scalabilité horizontale** massive
- **Tolérance aux pannes** → répartition automatique du travail

## Les concepts Kafka expliqués simplement

### Producer
Service qui **crée un événement** et l'envoie à Kafka.  
Exemple : Service Order → "une commande a été passée" → publie un message dans Kafka.

### Event / Message
Structure simple (souvent JSON) contenant :

- **Key** : ex. ID de la commande (utile pour l'ordre et le partitionnement)
- **Value** : les données (détails de la commande, montant, etc.)
- Métadonnées : timestamp, nom du service producteur, headers...

### Topic
Catégorie / dossier / file nommée où les événements sont organisés.  
Exemples concrets :

- `new-orders`
- `payments-processed`
- `inventory-updated`
- `order-cancelled`

### Consumer
Service qui **s'abonne** à un ou plusieurs topics et **lit** les événements quand ils arrivent.  
Exemple :

- Service Inventory lit `new-orders` → décrémenter le stock
- Service Notification lit `payments-processed` → envoyer email/SMS

### Partitions = la clé de la scalabilité
Un topic est divisé en **partitions** (morceaux parallèles).  
Exemple : topic `orders` avec 3 partitions :

- Partition 0 → commandes Europe
- Partition 1 → commandes US
- Partition 2 → commandes Afrique

Avantages :

- Parallélisme → plusieurs consumers lisent en même temps (1 consumer par partition)
- Si le trafic explose sur les US → ajouter des partitions + ajouter des consumers → scalabilité horizontale

### Consumer Group
Groupe de consumers qui **partagent le travail** d’un topic.

- Chaque partition est lue par **un seul** consumer du groupe (load balancing automatique)
- Si un consumer meurt → Kafka **réassigne** automatiquement ses partitions (rebalancing)
- Permet de scaler horizontalement : 1 topic → 10 consumers = ×10 throughput

## Exemple concret : Backend Food Delivery (type Uber Eats / DoorDash)

1. Client passe une commande → Service **Order** publie `OrderCreated` dans topic `new-orders`
2. Service **Inventory** consomme → vérifie & réserve le stock → publie `InventoryReserved`
3. Service **Payment** consomme → traite le paiement → publie `PaymentProcessed` ou `PaymentFailed`
4. Service **Notification** consomme → envoie confirmation au client
5. Service **Delivery** consomme → assigne un livreur

→ Tout est asynchrone, durable et scalable

## Démarrer Kafka localement (avec Docker)

```bash
# Générer un UUID pour KRaft (Kafka 3.x+ sans ZooKeeper)
docker run --rm confluentinc/cp-kafka:7.8.3 kafka-storage random-uuid
# Copie le UUID généré

# Lancer un cluster simple (exemple minimal avec 1 broker)
docker-compose up -d



[Des informations sur kafka](https://github.com/olaiamechal123/formations/blob/main/kafka%20formation.txt)

