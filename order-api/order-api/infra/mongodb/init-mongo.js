// Este script cria o banco "orders_db" e a coleção "orders" no MongoDB.
//
// Ele roda automaticamente sozinho, só na primeira vez que o container
// do Mongo é criado (é assim que a imagem oficial do MongoDB funciona).
//
// Também cria um índice único no campo "id", para garantir que dois
// pedidos nunca tenham o mesmo identificador.

db = db.getSiblingDB("orders_db");

db.createCollection("orders");

db.orders.createIndex({ id: 1 }, { unique: true, name: "uniq_order_id" });

print("MongoDB inicializado: banco 'orders_db', coleção 'orders' com índice único em 'id'.");
