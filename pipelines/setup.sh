#!/bin/bash

echo "=============================================="
echo "   EPL PIPELINE — DATABASE + TABLE CREATOR"
echo "=============================================="


# ---------------------------------------------
# 1. MYSQL — CREATE DATABASE + TABLE
# ---------------------------------------------
echo "🟡 Setting up MySQL..."

mysql -u root -proot << 'EOF'
CREATE DATABASE IF NOT EXISTS epl;

USE epl;

CREATE TABLE IF NOT EXISTS fan_sales (
    transaction_id VARCHAR(64) PRIMARY KEY,
    event_ts       VARCHAR(255),
    fan_id         INT,
    team           VARCHAR(100),
    country        VARCHAR(100),
    product_name   VARCHAR(255),
    unit_price     DOUBLE,
    quantity       INT,
    image_url      TEXT,
    source         VARCHAR(50)
);
EOF

echo "✔ MySQL fan_sales table created."


# ---------------------------------------------
# 2. CASSANDRA — CREATE KEYSPACE + TABLES
# ---------------------------------------------
echo "🟣 Setting up Cassandra..."

cat << 'EOF' > cassandra_setup.cql
CREATE KEYSPACE IF NOT EXISTS epl
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE epl;

CREATE TABLE IF NOT EXISTS fan_sales (
    transaction_id TEXT PRIMARY KEY,
    event_ts       TEXT,
    fan_id         INT,
    team           TEXT,
    country        TEXT,
    product_name   TEXT,
    unit_price     DOUBLE,
    quantity       INT,
    image_url      TEXT,
    source         TEXT
);

CREATE TABLE IF NOT EXISTS product_info (
    product_name TEXT PRIMARY KEY,
    image_url    TEXT,
    description  TEXT
);
EOF

cqlsh -f cassandra_setup.cql
echo "✔ Cassandra keyspace + tables created."


# ---------------------------------------------
# 3. INSERT PRODUCT INFO INTO CASSANDRA
# ---------------------------------------------
echo "🟣 Seeding Cassandra product_info..."

cat << 'EOF' > cassandra_inserts.cql
USE epl;

INSERT INTO product_info (product_name, image_url, description) VALUES
('Home Jersey', 'https://i.imgur.com/8y6H4pE.jpeg', 'Official team home kit'),
('Away Jersey', 'https://i.imgur.com/5qF8HW7.jpeg', 'Official team away kit'),
('Third Kit Jersey', 'https://i.imgur.com/4mpeVvH.jpeg', 'Alternate match jersey'),
('Scarf', 'https://i.imgur.com/8gF1jtk.jpeg', 'Warm knitted matchday scarf'),
('Hoodie', 'https://i.imgur.com/joelmWB.png', 'Premium supporter hoodie'),
('Signed Jersey', 'https://i.imgur.com/tWGbEHt.jpeg', 'Autographed collectors jersey'),
('Water Bottle', 'https://i.imgur.com/yn0oamC.jpeg', 'Stainless steel bottle'),
('Backpack', 'https://i.imgur.com/lbtd3K9.jpeg', 'Team-branded backpack');
EOF

cqlsh -f cassandra_inserts.cql
echo "✔ Product metadata inserted."


# ---------------------------------------------
# 4. OPTIONAL: INSERT SAMPLE MYSQL DATA
# ---------------------------------------------
echo "🟡 Adding small MySQL seed example..."

mysql -u root -proot << 'EOF'
USE epl;

INSERT IGNORE INTO fan_sales (
    transaction_id, event_ts, fan_id, team, country,
    product_name, unit_price, quantity, image_url, source
) VALUES (
    'seed12345', NOW(), 1011, 'Man Utd', 'UK',
    'Home Jersey', 99.99, 1,
    'https://i.imgur.com/8y6H4pE.jpeg', 'organic'
);
EOF

echo "✔ MySQL seed inserted."

echo "=============================================="
echo "   SETUP COMPLETE — ALL DATABASES READY 🎉"
echo "=============================================="

