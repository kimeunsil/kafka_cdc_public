-- 로컬 데모용 샘플 스키마 (가상의 쇼핑몰)
USE shop;

CREATE TABLE customers (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  email       VARCHAR(200) NOT NULL UNIQUE,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(200) NOT NULL,
  price       DECIMAL(12,2) NOT NULL,
  updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE orders (
  id           BIGINT AUTO_INCREMENT PRIMARY KEY,
  customer_id  BIGINT NOT NULL,
  product_id   BIGINT NOT NULL,
  quantity     INT NOT NULL,
  status       VARCHAR(20) NOT NULL DEFAULT 'CREATED',
  updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (customer_id) REFERENCES customers(id),
  FOREIGN KEY (product_id)  REFERENCES products(id)
);

INSERT INTO customers (name, email) VALUES
  ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com');
INSERT INTO products (name, price) VALUES
  ('Keyboard', 59000), ('Mouse', 29000);
INSERT INTO orders (customer_id, product_id, quantity) VALUES
  (1, 1, 1), (2, 2, 3);

-- Debezium 전용 계정: 최소 권한 (binlog 읽기 + 스냅샷용 SELECT)
CREATE USER 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'change-me';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
