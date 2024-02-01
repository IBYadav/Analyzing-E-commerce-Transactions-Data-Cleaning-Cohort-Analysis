# -- Load the sql extention ----
%load_ext sql
# --- Load your mysql db using credentials from the "DB" area ---
%sql mysql+pymysql://bed02a19:Cab#22se@localhost/bed02a19

%%sql
SELECT world_regions.region_name, COUNT(DISTINCT user_nodes.consumer_id) AS num_users
FROM world_regions
LEFT JOIN user_nodes ON world_regions.region_id = user_nodes.region_id
GROUP BY world_regions.region_name;

%%sql
WITH MaxTransactionAmounts AS (
    SELECT
        consumer_id,
        transaction_type,
        MAX(transaction_amount) AS largest_deposit
    FROM user_transaction
    GROUP BY consumer_id, transaction_type
)
SELECT
    consumer_id,
    transaction_type,
    largest_deposit
FROM MaxTransactionAmounts

WHERE (consumer_id, transaction_type, largest_deposit) = (
    SELECT consumer_id, transaction_type, MAX(largest_deposit)
    FROM MaxTransactionAmounts
);


%%sql
SELECT un.consumer_id, 
SUM(ut.transaction_amount) AS total_deposit_amount
FROM user_nodes un
JOIN user_transaction ut ON un.consumer_id = ut.consumer_id
JOIN world_regions wr ON un.region_id = wr.region_id
WHERE wr.region_name = 'Europe' AND ut.transaction_type = 'deposit'
GROUP BY un.consumer_id;

%%sql
SELECT un.consumer_id, COUNT(*) AS num_transactions
FROM user_nodes un
JOIN user_transaction ut ON un.consumer_id = ut.consumer_id
JOIN world_regions wr ON un.region_id = wr.region_id
WHERE wr.region_name = 'United States'
GROUP BY un.consumer_id;

%%sql
SELECT ut.consumer_id, COUNT(*) AS num_transactions
FROM user_transaction ut
WHERE ut.consumer_id IN 
(
    SELECT un.consumer_id
    FROM user_nodes un, world_regions wr
    WHERE  wr.region_name = 'United States'
)
GROUP BY ut.consumer_id
HAVING num_transactions > 5;

%%sql
SELECT wr.region_name, COUNT(un.node_id) AS num_nodes
FROM world_regions wr
JOIN user_nodes un ON wr.region_id = un.region_id
GROUP BY wr.region_name
ORDER BY num_nodes DESC;

%%sql
SELECT consumer_id, MAX(transaction_amount) AS largest_deposit
FROM user_transaction
WHERE consumer_id IN (
    SELECT un.consumer_id
    FROM user_nodes un
    JOIN world_regions wr ON un.region_id = wr.region_id
    WHERE wr.region_name = 'Australia'
)
AND transaction_type = 'deposit'
GROUP BY consumer_id
ORDER BY largest_deposit DESC
LIMIT 1;

%%sql
SELECT un.consumer_id, wr.region_name, 
SUM(ut.transaction_amount) AS total_deposit_amount
FROM user_nodes un
JOIN world_regions wr ON un.region_id = wr.region_id
JOIN user_transaction ut ON un.consumer_id = ut.consumer_id
WHERE ut.transaction_type = 'deposit'
GROUP BY un.consumer_id, wr.region_name;

%%sql
SELECT wr.region_name,
COUNT(ut.consumer_id) AS total_transactions
FROM world_regions wr
LEFT JOIN user_nodes un ON wr.region_id = un.region_id
LEFT JOIN user_transaction ut ON un.consumer_id = ut.consumer_id
GROUP BY wr.region_name
HAVING total_transactions > 0;

%%sql
SELECT wr.region_name, 
SUM(ut.transaction_amount) AS total_deposit_amount
FROM world_regions wr
JOIN user_nodes un ON wr.region_id = un.region_id
JOIN user_transaction ut ON un.consumer_id = ut.consumer_id
WHERE ut.transaction_type = 'deposit'
GROUP BY wr.region_name;

%%sql
SELECT ut.consumer_id,
SUM(ut.transaction_amount) AS total_transaction_amount
FROM user_transaction ut
WHERE transaction_type = 'deposit'
GROUP BY consumer_id
ORDER BY total_transaction_amount DESC
LIMIT 5;

%%sql
SELECT wr.region_id, wr.region_name, COUNT(DISTINCT un.consumer_id) AS num_of_customers
FROM world_regions wr
LEFT JOIN user_nodes un ON wr.region_id = un.region_id
GROUP BY wr.region_id, wr.region_name
HAVING num_of_customers > 0
ORDER BY num_of_customers DESC;

%%sql
SELECT wr.region_id, wr.region_name, COUNT(DISTINCT un.consumer_id) AS num_of_customers
FROM world_regions wr
LEFT JOIN user_nodes un ON wr.region_id = un.region_id
GROUP BY wr.region_id, wr.region_name
HAVING num_of_customers > 0
ORDER BY num_of_customers DESC;

%%sql
WITH deposit_summary AS
(
SELECT consumer_id,
transaction_type AS txn_type,
COUNT(*) AS deposit_count,
SUM(transaction_amount) AS deposit_amount
FROM user_transaction
WHERE transaction_type = 'deposit'
GROUP BY consumer_id, transaction_type
)
SELECT txn_type,
ROUND(AVG(deposit_count), 0) AS avg_deposit_count,
ROUND(AVG(deposit_amount), 0) AS avg_deposit_amount
FROM deposit_summary
GROUP BY txn_type;

%%sql
SELECT wr.region_name, COUNT(ut.consumer_id) AS num_transactions
FROM user_transaction ut
INNER JOIN user_nodes un ON ut.consumer_id = un.consumer_id
INNER JOIN world_regions wr ON un.region_id = wr.region_id
GROUP BY wr.region_name
ORDER BY num_transactions DESC;