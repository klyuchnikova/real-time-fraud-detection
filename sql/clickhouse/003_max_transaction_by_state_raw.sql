-- Direct query for correctness and baseline/optimized-table comparison.
SELECT
    us_state,
    tupleElement(winner, 1) AS cat_id,
    tupleElement(winner, 2) AS max_amount,
    tupleElement(winner, 3) AS transaction_id
FROM
(
    SELECT
        us_state,
        argMax(tuple(toString(cat_id), amount, transaction_id),
               tuple(amount, transaction_id)) AS winner
    FROM {database:Identifier}.transactions
    WHERE dataset_id = {dataset_id:String}
    GROUP BY us_state
)
ORDER BY us_state;
