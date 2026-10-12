-- Final query. Merge states at read time; no dependency on background merges.
SELECT
    us_state,
    tupleElement(winner, 1) AS cat_id,
    tupleElement(winner, 2) AS max_amount,
    tupleElement(winner, 3) AS transaction_id
FROM
(
    SELECT us_state, argMaxMerge(winner_state) AS winner
    FROM {database:Identifier}.state_max
    WHERE dataset_id = {dataset_id:String}
    GROUP BY us_state
)
ORDER BY us_state;
