"""Small shared constants; no collectors are registered on import."""
SCORE_BUCKETS = tuple(i / 20 for i in range(21))
LATENCY_BUCKETS = (.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 15, 30)

