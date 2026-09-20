"""
Self-contained Indore-area demo data.

Four synthetic 'edge nodes' around Indore, and a synthetic transaction
whose true origin, true clock bias, and true noise are known (because
we generated them) -- so the demo can show estimated-vs-actual without
needing your other backend to supply anything.
"""

import random
import math

from tdoa_solver import Node, latlon_to_local

INDORE_NODES = [
    Node("Indore",    22.7196, 75.8577),
    Node("Ujjain",    23.1765, 75.7885),
    Node("Dewas",     22.9676, 76.0534),
    Node("Pithampur", 22.6100, 75.6900),
]


def make_synthetic_transaction(true_lat=22.7350, true_lon=75.8750,
                                sigma_ms=1.0, v_km_per_ms=200.0, seed=None,
                                rtt_sigma_ms=None):
    """rtt_sigma_ms: if given, also generate a synthetic round-trip ping time
    per node (true_rtt = 2*dist/v + one-sided queuing delay + noise). Left
    None by default so existing callers/behavior are unchanged."""
    rng = random.Random(seed)
    true_node = Node("transaction", true_lat, true_lon)
    coords, ref = latlon_to_local(INDORE_NODES + [true_node], ref_idx=0)
    node_coords, tx_coord = coords[:-1], coords[-1]

    bias = 0.2 + rng.random() * 1.3
    t_meas = []
    rtt_meas = []
    for nx, ny in node_coords:
        d = math.hypot(tx_coord[0] - nx, tx_coord[1] - ny)
        noise = rng.gauss(0, sigma_ms)
        t_meas.append(bias + d / v_km_per_ms + noise)
        if rtt_sigma_ms is not None:
            # RTT noise is one-sided (extra queuing/routing delay only adds
            # to RTT, never subtracts) -- model with an exponential add-on.
            queuing = rng.expovariate(1.0 / max(rtt_sigma_ms, 1e-6))
            rtt_meas.append(2 * d / v_km_per_ms + queuing)

    if rtt_sigma_ms is not None:
        return INDORE_NODES, t_meas, sigma_ms, bias, (true_lat, true_lon), rtt_meas
    return INDORE_NODES, t_meas, sigma_ms, bias, (true_lat, true_lon)
