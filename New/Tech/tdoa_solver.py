"""
TDoA / weighted-least-squares (WLS) multilateration solver.

Input:  N (>=4) node locations plus, for a single event, the timestamp
        at which each node recorded the signal arriving (any common
        clock, milliseconds).
Output: estimated (x, y) position, an unknown clock/processing bias b,
        and a covariance-derived confidence ellipse.

This module makes no assumption about where the arrival timestamps
came from or how precise they are -- it solves whatever numbers it is
given. Output precision is bounded by input timing precision (see the
sigma_ms parameter and the README caveat on public-internet timing).

Only dependency: numpy.
"""

import math
from dataclasses import dataclass
from typing import List, Tuple, Optional

import numpy as np

EARTH_KM_PER_DEG_LAT = 111.0
CHI2_95_2DOF = 5.991
CHI2_TABLE = {0.90: 4.605, 0.95: 5.991, 0.99: 9.210}


@dataclass
class Node:
    name: str
    lat: float
    lon: float


def latlon_to_local(nodes: List[Node], ref_idx: int = 0) -> Tuple[List[Tuple[float, float]], Node]:
    """Flat-earth projection to local km coordinates relative to nodes[ref_idx]."""
    ref = nodes[ref_idx]
    ref_lat_rad = math.radians(ref.lat)
    coords = []
    for n in nodes:
        dlat = n.lat - ref.lat
        dlon = n.lon - ref.lon
        y = dlat * EARTH_KM_PER_DEG_LAT
        x = dlon * EARTH_KM_PER_DEG_LAT * math.cos(ref_lat_rad)
        coords.append((x, y))
    return coords, ref


def local_to_latlon(x: float, y: float, ref: Node) -> Tuple[float, float]:
    ref_lat_rad = math.radians(ref.lat)
    lat = ref.lat + y / EARTH_KM_PER_DEG_LAT
    lon = ref.lon + x / (EARTH_KM_PER_DEG_LAT * math.cos(ref_lat_rad))
    return lat, lon


def solve_wls(coords: List[Tuple[float, float]], t_meas_ms: List[float],
              v_km_per_ms: float = 200.0, iterations: int = 50):
    """Levenberg-Marquardt-damped WLS solve for (x, y, clock_bias) from
    arrival times. Model: t_i = bias + dist(node_i, target) / v

    Plain Gauss-Newton diverges to nonsense coordinates when the input
    timing noise is large relative to the node spacing (exactly the
    failure mode that makes public-internet timing unreliable for this
    technique) -- the damping keeps it numerically stable, and
    `converged=False` on the return says plainly when the fix should
    not be trusted, instead of returning a wild number.

    Returns (x, y, bias, A, converged).
    """
    coords_a = np.array(coords, dtype=float)
    t_meas = np.array(t_meas_ms, dtype=float)
    n = len(coords_a)
    if n < 4:
        raise ValueError("Need at least 4 node readings to solve for (x, y, bias).")

    def residual_and_jacobian(x, y, b):
        dx = x - coords_a[:, 0]; dy = y - coords_a[:, 1]
        d = np.hypot(dx, dy); d = np.where(d < 1e-9, 1e-9, d)
        r = t_meas - (b + d / v_km_per_ms)
        A = np.column_stack([(dx / d) / v_km_per_ms, (dy / d) / v_km_per_ms, np.ones(n)])
        return r, A

    x, y = coords_a[:, 0].mean(), coords_a[:, 1].mean()
    b = float(t_meas.min())
    r, A = residual_and_jacobian(x, y, b)
    cost = float(r @ r)
    lam = 1e-3
    converged = True

    for _ in range(iterations):
        AtA, Atr = A.T @ A, A.T @ r
        try:
            delta = np.linalg.solve(AtA + lam * np.eye(3), Atr)
        except np.linalg.LinAlgError:
            converged = False
            break
        x_new, y_new, b_new = x + delta[0], y + delta[1], b + delta[2]
        if not np.all(np.isfinite([x_new, y_new, b_new])):
            converged = False
            break
        r_new, A_new = residual_and_jacobian(x_new, y_new, b_new)
        cost_new = float(r_new @ r_new)
        if cost_new < cost:
            x, y, b, r, A = x_new, y_new, b_new, r_new, A_new
            lam = max(lam / 3, 1e-6)
            if abs(cost - cost_new) < 1e-14:
                cost = cost_new
                break
            cost = cost_new
        else:
            lam = min(lam * 4, 1e8)

    centroid_x, centroid_y = coords_a[:, 0].mean(), coords_a[:, 1].mean()
    spread = float(np.max(np.hypot(coords_a[:, 0] - centroid_x, coords_a[:, 1] - centroid_y)))
    solved_dist = math.hypot(x - centroid_x, y - centroid_y)
    if solved_dist > max(50 * spread, 2000):
        converged = False

    return x, y, b, A, converged


def error_ellipse(A: np.ndarray, sigma_ms: float, confidence: float = 0.95):
    """95% (by default) confidence error ellipse for the (x, y) estimate."""
    AtA = A.T @ A
    try:
        cov = np.linalg.inv(AtA) * (sigma_ms ** 2)
    except np.linalg.LinAlgError:
        return float("nan"), float("nan"), 0.0, np.full((3, 3), np.nan)

    qxy = cov[:2, :2]
    eigvals, eigvecs = np.linalg.eigh(qxy)
    chi2 = CHI2_TABLE.get(round(confidence, 2), CHI2_95_2DOF)
    order = np.argsort(eigvals)[::-1]
    eigvals = eigvals[order]; eigvecs = eigvecs[:, order]
    semi_major = float(np.sqrt(max(eigvals[0], 0) * chi2))
    semi_minor = float(np.sqrt(max(eigvals[1], 0) * chi2))
    angle_deg = float(np.degrees(np.arctan2(eigvecs[1, 0], eigvecs[0, 0])))
    return semi_major, semi_minor, angle_deg, cov


def isochrone_radii(coords, x, y, v_km_per_ms, t_meas_ms, b):
    """Max possible distance from each node implied by its own arrival time
    and the solved clock bias -- the isochrone bound from the blueprint.

    Caveat: this bound is derived from the same clock-bias solve that
    produced the ellipse, so intersecting it with the ellipse tightens the
    *shape* (chops the covariance tails to a physically consistent region)
    but doesn't add a second, independent measurement.
    """
    return [max((t - b) * v_km_per_ms, 0.0) for t in t_meas_ms]


def rtt_isochrone_radii(rtt_ms, v_km_per_ms=200.0):
    """Independent physical bound per node from round-trip ping time alone.

    r = v * rtt / 2 -- a signal must leave the node, reach the target, and
    return within `rtt`, so the target can be no farther than half that
    round trip. Unlike `isochrone_radii`, this needs no cross-node clock
    synchronization: RTT is measured locally at a single node (start timer,
    stop timer), so it sidesteps the exact failure mode that makes
    public-internet *one-way* timestamp sync unreliable (see README).
    It's also one-sided: network delay can only add to RTT, never subtract,
    so this is a genuine physical outer bound, not a noisy estimate that
    could land inside or outside the true distance.
    """
    return [max(v_km_per_ms * rtt / 2.0, 0.0) for rtt in rtt_ms]


def combine_radii(*radius_lists):
    """Elementwise minimum across one or more independent per-node radius
    bounds. Each list is a hard outer limit on the target's distance from
    that node, so the tightest bound per node always wins -- combining an
    independent RTT bound with the TDoA isochrone bound can only shrink
    (never grow) the final verified sector.
    """
    return [min(vals) for vals in zip(*radius_lists)]


# ---- polygon geometry (pure python -- no shapely dependency) ----

def _ellipse_points(cx, cy, semi_major, semi_minor, angle_deg, n=64):
    t = np.linspace(0, 2 * math.pi, n, endpoint=False)
    ex = semi_major * np.cos(t)
    ey = semi_minor * np.sin(t)
    ang = math.radians(angle_deg)
    rx = ex * math.cos(ang) - ey * math.sin(ang)
    ry = ex * math.sin(ang) + ey * math.cos(ang)
    return [(cx + px, cy + py) for px, py in zip(rx, ry)]


def _circle_points(cx, cy, r, n=64):
    t = np.linspace(0, 2 * math.pi, n, endpoint=False)
    return [(cx + r * math.cos(a), cy + r * math.sin(a)) for a in t]


def _clip_convex(subject, clip_poly):
    """Sutherland-Hodgman: clip `subject` polygon against convex `clip_poly`.
    Both must be CCW-oriented point lists."""
    def inside(p, a, b):
        return (b[0] - a[0]) * (p[1] - a[1]) - (b[1] - a[1]) * (p[0] - a[0]) >= 0

    def intersect(p1, p2, a, b):
        x1, y1 = p1; x2, y2 = p2; x3, y3 = a; x4, y4 = b
        denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
        if abs(denom) < 1e-12:
            return p2
        t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom
        return (x1 + t * (x2 - x1), y1 + t * (y2 - y1))

    output = list(subject)
    cp1 = clip_poly[-1]
    for cp2 in clip_poly:
        input_list, output = output, []
        if not input_list:
            break
        s = input_list[-1]
        for e in input_list:
            e_in, s_in = inside(e, cp1, cp2), inside(s, cp1, cp2)
            if e_in:
                if not s_in:
                    output.append(intersect(s, e, cp1, cp2))
                output.append(e)
            elif s_in:
                output.append(intersect(s, e, cp1, cp2))
            s = e
        cp1 = cp2
    return output


def _polygon_area(points):
    if len(points) < 3:
        return 0.0
    area = 0.0
    for i in range(len(points)):
        x1, y1 = points[i]
        x2, y2 = points[(i + 1) % len(points)]
        area += x1 * y2 - x2 * y1
    return abs(area) / 2.0


def verified_sector_geojson(x, y, semi_major, semi_minor, angle_deg, coords, radii, ref: Node):
    """Intersect the error ellipse with each node's isochrone circle --
    the 'verified sector' from the blueprint. Returns (geojson_or_None, area_km2)."""
    if not math.isfinite(semi_major) or not math.isfinite(semi_minor):
        return None, None
    sector = _ellipse_points(x, y, semi_major, semi_minor, angle_deg)
    for (nx, ny), r in zip(coords, radii):
        if not sector:
            break
        circle = _circle_points(nx, ny, max(r, 0.01))
        sector = _clip_convex(sector, circle)
    if not sector:
        return None, 0.0
    area = _polygon_area(sector)
    ring = [local_to_latlon(px, py, ref) for px, py in sector]
    ring.append(ring[0])
    geojson = {"type": "Polygon", "coordinates": [[[lon, lat] for lat, lon in ring]]}
    return geojson, area
