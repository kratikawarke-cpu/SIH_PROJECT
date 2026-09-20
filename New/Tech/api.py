"""
Minimal HTTP API -- stdlib only (json + http.server), no fastapi/flask
required, so it drops into any existing Python backend with zero new
dependencies beyond numpy.

POST /v1/verify
    body: {
      "transaction_id": "txn_123",
      "sigma_ms": 1.0,                 # your measured/assumed timing noise
      "v_km_per_ms": 200.0,            # optional, defaults to fiber speed
      "nodes": [                       # >=4, supplied by YOUR backend
        {"name": "node-a", "lat": 22.71, "lon": 75.85, "arrival_time_ms": 12.4},
        ...
      ]
    }
    -> estimated lat/lon, clock bias, 95% error ellipse (km),
       verified-sector polygon (GeoJSON) and area (km^2)

GET /v1/demo/indore[?sigma_ms=0.05&true_lat=22.735&true_lon=75.875]
    self-contained demo -- no body, no external backend needed.
    Defaults to near-ideal hardware-timestamp noise (0.05ms); raise
    sigma_ms to see the fix degrade under realistic internet jitter.

Run:
    python api.py
    curl http://localhost:8000/v1/demo/indore
    curl "http://localhost:8000/v1/demo/indore?sigma_ms=5"
"""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

from tdoa_solver import (
    Node, latlon_to_local, local_to_latlon, solve_wls, error_ellipse,
    isochrone_radii, rtt_isochrone_radii, combine_radii, verified_sector_geojson,
)
from demo_data import make_synthetic_transaction


def run_verify(payload):
    nodes_in = payload.get("nodes", [])
    if len(nodes_in) < 4:
        raise ValueError("Need at least 4 node readings (nodes array).")

    sigma_ms = float(payload.get("sigma_ms", 1.0))
    v = float(payload.get("v_km_per_ms", 200.0))

    node_objs = [Node(n["name"], float(n["lat"]), float(n["lon"])) for n in nodes_in]
    coords, ref = latlon_to_local(node_objs)
    t_meas = [float(n["arrival_time_ms"]) for n in nodes_in]

    x, y, b, A, converged = solve_wls(coords, t_meas, v_km_per_ms=v)
    lat, lon = local_to_latlon(x, y, ref)

    result = {
        "transaction_id": payload.get("transaction_id"),
        "converged": converged,
        "estimated_lat": lat,
        "estimated_lon": lon,
        "clock_bias_ms": b,
    }
    if not converged:
        result["warning"] = (
            "Solve is numerically unstable for this input -- the timing noise "
            "is too large relative to node spacing to trust a fix. Treat as "
            "no-result rather than acting on estimated_lat/estimated_lon."
        )
        return result

    semi_major, semi_minor, angle_deg, _ = error_ellipse(A, sigma_ms)
    radii = isochrone_radii(coords, x, y, v, t_meas, b)

    # Optional independent tightening: if your edge servers also measured a
    # round-trip ping to the client (rtt_ms per node), that bound needs no
    # cross-node clock sync and can only shrink the verified area further.
    rtt_present = all("rtt_ms" in n for n in nodes_in)
    if rtt_present:
        rtt_radii = rtt_isochrone_radii([float(n["rtt_ms"]) for n in nodes_in], v)
        radii = combine_radii(radii, rtt_radii)
        result["rtt_radii_km"] = rtt_radii

    sector_geojson, sector_area = verified_sector_geojson(
        x, y, semi_major, semi_minor, angle_deg, coords, radii, ref
    )
    result["error_ellipse_km"] = {
        "semi_major": semi_major, "semi_minor": semi_minor, "angle_deg": angle_deg,
    }
    result["verified_sector_km2"] = sector_area
    result["verified_sector_geojson"] = sector_geojson
    result["rtt_bound_applied"] = rtt_present
    # Echoed so the frontend can draw node markers + isochrone circles
    # without recomputing anything -- purely for the animation, not the math.
    result["nodes"] = [{"name": n.name, "lat": n.lat, "lon": n.lon} for n in node_objs]
    result["isochrone_radii_km"] = radii
    if semi_major > 1000:
        result["warning"] = (
            "Fix landed on a coordinate but the 95% ellipse is enormous -- "
            "not reliable enough to act on."
        )
    return result


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        # CORS preflight for the POST /v1/verify call from a browser page.
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/v1/demo/indore":
            q = parse_qs(parsed.query)
            sigma = float(q.get("sigma_ms", [0.05])[0])
            true_lat = float(q.get("true_lat", [22.7350])[0])
            true_lon = float(q.get("true_lon", [75.8750])[0])
            use_rtt = q.get("use_rtt", ["0"])[0] == "1"
            rtt_sigma = float(q.get("rtt_sigma_ms", [2.0])[0]) if use_rtt else None
            gen = make_synthetic_transaction(
                true_lat=true_lat, true_lon=true_lon, sigma_ms=sigma,
                rtt_sigma_ms=rtt_sigma,
            )
            if use_rtt:
                nodes, t_meas, sigma, true_bias, true_latlon, rtt_meas = gen
            else:
                nodes, t_meas, sigma, true_bias, true_latlon = gen
                rtt_meas = None
            node_dicts = []
            for i, (n, t) in enumerate(zip(nodes, t_meas)):
                nd = {"name": n.name, "lat": n.lat, "lon": n.lon, "arrival_time_ms": t}
                if rtt_meas is not None:
                    nd["rtt_ms"] = rtt_meas[i]
                node_dicts.append(nd)
            payload = {
                "transaction_id": "demo-indore",
                "sigma_ms": sigma,
                "nodes": node_dicts,
            }
            try:
                result = run_verify(payload)
                result["true_lat"], result["true_lon"] = true_latlon
                result["true_clock_bias_ms"] = true_bias
                self._send(200, result)
            except Exception as e:
                self._send(400, {"error": str(e)})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/v1/verify":
            length = int(self.headers.get("Content-Length", 0))
            try:
                payload = json.loads(self.rfile.read(length) or b"{}")
                self._send(200, run_verify(payload))
            except Exception as e:
                self._send(400, {"error": str(e)})
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    port = 8000
    print(f"Serving on http://localhost:{port}")
    print(f"Try:  curl http://localhost:{port}/v1/demo/indore")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
