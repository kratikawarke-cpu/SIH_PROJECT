# TDoA / WLS transaction-location solver

A working implementation of the 4-server weighted-least-squares
multilateration math from the blueprint: given node locations and
per-node signal arrival timestamps for one event, it solves for
position + clock bias, and returns a 95% confidence error ellipse.

Only dependency: `numpy` (`pip install -r requirements.txt`).

## Run the Indore demo -- no backend needed

```
python demo.py
```

Prints a fix for a synthetic Indore-area transaction (4 nodes:
Indore, Ujjain, Dewas, Pithampur) at two noise levels, so you can see
the same math produce a usable fix or a useless one depending on
input timing quality.

Or as an HTTP endpoint (still fully self-contained):

```
python api.py
curl http://localhost:8000/v1/demo/indore
curl "http://localhost:8000/v1/demo/indore?sigma_ms=5"
curl "http://localhost:8000/v1/demo/indore?true_lat=22.70&true_lon=75.90"
```

## Wiring up your other backend

Your backend owns node locations and however it measures arrival
timing -- this service only does the math. Point it at:

```
POST /v1/verify
Content-Type: application/json

{
  "transaction_id": "txn_123",
  "sigma_ms": 1.0,
  "v_km_per_ms": 200.0,
  "nodes": [
    {"name": "node-a", "lat": 22.71, "lon": 75.85, "arrival_time_ms": 12.4},
    {"name": "node-b", "lat": 23.17, "lon": 75.78, "arrival_time_ms": 12.9},
    {"name": "node-c", "lat": 22.96, "lon": 76.05, "arrival_time_ms": 12.6},
    {"name": "node-d", "lat": 22.61, "lon": 75.69, "arrival_time_ms": 12.1}
  ]
}
```

`sigma_ms` is your honest estimate of the standard deviation of your
timing measurements -- it directly sets the size of the returned
error ellipse, so it should reflect your real measurement noise, not
a number chosen to make the ellipse look small.

Response:

```json
{
  "converged": true,
  "estimated_lat": 22.76, "estimated_lon": 75.89,
  "clock_bias_ms": 1.05,
  "error_ellipse_km": {"semi_major": 25.2, "semi_minor": 8.0, "angle_deg": 114.2},
  "verified_sector_km2": 210.4,
  "verified_sector_geojson": { "type": "Polygon", "coordinates": [...] }
}
```

If `converged` is `false`, or a `warning` field is present, treat it
as **no result** -- don't act on `estimated_lat`/`estimated_lon`. That
happens when the timing noise is too large relative to your node
spacing for the fix to mean anything, which is the normal outcome
over the public internet (see caveat below).

## Optional: tightening the verified sector with RTT

If your edge servers can also measure a round-trip ping time to the
client (`rtt_ms` per node, alongside `arrival_time_ms`), the response
gets meaningfully tighter for free:

```json
"nodes": [
  {"name": "node-a", "lat": 22.71, "lon": 75.85, "arrival_time_ms": 12.4, "rtt_ms": 3.1},
  ...
]
```

Why this helps: `arrival_time_ms` needs every node's clock to already
be synchronized to a common reference, which is exactly what public-
internet jitter ruins. `rtt_ms` needs no cross-node sync at all --
it's a single node timing its own round trip (send, wait, receive),
so `r = v * rtt / 2` is a genuine physical outer bound: light/signal
cannot travel to the client and back faster than that, full stop.

Supply `rtt_ms` on every node and the API takes the *tighter* of the
TDoA isochrone bound and the RTT bound, per node, before computing
`verified_sector_geojson` -- so the sector can only shrink, never
grow, relative to leaving it out. `rtt_bound_applied: true` in the
response confirms it was used; `rtt_radii_km` shows the raw RTT bound
per node so you can see which one won.

One real caveat: RTT precision is still bounded by how precisely your
server timestamps sends/receives -- millisecond-scale RTT jitter
still costs you ~100s of km at 200 km/ms fiber speed, same as with
one-way timing. And if RTT is biased (e.g. asymmetric routing makes
the return leg slower than the forward leg), the bound can be too
tight and wrongly zero out `verified_sector_geojson` for a fix that
was actually fine -- an empty sector with `rtt_bound_applied: true`
means "the RTT bound and the ellipse disagree," not "the user is
nowhere." Treat it as no-result and fall back to the TDoA-only
ellipse, don't discard the transaction outright.

`api.py` is stdlib-only (`http.server` + `json`) so it drops into any
Python backend with no new framework dependency. If your backend
isn't Python, call it as a sidecar over HTTP, or port `tdoa_solver.py`
directly -- it's ~150 lines of numpy with no framework coupling.

## One honest caveat

This solver is exactly the multilateration math used in GPS/cellular
positioning. What it can't do is manufacture timing precision that
isn't in your input: feed it public-internet ping times and
`sigma_ms` will realistically be several milliseconds, which the
Indore demo shows blows the "3-5km" figure out to hundreds or
thousands of km. The 3-5km claim only holds if `arrival_time_ms`
comes from something with genuinely sub-millisecond, synchronized
precision (e.g. real TDoA radio receivers, not TCP/IP round trips) --
that's a hardware/telemetry problem your other backend has to solve,
not something this math can paper over.

Also note: this Indore deployment (Indore/Ujjain/Dewas/Pithampur, all
within ~50km) is a tighter cluster than the Mumbai/Pune/Nashik/Surat
example in the original blueprint (~100-250km spread). Tighter
clustering means worse geometric dilution of precision -- you'll see
bigger ellipses here than a wider deployment would give at the same
`sigma_ms`. Swap in your real node coordinates in `demo_data.py` to
see your actual deployment's numbers.
