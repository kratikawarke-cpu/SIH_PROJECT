# Dashboard visualizations

Three standalone HTML files, no build step -- just open them in a browser.

- `tdoa_animation.html` -- simple Leaflet map, baked-in demo data, node
  pulses converging to a fix + error ellipse. Good for a first look.
- `tdoa_cinematic.html` -- MapLibre GL + Turf.js replay of the full
  dashboard sequence (camera fly-to, red ellipse fade-in, live
  turf.intersect() against isochrone circles, red-to-green snap).
  Baked-in demo data, no server required.
- `tdoa_live.html` -- same cinematic sequence, but fetches real numbers
  from your running `api.py` instead of baked-in data.
  1. `python ../api.py` (starts on http://localhost:8000)
  2. open this file in a browser
  3. tap "Fetch + Play" -- edit the URL box to hit
     `/v1/demo/indore?sigma_ms=...&use_rtt=1&rtt_sigma_ms=...`
     to see the RTT-tightening effect, or point it at your own
     deployment once `/v1/verify` is live.

All three are presentation only -- none of them do any of the actual
math. If a transaction doesn't converge or its ellipse is enormous,
each of these correctly shows red/"unreliable" instead of a fake
green result.
