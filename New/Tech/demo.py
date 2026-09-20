"""
Self-contained Indore demo -- no backend, no server, just:

    python demo.py

Runs the same transaction at two different timing-precision levels so
you can see why the precision claim only holds with near-ideal timing.
"""

from tdoa_solver import (latlon_to_local, local_to_latlon, solve_wls, error_ellipse,
                          isochrone_radii, rtt_isochrone_radii, combine_radii,
                          verified_sector_geojson)
from demo_data import make_synthetic_transaction


def run(sigma_ms, seed=42):
    nodes, t_meas, sigma, true_bias, (true_lat, true_lon) = make_synthetic_transaction(
        sigma_ms=sigma_ms, seed=seed
    )
    coords, ref = latlon_to_local(nodes)
    x, y, b, A, converged = solve_wls(coords, t_meas)
    lat, lon = local_to_latlon(x, y, ref)

    print(f"  true origin:       {true_lat:.4f}, {true_lon:.4f}")
    if not converged:
        print(f"  estimated origin:  UNRELIABLE -- timing noise too large for this node")
        print(f"                     spacing to trust a fix (raw solve: {lat:.2f}, {lon:.2f})")
        return
    semi_major, semi_minor, angle_deg, _ = error_ellipse(A, sigma)
    print(f"  estimated origin:  {lat:.4f}, {lon:.4f}")
    print(f"  95% error ellipse: {semi_major:.2f} km x {semi_minor:.2f} km")
    print(f"  clock bias:        true {true_bias:.3f} ms, solved {b:.3f} ms")
    if semi_major > 1000:
        print("  note: the fix landed somewhere plausible-looking, but the ellipse")
        print("        is enormous -- this is not a location you could act on.")


def run_with_rtt(sigma_ms, rtt_sigma_ms=0.02, seed=9):
    """Same transaction, but each node also has an independent RTT ping --
    shows how much the RTT-based physical bound tightens the verified area
    on top of the TDoA-only isochrone bound.

    rtt_sigma_ms is deliberately small (~20 microseconds of one-sided
    queuing jitter): the whole point of RTT is that it's measured locally
    at one node with no cross-node clock sync needed, so it can be far more
    precise than the multi-node one-way sync this demo's sigma_ms models --
    but only if your NICs/servers actually timestamp at that precision.
    Feed it noisy or biased RTT (e.g. asymmetric routing) and the bound can
    over-constrain and wrongly zero out a real fix -- see the caveat this
    prints if that happens.
    """
    nodes, t_meas, sigma, true_bias, (true_lat, true_lon), rtt_meas = make_synthetic_transaction(
        sigma_ms=sigma_ms, seed=seed, rtt_sigma_ms=rtt_sigma_ms
    )
    coords, ref = latlon_to_local(nodes)
    x, y, b, A, converged = solve_wls(coords, t_meas)
    if not converged:
        print("  UNRELIABLE -- solve did not converge.")
        return
    semi_major, semi_minor, angle_deg, _ = error_ellipse(A, sigma)

    tdoa_radii = isochrone_radii(coords, x, y, 200.0, t_meas, b)
    rtt_radii = rtt_isochrone_radii(rtt_meas, 200.0)
    combined = combine_radii(tdoa_radii, rtt_radii)

    _, area_tdoa_only = verified_sector_geojson(x, y, semi_major, semi_minor, angle_deg, coords, tdoa_radii, ref)
    _, area_combined = verified_sector_geojson(x, y, semi_major, semi_minor, angle_deg, coords, combined, ref)

    print(f"  TDoA-only verified sector:        {area_tdoa_only:.2f} km^2" if area_tdoa_only else "  TDoA-only verified sector:        0 (empty intersection)")
    print(f"  + RTT-tightened verified sector:  {area_combined:.2f} km^2" if area_combined else "  + RTT-tightened verified sector:  0 (empty intersection -- RTT bound was too tight/biased; treat as no-result, not as 'nowhere')")
    if area_tdoa_only and area_combined:
        print(f"  reduction: {100 * (1 - area_combined / area_tdoa_only):.1f}%")


if __name__ == "__main__":
    print("Indore demo -- 4 synthetic edge nodes, 1 synthetic transaction\n")
    print("Hardware-timestamp precision (sigma = 0.03 ms):")
    run(sigma_ms=0.03)
    print("\nTypical internet jitter (sigma = 5 ms):")
    run(sigma_ms=5.0)
    print("\nEffect of adding an independent RTT ping bound (hardware timing, sigma = 0.03 ms):")
    run_with_rtt(sigma_ms=0.03)
