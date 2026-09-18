"""Approximate Indian state / UT lookup from latitude and longitude."""

from typing import Dict, List, Optional, Tuple

# name -> (lat_min, lat_max, lon_min, lon_max)
# Smaller / enclave regions are listed before the larger states that
# geographically surround them, so a simple first-match lookup resolves
# overlaps sensibly (e.g. Delhi is checked before Haryana/UP).
_BOUNDS: Dict[str, Tuple[float, float, float, float]] = {
    "Goa": (14.9, 15.8, 73.7, 74.3),
    "Delhi": (28.4, 28.9, 76.8, 77.4),
    "Chandigarh": (30.6, 30.8, 76.7, 76.9),
    "Puducherry": (11.8, 12.1, 79.7, 79.9),
    "Sikkim": (27.0, 28.2, 88.0, 88.9),
    "Tripura": (22.9, 24.5, 91.1, 92.3),
    "Mizoram": (21.9, 24.6, 92.2, 93.5),
    "Nagaland": (25.2, 27.0, 93.3, 95.2),
    "Manipur": (23.8, 25.7, 93.0, 94.8),
    "Meghalaya": (25.0, 26.1, 89.8, 92.8),
    "Haryana": (27.6, 30.9, 74.4, 77.6),
    "Punjab": (29.5, 32.5, 73.8, 76.9),
    "Himachal Pradesh": (30.2, 33.3, 75.5, 79.0),
    "Uttarakhand": (28.7, 31.5, 77.5, 81.1),
    "Kerala": (8.1, 12.8, 74.8, 77.4),
    "Tamil Nadu": (8.1, 13.6, 76.2, 80.4),
    "Karnataka": (11.5, 18.5, 74.0, 78.6),
    "Andhra Pradesh": (12.6, 19.9, 76.8, 84.8),
    "Telangana": (15.8, 19.9, 77.2, 81.3),
    "Odisha": (17.8, 22.6, 81.3, 87.6),
    "Chhattisgarh": (17.8, 24.1, 80.2, 84.4),
    "Jharkhand": (21.9, 25.4, 83.3, 87.9),
    "West Bengal": (21.5, 27.2, 85.8, 89.9),
    "Bihar": (24.2, 27.6, 83.3, 88.3),
    "Assam": (24.1, 28.2, 89.7, 96.0),
    "Arunachal Pradesh": (26.6, 29.5, 91.6, 97.4),
    "Madhya Pradesh": (21.0, 26.9, 74.0, 82.8),
    "Maharashtra": (15.6, 22.0, 72.6, 80.9),
    "Gujarat": (20.0, 24.7, 68.1, 74.5),
    "Rajasthan": (23.0, 30.2, 69.4, 78.3),
    "Uttar Pradesh": (23.8, 30.5, 77.0, 84.7),
    "Jammu and Kashmir": (32.2, 37.1, 73.7, 80.3),
    "Ladakh": (32.2, 36.0, 75.7, 79.9),
    "Andaman and Nicobar Islands": (6.5, 13.7, 92.2, 93.9),
}

ALL_STATES: List[str] = list(_BOUNDS.keys())


def infer_state(lat, lon) -> Optional[str]:
    """Best-effort state/UT name for a device lat/lon, or None."""
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        return None

    for name, (lat_min, lat_max, lon_min, lon_max) in _BOUNDS.items():
        if lat_min <= lat_f <= lat_max and lon_min <= lon_f <= lon_max:
            return name

    return None
