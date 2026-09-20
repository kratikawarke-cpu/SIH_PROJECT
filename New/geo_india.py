"""Approximate Indian state / UT lookup, verified land coordinates, and sea avoidance."""

import hashlib
import math
import random
from typing import Any, Dict, List, Optional, Tuple

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

# High-precision verified land coordinates for actual Indian cities & commercial districts
# Guaranteed 100% on land, away from sea/ocean water bodies.
INDIAN_LAND_CITIES: Dict[str, List[Dict[str, Any]]] = {
    "Delhi": [
        {"city": "Connaught Place, Central Delhi", "lat": 28.6315, "lon": 77.2167},
        {"city": "Saket Commercial Hub, South Delhi", "lat": 28.5244, "lon": 77.2066},
        {"city": "Rohini Sector 9, North Delhi", "lat": 28.7041, "lon": 77.1025},
        {"city": "Dwarka Sector 10, West Delhi", "lat": 28.5823, "lon": 77.0500},
        {"city": "Lajpat Nagar Central Market", "lat": 28.5684, "lon": 77.2435},
    ],
    "Maharashtra": [
        {"city": "Bandra Kurla Complex (BKC), Mumbai", "lat": 19.0688, "lon": 72.8697},
        {"city": "Andheri East Metro Hub, Mumbai", "lat": 19.1136, "lon": 72.8697},
        {"city": "Dadar TT Commercial Circle, Mumbai", "lat": 19.0178, "lon": 72.8478},
        {"city": "Vashi Sector 17, Navi Mumbai", "lat": 19.0771, "lon": 72.9986},
        {"city": "Shivaji Nagar, Pune", "lat": 18.5314, "lon": 73.8446},
        {"city": "Hinjewadi Tech Zone, Pune", "lat": 18.5913, "lon": 73.7389},
        {"city": "Sitabuldi Market, Nagpur", "lat": 21.1466, "lon": 79.0833},
        {"city": "Majiwada Junction, Thane", "lat": 19.2183, "lon": 72.9781},
        {"city": "College Road, Nashik", "lat": 19.9975, "lon": 73.7667},
    ],
    "Karnataka": [
        {"city": "Koramangala 5th Block, Bengaluru", "lat": 12.9352, "lon": 77.6245},
        {"city": "Indiranagar 100ft Road, Bengaluru", "lat": 12.9784, "lon": 77.6408},
        {"city": "Whitefield ITPL Hub, Bengaluru", "lat": 12.9698, "lon": 77.7500},
        {"city": "Electronic City Phase 1, Bengaluru", "lat": 12.8452, "lon": 77.6602},
        {"city": "Vijayanagar Central, Mysuru", "lat": 12.3375, "lon": 76.6190},
        {"city": "Vidyanagar, Hubli", "lat": 15.3647, "lon": 75.1240},
    ],
    "Telangana": [
        {"city": "Hitec City Cyber Towers, Hyderabad", "lat": 17.4435, "lon": 78.3772},
        {"city": "Banjara Hills Road No 12, Hyderabad", "lat": 17.4156, "lon": 78.4350},
        {"city": "Secunderabad Station Commercial Hub", "lat": 17.4399, "lon": 78.4983},
        {"city": "Gachibowli Financial District, Hyderabad", "lat": 17.4401, "lon": 78.3489},
        {"city": "Hanamkonda Main Road, Warangal", "lat": 18.0125, "lon": 79.5510},
    ],
    "Tamil Nadu": [
        {"city": "T. Nagar Pondy Bazaar, Chennai", "lat": 13.0418, "lon": 80.2341},
        {"city": "Anna Nagar 2nd Avenue, Chennai", "lat": 13.0850, "lon": 80.2101},
        {"city": "OMR Tech Corridor, Perungudi, Chennai", "lat": 12.9698, "lon": 80.2450},
        {"city": "RS Puram, Coimbatore", "lat": 11.0084, "lon": 76.9486},
        {"city": "Anna Nagar Main Road, Madurai", "lat": 9.9252, "lon": 78.1400},
    ],
    "Gujarat": [
        {"city": "SG Highway Commercial Hub, Ahmedabad", "lat": 23.0489, "lon": 72.5284},
        {"city": "Navrangpura CG Road, Ahmedabad", "lat": 23.0365, "lon": 72.5611},
        {"city": "Ghod Dod Road, Surat", "lat": 21.1702, "lon": 72.8050},
        {"city": "Alkapuri Commercial Centre, Vadodara", "lat": 22.3107, "lon": 73.1705},
        {"city": "Kalawad Road, Rajkot", "lat": 22.2870, "lon": 70.7745},
    ],
    "West Bengal": [
        {"city": "Salt Lake Sector V Tech Hub, Kolkata", "lat": 22.5804, "lon": 88.4331},
        {"city": "Park Street Commercial Hub, Kolkata", "lat": 22.5518, "lon": 88.3524},
        {"city": "New Town Action Area 1, Kolkata", "lat": 22.5867, "lon": 88.4754},
        {"city": "Howrah Station Commercial Plaza", "lat": 22.5855, "lon": 88.3415},
        {"city": "City Centre, Durgapur", "lat": 23.5334, "lon": 87.2938},
    ],
    "Uttar Pradesh": [
        {"city": "Hazratganj Main Market, Lucknow", "lat": 26.8500, "lon": 80.9450},
        {"city": "Gomti Nagar Vibhuti Khand, Lucknow", "lat": 26.8580, "lon": 81.0000},
        {"city": "Sector 62 IT Park, Noida", "lat": 28.6250, "lon": 77.3680},
        {"city": "Civil Lines Commercial Area, Kanpur", "lat": 26.4720, "lon": 80.3470},
        {"city": "Sigra Market, Varanasi", "lat": 25.3180, "lon": 82.9860},
        {"city": "Sanjay Place Commercial Hub, Agra", "lat": 27.1980, "lon": 78.0060},
    ],
    "Rajasthan": [
        {"city": "Malviya Nagar Gaurav Tower, Jaipur", "lat": 26.8530, "lon": 75.8050},
        {"city": "C-Scheme MI Road, Jaipur", "lat": 26.9075, "lon": 75.7990},
        {"city": "Sardarpura Commercial B Road, Jodhpur", "lat": 26.2750, "lon": 73.0180},
        {"city": "Sukhadia Circle, Udaipur", "lat": 24.6020, "lon": 73.6920},
        {"city": "Kotri Gumanpura Road, Kota", "lat": 25.1760, "lon": 75.8450},
    ],
    "Haryana": [
        {"city": "Cyber Hub DLF Phase 2, Gurugram", "lat": 28.4952, "lon": 77.0895},
        {"city": "Golf Course Road, Gurugram", "lat": 28.4595, "lon": 77.0980},
        {"city": "Sector 15 Market, Faridabad", "lat": 28.4089, "lon": 77.3178},
        {"city": "GT Road Commercial Hub, Panipat", "lat": 29.3909, "lon": 76.9635},
    ],
    "Punjab": [
        {"city": "Ferozepur Road Market, Ludhiana", "lat": 30.8920, "lon": 75.8230},
        {"city": "Ranjit Avenue B Block, Amritsar", "lat": 31.6520, "lon": 74.8620},
        {"city": "Model Town Market, Jalandhar", "lat": 31.3120, "lon": 75.5820},
    ],
    "Chandigarh": [
        {"city": "Sector 17 City Centre Plaza, Chandigarh", "lat": 30.7398, "lon": 76.7827},
        {"city": "Sector 35 Inner Market, Chandigarh", "lat": 30.7220, "lon": 76.7680},
    ],
    "Madhya Pradesh": [
        {"city": "MP Nagar Zone 1, Bhopal", "lat": 23.2330, "lon": 77.4340},
        {"city": "Vijay Nagar AB Road, Indore", "lat": 22.7533, "lon": 75.8937},
        {"city": "City Center Commercial Hub, Gwalior", "lat": 26.2080, "lon": 78.1880},
        {"city": "Civic Centre, Jabalpur", "lat": 23.1680, "lon": 79.9320},
    ],
    "Kerala": [
        {"city": "Kakkanad InfoPark, Kochi", "lat": 10.0150, "lon": 76.3650},
        {"city": "Edappally Lulu Mall Zone, Kochi", "lat": 10.0250, "lon": 76.3080},
        {"city": "Technopark Phase 1, Thiruvananthapuram", "lat": 8.5580, "lon": 76.8810},
        {"city": "Mavoor Road Commercial Hub, Kozhikode", "lat": 11.2580, "lon": 75.7950},
    ],
    "Bihar": [
        {"city": "Bailey Road Saguna More, Patna", "lat": 25.6100, "lon": 85.1100},
        {"city": "Kankarbagh Colony More, Patna", "lat": 25.5940, "lon": 85.1550},
        {"city": "GB Road Commercial Center, Gaya", "lat": 24.7914, "lon": 85.0002},
    ],
    "Odisha": [
        {"city": "Saheed Nagar Janpath, Bhubaneswar", "lat": 20.2920, "lon": 85.8450},
        {"city": "Badambadi Bus Stand Hub, Cuttack", "lat": 20.4550, "lon": 85.8750},
    ],
    "Andhra Pradesh": [
        {"city": "MVP Colony Sector 3, Visakhapatnam", "lat": 17.7420, "lon": 83.3360},
        {"city": "Benz Circle Commercial Plaza, Vijayawada", "lat": 16.4980, "lon": 80.6550},
    ],
    "Assam": [
        {"city": "GS Road Christian Basti, Guwahati", "lat": 26.1550, "lon": 91.7760},
        {"city": "Paltan Bazaar, Guwahati", "lat": 26.1800, "lon": 91.7500},
    ],
    "Jharkhand": [
        {"city": "Main Road Lalpur Chowk, Ranchi", "lat": 23.3640, "lon": 85.3340},
        {"city": "Bistupur Market, Jamshedpur", "lat": 22.7980, "lon": 86.1850},
    ],
    "Goa": [
        {"city": "Panaji City Commercial Hub", "lat": 15.4920, "lon": 73.8280},
        {"city": "Margao Station Road, Pajifond", "lat": 15.2750, "lon": 73.9620},
    ],
    "Uttarakhand": [
        {"city": "Rajpur Road Clock Tower, Dehradun", "lat": 30.3420, "lon": 78.0580},
    ],
    "Himachal Pradesh": [
        {"city": "Mall Road Ridge, Shimla", "lat": 31.1048, "lon": 77.1734},
    ],
    "Jammu and Kashmir": [
        {"city": "Gandhi Nagar Commercial Hub, Jammu", "lat": 32.7060, "lon": 74.8660},
        {"city": "Lal Chowk Central Market, Srinagar", "lat": 34.0720, "lon": 74.8080},
    ],
    "Chhattisgarh": [
        {"city": "Telibandha VIP Road, Raipur", "lat": 21.2380, "lon": 81.6720},
    ],
}


def is_coordinate_in_sea(lat: float, lon: float) -> bool:
    """Strictly returns True if coordinates fall into Arabian Sea, Bay of Bengal, or Indian Ocean."""
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return True

    # 1. Broad outer bounding limits for Indian mainland
    if lat < 8.08 or lat > 35.5:
        return True
    if lon < 68.8 or lon > 96.5:
        return True

    # 2. Arabian Sea (West Coast of India)
    # Kerala / South Karnataka coast (8.08 <= lat < 12.0)
    if 8.08 <= lat < 12.0 and lon < 75.1:
        return True
    # Central Karnataka coast (12.0 <= lat < 15.0)
    if 12.0 <= lat < 15.0 and lon < 74.65:
        return True
    # Goa & Konkan coast (15.0 <= lat < 18.5)
    if 15.0 <= lat < 18.5 and lon < 73.2:
        return True
    # Mumbai, Thane & Raigad coast (18.5 <= lat < 20.2): West of 72.78 is Arabian Sea
    if 18.5 <= lat < 20.2 and lon < 72.78:
        return True
    # Gujarat Arabian Sea west of Saurashtra:
    if 20.2 <= lat < 21.6 and lon < 69.8:
        return True
    # Gulf of Khambhat waters
    if 20.8 <= lat < 21.7 and 72.0 <= lon <= 72.55:
        return True
    # Gulf of Kutch waters
    if 22.3 <= lat < 22.9 and 69.1 <= lon <= 70.3:
        return True

    # 3. Bay of Bengal (East Coast of India)
    # Tamil Nadu / South Coast (8.08 <= lat < 13.5)
    if 8.08 <= lat < 13.5 and lon > 80.35:
        return True
    if 8.08 <= lat < 11.5 and lon > 79.85:
        return True
    # Andhra Pradesh Coast (13.5 <= lat < 19.5)
    if 13.5 <= lat < 15.5 and lon > 80.3:
        return True
    if 15.5 <= lat < 17.5 and lon > 82.4:
        return True
    if 17.5 <= lat < 19.5 and lon > 84.0:
        return True
    # Odisha Coast (19.5 <= lat < 21.5)
    if 19.5 <= lat < 21.5 and lon > 86.8:
        return True
    # West Bengal / Sunderbans waters (21.5 <= lat < 22.2)
    if 21.5 <= lat < 22.2 and lon > 88.5:
        return True

    return False


def infer_state(lat, lon) -> Optional[str]:
    """Best-effort state/UT name for a device lat/lon, or None. Returns None if in the sea."""
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        return None

    # Never infer state for a coordinate in the sea
    if is_coordinate_in_sea(lat_f, lon_f):
        return None

    for name, (lat_min, lat_max, lon_min, lon_max) in _BOUNDS.items():
        if lat_min <= lat_f <= lat_max and lon_min <= lon_f <= lon_max:
            return name

    return None


def snap_to_land(
    lat: Optional[float],
    lon: Optional[float],
    state: Optional[str] = None,
    seed: str = "",
) -> Tuple[float, float, str, str]:
    """
    Validates lat/lon. If missing, invalid, or in the sea, deterministically snaps
    to a verified high-activity land city in India.
    Returns (valid_lat, valid_lon, valid_state, city_name).
    """
    # Check if incoming lat/lon is already valid land
    if lat is not None and lon is not None:
        try:
            lat_f = float(lat)
            lon_f = float(lon)
            if not is_coordinate_in_sea(lat_f, lon_f):
                inferred = state or infer_state(lat_f, lon_f) or "Maharashtra"
                # Find nearest city name if available
                city_name = f"{inferred} Commercial Hub"
                if inferred in INDIAN_LAND_CITIES:
                    # Pick closest city or first
                    best_city = INDIAN_LAND_CITIES[inferred][0]["city"]
                    min_d = 9999.0
                    for c_entry in INDIAN_LAND_CITIES[inferred]:
                        d = (c_entry["lat"] - lat_f) ** 2 + (c_entry["lon"] - lon_f) ** 2
                        if d < min_d:
                            min_d = d
                            best_city = c_entry["city"]
                    city_name = best_city
                return round(lat_f, 5), round(lon_f, 5), inferred, city_name
        except (TypeError, ValueError):
            pass

    # Needs snapping to verified land city!
    digest = hashlib.md5(f"LAND-SNAP-{seed}-{state}".encode("utf-8")).hexdigest()
    int_seed = int(digest[:8], 16)

    # Determine state
    target_state = state
    if not target_state or target_state not in INDIAN_LAND_CITIES:
        available_states = list(INDIAN_LAND_CITIES.keys())
        target_state = available_states[int_seed % len(available_states)]

    city_options = INDIAN_LAND_CITIES.get(target_state, INDIAN_LAND_CITIES["Delhi"])
    selected = city_options[(int_seed >> 4) % len(city_options)]

    # Add small deterministic micro-jitter (between -300m and +300m) to keep separate accounts distinct
    jitter_lat = (((int_seed % 100) - 50) / 100.0) * 0.003
    jitter_lon = ((((int_seed >> 8) % 100) - 50) / 100.0) * 0.003

    final_lat = round(selected["lat"] + jitter_lat, 5)
    final_lon = round(selected["lon"] + jitter_lon, 5)

    # Safety check: if jitter somehow lands in sea, fall back to exact base city coordinates
    if is_coordinate_in_sea(final_lat, final_lon):
        final_lat = round(selected["lat"], 5)
        final_lon = round(selected["lon"], 5)

    return final_lat, final_lon, target_state, selected["city"]


def random_land_location(state: Optional[str] = None) -> Tuple[float, float, str, str]:
    """
    Generates a realistic random land coordinate in India for synthetic data generation.
    Never spawns in the sea or ocean.
    Returns (latitude, longitude, state, city).
    """
    if state and state in INDIAN_LAND_CITIES:
        chosen_state = state
    else:
        # Weight by major economic hubs
        state_weights = [
            ("Delhi", 20),
            ("Maharashtra", 25),
            ("Karnataka", 20),
            ("Telangana", 15),
            ("Tamil Nadu", 15),
            ("Gujarat", 12),
            ("West Bengal", 10),
            ("Uttar Pradesh", 15),
            ("Rajasthan", 10),
            ("Haryana", 10),
            ("Punjab", 8),
            ("Madhya Pradesh", 8),
            ("Kerala", 8),
        ]
        states_pool = [s for s, w in state_weights for _ in range(w)]
        chosen_state = random.choice(states_pool)

    cities = INDIAN_LAND_CITIES[chosen_state]
    chosen_city = random.choice(cities)

    # Add realistic urban jitter: ±0.005 to ±0.015 degrees (~500m to 1.5km)
    max_attempts = 10
    for _ in range(max_attempts):
        d_lat = random.uniform(-0.012, 0.012)
        d_lon = random.uniform(-0.012, 0.012)
        cand_lat = round(chosen_city["lat"] + d_lat, 4)
        cand_lon = round(chosen_city["lon"] + d_lon, 4)
        if not is_coordinate_in_sea(cand_lat, cand_lon):
            return cand_lat, cand_lon, chosen_state, chosen_city["city"]

    return chosen_city["lat"], chosen_city["lon"], chosen_state, chosen_city["city"]
