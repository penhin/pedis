import math

from ..errors import InvalidGeoCoordinateError

__all__ = [
    "encode_geohash_score",
    "decode_geohash_score",
    "distance",
    "distance_to_meters",
    "meters_to_distance",
    "validate_point",
]

MIN_LATITUDE = -85.05112878
MAX_LATITUDE = 85.05112878
MIN_LONGITUDE = -180
MAX_LONGITUDE = 180

LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
GEO_STEP = 26
GEO_SCALE = 1 << GEO_STEP
EARTH_RADIUS_IN_METERS = 6372797.560856

UNIT_FACTORS = {
    b"m": 1.0,
    b"km": 1000.0,
    b"mi": 1609.344,
    b"ft": 0.3048,
}

def validate_point(lon: float, lat: float):
    if not (MIN_LONGITUDE <= lon <= MAX_LONGITUDE):
        raise InvalidGeoCoordinateError
    if not (MIN_LATITUDE <= lat <= MAX_LATITUDE):
        raise InvalidGeoCoordinateError

def _encode_range(value: float, min_value: float, value_range: float) -> int:
    encoded = int((value - min_value) * GEO_SCALE / value_range)
    return min(max(encoded, 0), GEO_SCALE - 1)

def _interleave(lon_bits: int, lat_bits: int) -> int:
    score = 0
    for i in range(GEO_STEP):
        shift = GEO_STEP - i - 1
        score = (score << 1) | ((lon_bits >> shift) & 1)
        score = (score << 1) | ((lat_bits >> shift) & 1)
    return score

def _deinterleave(score: int) -> tuple[int, int]:
    lon_bits = 0
    lat_bits = 0
    for i in range(GEO_STEP):
        shift = (GEO_STEP - i - 1) * 2
        lon_bits = (lon_bits << 1) | ((score >> (shift + 1)) & 1)
        lat_bits = (lat_bits << 1) | ((score >> shift) & 1)
    return lon_bits, lat_bits

def encode_geohash_score(lon: float, lat: float) -> float:
    validate_point(lon, lat)
    lon_bits = _encode_range(lon, MIN_LONGITUDE, LONGITUDE_RANGE)
    lat_bits = _encode_range(lat, MIN_LATITUDE, LATITUDE_RANGE)
    return float(_interleave(lon_bits, lat_bits))

def decode_geohash_score(score: float) -> tuple[float, float]:
    lon_bits, lat_bits = _deinterleave(int(score))
    lon = MIN_LONGITUDE + ((lon_bits + 0.5) * LONGITUDE_RANGE / GEO_SCALE)
    lat = MIN_LATITUDE + ((lat_bits + 0.5) * LATITUDE_RANGE / GEO_SCALE)
    return lon, lat

def distance(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = lat2_rad - lat1_rad
    dlon = math.radians(lon2 - lon1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_IN_METERS * math.asin(math.sqrt(a))

def distance_to_meters(value: float, unit: bytes) -> float:
    factor = UNIT_FACTORS.get(unit.lower())
    if factor is None:
        raise ValueError("unsupported unit")
    return value * factor

def meters_to_distance(value: float, unit: bytes) -> float:
    factor = UNIT_FACTORS.get(unit.lower())
    if factor is None:
        raise ValueError("unsupported unit")
    return value / factor
