from app.protocol import NullBulk, NullArray
from app.storage.errors import *
from app.storage.value import distance_to_meters, meters_to_distance, validate_point

from ..core.base import CommandError, command, CommandFlag, CommandResult

WRONGTYPE_MESSAGE = "WRONGTYPE Operation against a key holding the wrong kind of value"

def _parse_float(value: bytes) -> float:
    try:
        return float(value)
    except ValueError:
        raise CommandError("ERR value is not a valid float")

def _parse_unit(unit: bytes):
    try:
        distance_to_meters(1.0, unit)
    except ValueError:
        raise CommandError("ERR unsupported unit provided. please use m, km, ft, mi")

def _format_coord(value: float) -> bytes:
    return f"{value:.17f}".encode()

def _format_distance(value: float) -> bytes:
    return f"{value:.4f}".encode()

@command("GEOADD", -4, flags=[CommandFlag.WRITE])
def geoadd_command(args, context):
    if len(args[1:]) % 3 != 0:
        raise CommandError("ERR syntax error")

    key = args[0]

    try:
        pairs = []
        for i in range(1, len(args), 3):
            lon = float(args[i])
            lat = float(args[i+1])
            member = args[i+2]
            pairs.append((lon, lat, member))

    except ValueError:
        raise CommandError("ERR value is not a valid float")
    
    try:
        return context.storage.geoadd(key, pairs)
    except WrongTypeError:
        raise CommandError(WRONGTYPE_MESSAGE)
    except InvalidGeoCoordinateError:
        raise CommandError("ERR invalid longitude,latitude pair")

@command("GEOPOS", -2, flags=[])
def geopos_command(args, context):
    key = args[0]
    members = args[1:]

    try:
        positions = context.storage.geopos(key, members)
    except WrongTypeError:
        raise CommandError(WRONGTYPE_MESSAGE)

    result = []
    for position in positions:
        if position is None:
            result.append(NullArray())
        else:
            lon, lat = position
            result.append([_format_coord(lon), _format_coord(lat)])
    return CommandResult.resp(result, propagate=False)

@command("GEODIST", -4, flags=[])
def geodist_command(args, context):
    key = args[0]
    member1 = args[1]
    member2 = args[2]
    unit = args[3] if len(args) > 3 else b"m"

    _parse_unit(unit)
    if len(args) > 4:
        raise CommandError("ERR syntax error")

    try:
        dist = context.storage.geodist(key, member1, member2)
    except WrongTypeError:
        raise CommandError(WRONGTYPE_MESSAGE)

    if dist is None:
        return CommandResult.resp(NullBulk(), propagate=False)
    return CommandResult.resp(_format_distance(meters_to_distance(dist, unit)), propagate=False)

@command("GEOSEARCH", -7, flags=[])
def geosearch_command(args, context):
    key = args[0]
    idx = 1
    center = None
    shape = None
    order = None
    count = None
    withcoord = False
    withdist = False
    withhash = False
    output_unit = b"m"

    while idx < len(args):
        token = args[idx].upper()

        if token == b"FROMMEMBER":
            if center is not None or idx + 1 >= len(args):
                raise CommandError("ERR syntax error")
            try:
                positions = context.storage.geopos(key, [args[idx + 1]])
            except WrongTypeError:
                raise CommandError(WRONGTYPE_MESSAGE)
            if positions[0] is None:
                return CommandResult.resp([], propagate=False)
            center = positions[0]
            idx += 2

        elif token == b"FROMLONLAT":
            if center is not None or idx + 2 >= len(args):
                raise CommandError("ERR syntax error")
            lon = _parse_float(args[idx + 1])
            lat = _parse_float(args[idx + 2])
            try:
                validate_point(lon, lat)
            except InvalidGeoCoordinateError:
                raise CommandError("ERR invalid longitude,latitude pair")
            center = (lon, lat)
            idx += 3

        elif token == b"BYRADIUS":
            if shape is not None or idx + 2 >= len(args):
                raise CommandError("ERR syntax error")
            radius = _parse_float(args[idx + 1])
            unit = args[idx + 2]
            _parse_unit(unit)
            if radius < 0:
                raise CommandError("ERR radius cannot be negative")
            shape = ("radius", distance_to_meters(radius, unit))
            output_unit = unit
            idx += 3

        elif token == b"BYBOX":
            if shape is not None or idx + 3 >= len(args):
                raise CommandError("ERR syntax error")
            width = _parse_float(args[idx + 1])
            height = _parse_float(args[idx + 2])
            unit = args[idx + 3]
            _parse_unit(unit)
            if width < 0 or height < 0:
                raise CommandError("ERR width or height cannot be negative")
            shape = (
                "box",
                distance_to_meters(width, unit),
                distance_to_meters(height, unit),
            )
            output_unit = unit
            idx += 4

        elif token in (b"ASC", b"DESC"):
            if order is not None:
                raise CommandError("ERR syntax error")
            order = token.decode()
            idx += 1

        elif token == b"COUNT":
            if count is not None or idx + 1 >= len(args):
                raise CommandError("ERR syntax error")
            try:
                count = int(args[idx + 1])
            except ValueError:
                raise CommandError("ERR value is not an integer or out of range")
            if count < 0:
                raise CommandError("ERR value is out of range")
            idx += 2
            if idx < len(args) and args[idx].upper() == b"ANY":
                idx += 1

        elif token == b"WITHCOORD":
            withcoord = True
            idx += 1

        elif token == b"WITHDIST":
            withdist = True
            idx += 1

        elif token == b"WITHHASH":
            withhash = True
            idx += 1

        else:
            raise CommandError("ERR syntax error")

    if center is None or shape is None:
        raise CommandError("ERR syntax error")

    try:
        matches = context.storage.geosearch(key, center, shape, order, count)
    except WrongTypeError:
        raise CommandError(WRONGTYPE_MESSAGE)

    result = []
    for match in matches:
        if not (withcoord or withdist or withhash):
            result.append(match["member"])
            continue

        item = [match["member"]]
        if withdist:
            item.append(_format_distance(meters_to_distance(match["dist"], output_unit)))
        if withhash:
            item.append(str(int(match["score"])).encode())
        if withcoord:
            item.append([_format_coord(match["lon"]), _format_coord(match["lat"])])
        result.append(item)

    return CommandResult.resp(result, propagate=False)
