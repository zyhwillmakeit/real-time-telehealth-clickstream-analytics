"""Pure Step 8 decoding and routing; Registry outages are not bad records."""

import json
import struct
from datetime import datetime
from io import BytesIO

import requests
from fastavro import schemaless_reader

from scripts.validate_contracts import validate_event

RULE_VERSION = "1.0.0"


class RegistryUnavailable(RuntimeError):
    pass


class RegistryResolver:
    def __init__(self, url, api_key, api_secret):
        if not url.startswith("https://") or not api_key or not api_secret:
            raise ValueError("HTTPS Registry URL and credentials required")
        self.url = url.rstrip("/")
        self.session = requests.Session()
        self.session.auth = (api_key, api_secret)
        self.cache = {}

    def resolve(self, schema_id):
        if schema_id in self.cache:
            return self.cache[schema_id]
        try:
            response = self.session.get(
                f"{self.url}/schemas/ids/{schema_id}", timeout=15, allow_redirects=False
            )
            if (
                response.status_code == 404
                and response.json().get("error_code") == 40403
            ):
                return None  # Do not cache a missing ID across future batches.
            if response.status_code != 200:
                raise RegistryUnavailable(f"Registry HTTP {response.status_code}")
            body = response.json()
            schema = json.loads(body["schema"])
            if body.get("schemaType", "AVRO") != "AVRO":
                schema = {"unsupported_schema_type": body.get("schemaType")}
        except (requests.RequestException, ValueError, KeyError) as exc:
            raise RegistryUnavailable(
                "Registry lookup failed; batch must retry"
            ) from exc
        self.cache[schema_id] = schema
        return schema


def header_id(value):
    if value is None or len(value) < 5 or value[0] != 0:
        return None
    return struct.unpack(">I", bytes(value[1:5]))[0]


def classify(value, schemas, contract, rules):
    schema_id = header_id(value)
    event = None
    errors = []
    if value is None:
        errors = ["KAFKA_TOMBSTONE"]
    elif len(value) < 5:
        errors = ["FRAME_TRUNCATED"]
    elif value[0] != 0:
        errors = ["MAGIC_BYTE_INVALID"]
    elif schema_id not in schemas:
        raise RegistryUnavailable("Schema was not prefetched")
    elif schemas[schema_id] is None:
        errors = ["SCHEMA_ID_UNKNOWN"]
    elif schemas[schema_id] != contract:
        errors = ["WRITER_SCHEMA_UNSUPPORTED"]
    else:
        stream = BytesIO(bytes(value[5:]))
        try:
            event = schemaless_reader(stream, schemas[schema_id])
            if stream.read(1):
                errors = ["AVRO_TRAILING_BYTES"]
            else:
                errors = validate_event(event, contract, rules)
        except (ValueError, EOFError, IndexError, TypeError, OverflowError):
            errors = ["AVRO_DECODE_FAILED"]

    def encode(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError("Unsupported decoded value")

    return {
        "schema_id": schema_id,
        "rule_version": RULE_VERSION,
        "route": "quarantine" if errors else "valid",
        "error_codes": errors,
        "event_json": json.dumps(event, default=encode, sort_keys=True)
        if event is not None
        else None,
    }
