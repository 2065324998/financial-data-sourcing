#!/usr/bin/env python3
"""
Mock gRPC server implementing SecurityService (:8082) and PriceService (:8083).

Stores all data in memory. Pre-loads fixture data so scripts can run end-to-end
without a real ledger. Supports the RPCs actually used by the ETL scripts:
  - SecurityService: CreateOrUpdate, Search, ValidateCreateOrUpdate
  - PriceService:    CreateOrUpdate, Search, GetByIds

Usage:
  python3 mock_server.py                  # Start both services
  python3 mock_server.py --no-fixtures    # Start empty (no pre-loaded data)
  python3 mock_server.py --verbose        # Log every RPC call

Then in another terminal:
  API_URL=localhost python3 bond/fedinvest.py --mock
  API_URL=localhost python3 equity/equities.py --mock
  API_URL=localhost python3 equity/yahoo.py --mock --tickers AAPL MSFT
"""

import argparse
import json
import os
import sys
import time
import uuid
from concurrent import futures
from datetime import date, datetime

# Add project root and ledger-models to path (same as other scripts in this repo)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_LEDGER_PATHS = [
    os.path.expanduser("~/projects/ledger-models/ledger-models-python"),
    os.path.expanduser("~/github/bot-farm/worktrees/lm-orchestrator/ledger-models-python"),
]
for _p in _LEDGER_PATHS:
    if os.path.isdir(_p):
        sys.path.insert(0, _p)
        break

import grpc

from fintekkers.models.price.price_pb2 import PriceProto
from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
from fintekkers.models.security.identifier.identifier_type_pb2 import CASH, EXCH_TICKER, IdentifierTypeProto
from fintekkers.models.security.security_pb2 import SecurityProto
from fintekkers.models.security.security_type_pb2 import CASH_SECURITY
from fintekkers.models.security.security_quantity_type_pb2 import UNITS
from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto
from fintekkers.models.util.uuid_pb2 import UUIDProto
from fintekkers.requests.price.create_price_request_pb2 import CreatePriceRequestProto
from fintekkers.requests.price.create_price_response_pb2 import CreatePriceResponseProto
from fintekkers.requests.price.query_price_request_pb2 import QueryPriceRequestProto
from fintekkers.requests.price.query_price_response_pb2 import QueryPriceResponseProto
from fintekkers.requests.security.create_security_request_pb2 import CreateSecurityRequestProto
from fintekkers.requests.security.create_security_response_pb2 import CreateSecurityResponseProto
from fintekkers.requests.security.query_security_request_pb2 import QuerySecurityRequestProto
from fintekkers.requests.security.query_security_response_pb2 import QuerySecurityResponseProto
from fintekkers.requests.util.errors.summary_pb2 import SummaryProto
from fintekkers.services.price_service.price_service_pb2_grpc import (
    PriceServicer,
    add_PriceServicer_to_server,
)
from fintekkers.services.security_service.security_service_pb2_grpc import (
    SecurityServicer,
    add_SecurityServicer_to_server,
)
from google.protobuf.timestamp_pb2 import Timestamp

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")

# Deterministic UUID for the seed USD cash security
_USD_CASH_UUID = uuid.uuid5(uuid.UUID("00000000-0000-0000-0000-000000000000"), "cash-USD")


# ---------------------------------------------------------------------------
# In-memory security store
# ---------------------------------------------------------------------------

class SecurityStore:
    def __init__(self):
        self._by_uuid: dict[bytes, SecurityProto] = {}
        self._by_identifier: dict[tuple[int, str], list[bytes]] = {}

    def upsert(self, proto: SecurityProto) -> SecurityProto:
        raw_uuid = bytes(proto.uuid.raw_uuid)
        self._by_uuid[raw_uuid] = proto

        if proto.HasField("identifier"):
            key = (proto.identifier.identifier_type, proto.identifier.identifier_value)
            if key not in self._by_identifier:
                self._by_identifier[key] = []
            if raw_uuid not in self._by_identifier[key]:
                self._by_identifier[key].append(raw_uuid)

        return proto

    def search(self, request: QuerySecurityRequestProto) -> list[SecurityProto]:
        if request.uuIds:
            results = []
            for uid in request.uuIds:
                proto = self._by_uuid.get(bytes(uid.raw_uuid))
                if proto:
                    results.append(proto)
            return results

        if request.HasField("search_security_input"):
            filters = request.search_security_input.filters
            for f in filters:
                from fintekkers.models.position.field_pb2 import FieldProto
                if f.field == FieldProto.IDENTIFIER:
                    id_proto = IdentifierProto()
                    f.field_value_packed.Unpack(id_proto)
                    key = (id_proto.identifier_type, id_proto.identifier_value)
                    uuids = self._by_identifier.get(key, [])
                    return [self._by_uuid[u] for u in uuids if u in self._by_uuid]

                if f.field == FieldProto.ID:
                    uid = UUIDProto()
                    f.field_value_packed.Unpack(uid)
                    proto = self._by_uuid.get(bytes(uid.raw_uuid))
                    return [proto] if proto else []

        return list(self._by_uuid.values())

    @property
    def count(self):
        return len(self._by_uuid)


# ---------------------------------------------------------------------------
# In-memory price store
# ---------------------------------------------------------------------------

class PriceStore:
    def __init__(self):
        self._by_uuid: dict[bytes, PriceProto] = {}
        self._by_security: dict[bytes, list[bytes]] = {}

    def upsert(self, proto: PriceProto) -> PriceProto:
        raw_uuid = bytes(proto.uuid.raw_uuid)
        self._by_uuid[raw_uuid] = proto

        if proto.HasField("security"):
            sec_uuid = bytes(proto.security.uuid.raw_uuid)
            if sec_uuid not in self._by_security:
                self._by_security[sec_uuid] = []
            if raw_uuid not in self._by_security[sec_uuid]:
                self._by_security[sec_uuid].append(raw_uuid)

        return proto

    def get_by_ids(self, uuids: list[UUIDProto]) -> list[PriceProto]:
        results = []
        for uid in uuids:
            proto = self._by_uuid.get(bytes(uid.raw_uuid))
            if proto:
                results.append(proto)
        return results

    def search(self, request: QueryPriceRequestProto) -> list[PriceProto]:
        if request.uuIds:
            return self.get_by_ids(request.uuIds)
        return list(self._by_uuid.values())

    @property
    def count(self):
        return len(self._by_uuid)


# ---------------------------------------------------------------------------
# Mock SecurityServicer
# ---------------------------------------------------------------------------

class MockSecurityServicer(SecurityServicer):
    def __init__(self, store: SecurityStore, verbose: bool = False):
        self._store = store
        self._verbose = verbose

    def CreateOrUpdate(self, request: CreateSecurityRequestProto, context):
        proto = request.security_input
        result = self._store.upsert(proto)
        if self._verbose:
            _id = proto.identifier.identifier_value if proto.HasField("identifier") else "?"
            print(f"  [SecurityService] CreateOrUpdate: {_id}")
        return CreateSecurityResponseProto(
            object_class="CreateSecurityResponseProto",
            version="0.0.1",
            security_response=result,
        )

    def Search(self, request: QuerySecurityRequestProto, context):
        results = self._store.search(request)
        if self._verbose:
            print(f"  [SecurityService] Search: {len(results)} results")
        yield QuerySecurityResponseProto(
            object_class="QuerySecurityResponseProto",
            version="0.0.1",
            security_response=results,
        )

    def ValidateCreateOrUpdate(self, request: CreateSecurityRequestProto, context):
        if self._verbose:
            print("  [SecurityService] ValidateCreateOrUpdate")
        return SummaryProto()

    def GetByIds(self, request: QuerySecurityRequestProto, context):
        results = self._store.search(request)
        if self._verbose:
            print(f"  [SecurityService] GetByIds: {len(results)} results")
        return QuerySecurityResponseProto(
            object_class="QuerySecurityResponseProto",
            version="0.0.1",
            security_response=results,
        )


# ---------------------------------------------------------------------------
# Mock PriceServicer
# ---------------------------------------------------------------------------

class MockPriceServicer(PriceServicer):
    def __init__(self, store: PriceStore, verbose: bool = False):
        self._store = store
        self._verbose = verbose

    def CreateOrUpdate(self, request: CreatePriceRequestProto, context):
        proto = request.create_price_input
        result = self._store.upsert(proto)
        if self._verbose:
            print(f"  [PriceService] CreateOrUpdate: uuid={bytes(proto.uuid.raw_uuid).hex()[:8]}...")
        return CreatePriceResponseProto(
            object_class="CreatePriceResponseProto",
            version="0.0.1",
            price_response=[result],
        )

    def Search(self, request: QueryPriceRequestProto, context):
        results = self._store.search(request)
        if self._verbose:
            print(f"  [PriceService] Search: {len(results)} results")
        yield QueryPriceResponseProto(
            object_class="QueryPriceResponseProto",
            version="0.0.1",
            price_response=results,
        )

    def GetByIds(self, request: QueryPriceRequestProto, context):
        results = self._store.get_by_ids(request.uuIds)
        if self._verbose:
            print(f"  [PriceService] GetByIds: {len(results)} results")
        return QueryPriceResponseProto(
            object_class="QueryPriceResponseProto",
            version="0.0.1",
            price_response=results,
        )

    def ValidateCreateOrUpdate(self, request: CreatePriceRequestProto, context):
        if self._verbose:
            print("  [PriceService] ValidateCreateOrUpdate")
        return SummaryProto()


# ---------------------------------------------------------------------------
# Fixture loading — seed the stores with a USD cash security
# ---------------------------------------------------------------------------

def _make_as_of():
    return LocalTimestampProto(
        time_zone="UTC",
        timestamp=Timestamp(seconds=int(time.time()), nanos=0),
    )


def seed_usd_cash(security_store: SecurityStore):
    proto = SecurityProto(
        object_class="Security",
        version="0.0.1",
        uuid=UUIDProto(raw_uuid=_USD_CASH_UUID.bytes),
        as_of=_make_as_of(),
        security_type=CASH_SECURITY,
        asset_class="Cash",
        issuer_name="US Dollar",
        description="USD Cash",
        quantity_type=UNITS,
        identifier=IdentifierProto(identifier_type=CASH, identifier_value="USD"),
    )
    security_store.upsert(proto)
    print(f"  Seeded USD cash security (uuid={str(_USD_CASH_UUID)[:8]}...)")


def load_fixtures(security_store: SecurityStore, price_store: PriceStore):
    """Pre-load fixture data into the stores."""
    seed_usd_cash(security_store)
    print(f"  Securities: {security_store.count}  |  Prices: {price_store.count}")


# ---------------------------------------------------------------------------
# Server startup
# ---------------------------------------------------------------------------

def serve(security_port: int = 8082, price_port: int = 8083,
          load_fixtures_flag: bool = True, verbose: bool = False):
    security_store = SecurityStore()
    price_store = PriceStore()

    if load_fixtures_flag:
        print("Loading fixtures...")
        load_fixtures(security_store, price_store)

    security_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_SecurityServicer_to_server(
        MockSecurityServicer(security_store, verbose=verbose), security_server
    )
    security_server.add_insecure_port(f"[::]:{security_port}")

    price_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_PriceServicer_to_server(
        MockPriceServicer(price_store, verbose=verbose), price_server
    )
    price_server.add_insecure_port(f"[::]:{price_port}")

    security_server.start()
    price_server.start()

    print(f"\nMock gRPC servers running:")
    print(f"  SecurityService  localhost:{security_port}")
    print(f"  PriceService     localhost:{price_port}")
    print(f"\nPress Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("\nShutting down...")
        security_server.stop(0)
        price_server.stop(0)


def main():
    parser = argparse.ArgumentParser(description="Mock gRPC SecurityService + PriceService")
    parser.add_argument("--security-port", type=int, default=8082)
    parser.add_argument("--price-port", type=int, default=8083)
    parser.add_argument("--no-fixtures", action="store_true",
                        help="Start with empty stores (no USD cash seed)")
    parser.add_argument("--verbose", action="store_true",
                        help="Log every RPC call")
    args = parser.parse_args()

    serve(
        security_port=args.security_port,
        price_port=args.price_port,
        load_fixtures_flag=not args.no_fixtures,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
