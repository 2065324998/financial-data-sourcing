from datetime import date
from typing import Generator

import os as _os, sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))
from auction_data import RawAuctionData

import os
from uuid import UUID

import grpc

from fintekkers.requests.security.create_security_request_pb2 import (
    CreateSecurityRequestProto,
)
from fintekkers.requests.security.create_security_response_pb2 import (
    CreateSecurityResponseProto,
)
from fintekkers.models.position.field_pb2 import FieldProto

from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
from fintekkers.models.security.identifier.identifier_type_pb2 import (
    IdentifierTypeProto,
)
from fintekkers.models.security.bond.issuance_pb2 import IssuanceProto
from fintekkers.models.security.security_pb2 import SecurityProto

from fintekkers.wrappers.models.security.security import Security
from fintekkers.wrappers.models.issues.issuance import Issuance

from fintekkers.wrappers.requests.security import (
    CreateSecurityRequest,
    QuerySecurityRequest,
)
from fintekkers.wrappers.services.security import SecurityService
from fintekkers.wrappers.models.util.date_utils import (
    get_date_proto,
)

from google.protobuf.wrappers_pb2 import StringValue
from fintekkers.wrappers.models.util.serialization import ProtoSerializationUtil


def create_security(data: RawAuctionData) -> SecurityProto:
    # Check if a security with this CUSIP, issue date, and maturity date already exists
    existing = get_security_by_id(data.cusip, IdentifierTypeProto.CUSIP)
    for security in existing:
        if (security.get_maturity_date() == data.get_maturity_date()
                and security.get_issue_date() == data.get_issue_date()):
            print(f"Security {data.cusip} already exists, skipping creation")
            return security.proto

    cash_security = get_security_by_id("USD", IdentifierTypeProto.CASH)[0]

    request: CreateSecurityRequest = CreateSecurityRequest.create_ust_security_request(
        cusip=data.cusip,
        cash_security=cash_security.proto,
        issue_date=data.get_issue_date(),
        dated_date=data.get_dated_date(),
        maturity_date=data.get_maturity_date(),
        security_type=data.get_security_type_proto(),
        coupon_rate=data.get_coupon_rate_float(),
        face_value=1000.0,
    )

    add_auction_data(request.proto.security_input, data)

    service = SecurityService()
    try:
        response: CreateSecurityResponseProto = service.create_or_update(request)
    except Exception as e:
        if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
            validation = service.validate_create_or_update(request)
            print(f"  Validation errors for {data.cusip}: {validation}")
        raise

    if data.accrued_interest is not None and data.accrued_interest != "":
        print(f"Accrued interest: {data.accrued_interest}")

    return response.security_response


def update_issue_date_on_security(existing_security: Security, issue_date: date):
    issue_date_proto = get_date_proto(issue_date)
    existing_security.proto.issue_date.CopyFrom(issue_date_proto)
    # Need to think through the as of date, as the original will have a newer asof date. Ideally we delete the original I think, or use the system timestamp concept
    request = CreateSecurityRequest(
        CreateSecurityRequestProto(security_input=existing_security.proto)
    )
    response: CreateSecurityResponseProto = SecurityService().create_or_update(request)


def upload_security(file: str):
    json_str = open("data/raw_json/" + file).read()
    data = RawAuctionData.from_json(json_str)

    # if data.cusip != "912797GK7":
    #     return

    upload_security_from_data_dict(data)


def add_auction_data(security: SecurityProto, data: RawAuctionData):
    issuance_proto = IssuanceProto(
        as_of=security.as_of,
        version="1.0.0",
    )

    #### ADDING ISSUANCE DATA
    if (
            data.get_post_auction_amount_outstanding() is None
            and data.get_offering_amount() is None
            and data.get_maturity_date() > date.today()
    ):
        # Ignore them if they already matured. Likely due to it being a pre-2008 treasury
        # For Pre 2010 bonds they'll have to be 30 year issues, so may have to patch
        # that data.
        return

    announcement_date = data.get_announcement_date()
    issuance_proto.auction_announcement_date.CopyFrom(
        ProtoSerializationUtil.serialize(announcement_date)
    )

    for auction in security.issuance_info:
        auction: IssuanceProto

        if get_date_proto(announcement_date) == auction.auction_announcement_date:
            return  ## Already added

    if data.get_offering_amount() is not None and data.get_offering_amount() > 0:
        issuance_proto.auction_offering_amount.CopyFrom(
            ProtoSerializationUtil.serialize(data.get_offering_amount())
        )

    if data.get_total_accepted() is not None and data.get_total_accepted() > 0:
        issuance_proto.total_accepted.CopyFrom(
            ProtoSerializationUtil.serialize(data.get_total_accepted())
        )

    if (
            data.get_post_auction_amount_outstanding() is not None
            and data.get_post_auction_amount_outstanding() > 0
    ):
        issuance_proto.post_auction_outstanding_quantity.CopyFrom(
            ProtoSerializationUtil.serialize(data.get_post_auction_amount_outstanding())
        )
    elif (
            data.get_maturity_date().year < 2010
            and data.get_post_auction_amount_outstanding() == 0
    ):
        # Old data from the Fed is buggy it seems. Setting the post auction amount to the total
        # accepted amount. This might be wrong but this is very old data so not worrying about it
        # unless someone raises an issue
        issuance_proto.post_auction_outstanding_quantity.CopyFrom(
            ProtoSerializationUtil.serialize(data.get_total_accepted())
        )
    else:
        print("Missing post auction quantity outstanding amount for " + data.cusip)

    if (
            data.get_maturity_security_amount() is not None
    ):  # Sometimes seen negative numbers # and data.get_maturity_security_amount() > 0:
        issuance_proto.mature_security_amount.CopyFrom(
            ProtoSerializationUtil.serialize(data.get_maturity_security_amount())
        )

    security.issuance_info.append(issuance_proto)


def get_security(identifier, identifier_type, issue_date):
    securities = get_security_by_id(
        identifier=identifier, identifier_type=identifier_type
    )

    for security in securities:
        if (
                issue_date.date() is not None
                and security.get_issue_date() is not None
                and security.get_issue_date() == issue_date.date()
        ):
            return security


def upload_security_from_data_dict(data: RawAuctionData):
    # maturity_date = get_date_proto(data.get_maturity_date())

    # Get securities with the same identifier (ignoring issue/maturity date)
    securities = get_security_by_id(
        identifier=data.cusip, identifier_type=IdentifierTypeProto.CUSIP
    )

    existing_security = None

    # If security exists with same issue/maturity date then it exists and we can return
    for security in securities:
        if (
                security.get_maturity_date() == data.get_maturity_date()
                and security.get_issue_date() == data.get_issue_date()
        ):
            existing_security = security

        if (
                security.get_maturity_date() == data.get_maturity_date()
                and security.get_issue_date() < data.get_issue_date()
        ):
            print("Security exists and the issue date is earlier so not updating")
            existing_security = security

    # If security exists with same maturity date but later issue date,
    # then this security is an earlier issue and we need to update the
    # issue date.
    for security in securities:
        if (
                security.get_maturity_date() == data.get_maturity_date()
                and security.get_issue_date() > data.get_issue_date()
        ):
            print(f"Updating the issue date")
            update_issue_date_on_security(security, data.get_issue_date())
            existing_security = security

    # No security exists so we'll create it
    if existing_security is None:
        print("Creating security: " + data.cusip)
        response = create_security(data)
        existing_security = response

        if response is None:
            print("Couldn't process data for :" + data.to_json())
        else:
            print(f"Created security {data.cusip}")

        return response
    else:
        # Rebuild from raw data so coupon_type, coupon_frequency etc. are always correct,
        # then stamp the existing UUID onto the rebuilt proto so this is an update not a create.
        cash_security = get_security_by_id("USD", IdentifierTypeProto.CASH)[0]
        request: CreateSecurityRequest = CreateSecurityRequest.create_ust_security_request(
            cusip=data.cusip,
            cash_security=cash_security.proto,
            issue_date=data.get_issue_date(),
            dated_date=data.get_dated_date(),
            maturity_date=data.get_maturity_date(),
            security_type=data.get_security_type_proto(),
            coupon_rate=data.get_coupon_rate_float(),
            face_value=1000.0,
        )
        request.proto.security_input.uuid.CopyFrom(existing_security.proto.uuid)
        add_auction_data(request.proto.security_input, data)
        service = SecurityService()
        try:
            service.create_or_update(request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                validation = service.validate_create_or_update(request)
                print(f"  Validation errors for {data.cusip}: {validation}")
            raise
        return None


def get_security_by_id(
        identifier: str, identifier_type: IdentifierTypeProto
) -> list[Security]:
    """
    Parameters:
        An Identifier pair

    Returns:
        request (list[SecurityProto]): Returns all securities that match the identifier. May return
        multiple securities, e.g. where a CUSIP has been reused
    """
    id_proto = IdentifierProto(
        identifier_value=identifier, identifier_type=identifier_type
    )

    request: QuerySecurityRequest = QuerySecurityRequest.create_query_request(
        {
            FieldProto.IDENTIFIER: id_proto,
        }
    )
    securities = []
    for security in SecurityService().search(request):
        securities.append(security)

    return securities


def get_security_ids() -> Generator[UUID, None, None]:
    request: QuerySecurityRequest = QuerySecurityRequest.create_query_request(
        {
            FieldProto.ASSET_CLASS: "Fixed Income",
        }
    )

    securities: list[SecurityProto] = SecurityService().search(request)

    for security in securities:
        yield UUID(bytes=security.proto.uuid.raw_uuid)


def get_field_values(field: FieldProto) -> list[object]:
    ids = get_security_ids()
    values = []
    for id in ids:
        _security: Security = SecurityService.get_security_by_uuid(id)
        values.append(_security.get_field(field))

    return values


def _get_security_types_from_auction_data() -> dict:
    """Build a CUSIP → security_type lookup from the primary auction JSON data.

    The raw JSON files (from TreasuryDirect auction XML) have a reliable
    'security_type' field ('BOND', 'TIPS', 'FRN', 'NOTE', 'BILL').
    We use this as the source of truth for TIPS/FRN detection when
    importing from secondary sources (ODM XML) that lack a SecurityType field.
    """
    import json as _json

    # Pre-2008 TIPS not present in TreasuryDirect auction XML data.
    # Confirmed via treasurydirect.gov/instit/annceresult/tipscpi/ pages.
    types = {
        "912810FD5": "TIPS",  # 3-5/8% 30Y TIPS due 2028-04-15
        "912810FH6": "TIPS",  # 3-7/8% 30Y TIPS due 2029-04-15
        "912810FQ6": "TIPS",  # 3-3/8% 30Y TIPS due 2032-04-15
        "912810US5": "TIPS",  # 2-3/8% 30Y TIPS due 2042-01-15
    }

    json_dir = "data/raw_json"
    if not os.path.isdir(json_dir):
        return types
    for f in os.listdir(json_dir):
        try:
            with open(os.path.join(json_dir, f)) as fh:
                data = _json.load(fh)
                cusip = data.get("cusip")
                sec_type = data.get("security_type")
                if cusip and sec_type:
                    types[cusip] = sec_type
        except Exception:
            continue
    return types


def _create_securities_from_secondary_source():
    from pipeline.convert_xml import get_data
    import json
    import xmltodict
    from auction_data import RawAuctionData

    # Build lookup of CUSIP → security_type from primary auction data
    known_types = _get_security_types_from_auction_data()

    # These files contain a number of securities from the 1990s. We will have issue/maturity date/coupon rate, but not everything
    holdings_files = ['data/raw_xml/odm_ga_11032022.xml', './data/raw_xml/odm_ga_07092024.xml']

    for _file in holdings_files:
        xml = open(_file).read()
        doc = xmltodict.parse(xml)
        doc = doc['bpd:ODMDataFeed']['ODMData']

        for entry in doc:
            tmp_cusip = entry['SecurityIdentifier']

            security_missing = len(get_security_by_id(tmp_cusip, IdentifierTypeProto.CUSIP)) == 0

            if security_missing:
                # Determine security type from primary auction data if available,
                # otherwise default to BOND
                security_type = known_types.get(tmp_cusip, 'BOND')

                result = {
                    'issue_date': entry['IssueDate'],
                    'maturity_date': entry['MaturityDate'],
                    'cusip': tmp_cusip,
                    'investment_rate': entry["InterestRate"],
                    'security_type': security_type,
                    'accrued_interest': [0],
                    'original_issue_date': entry['IssueDate'],
                    'announcement_date': entry['IssueDate'],
                    'dated_date': entry['IssueDate']
                }

                data: RawAuctionData = RawAuctionData.from_json(json.dumps(result))
                response: SecurityProto = upload_security_from_data_dict(data)
                print(response.identifier.identifier_value)


if __name__ == "__main__":
    print(os.getcwd())

    files = os.listdir("data/raw_json")

    # if not check_exists("USD", IdentifierTypeProto.CASH, None):
    #     raise ValueError(
    #         "There is no USD currency in the ledger. This suggests a fundamental problem"
    #     )

    _create_securities_from_secondary_source()

    identifiers = get_field_values(FieldProto.IDENTIFIER)

    len(identifiers)
    import timeit

    start_time = timeit.default_timer()

    for file in files:
        upload_security(file=file)

    elapsed = timeit.default_timer() - start_time
    print(f"Time taken: {elapsed}")

    security: Security = get_security_by_id("912828DN7", IdentifierTypeProto.CUSIP)[0]
    security_proto: SecurityProto = security.proto

    issuance_info = security_proto.issuance_info
    Issuance(issuance_info).sort_by_auction_announcement_date().print_auction_history()
