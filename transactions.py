import json
import os
from recon_result import ReconResult, ReconResultStatus
from datetime import datetime
from securities import *

from env import CHANNEL

from fintekkers.services.transaction_service.transaction_service_pb2_grpc import (
    TransactionStub,
)

transaction_service: TransactionStub = TransactionStub(CHANNEL)

from fintekkers.requests.transaction.query_transaction_request_pb2 import (
    QueryTransactionRequestProto,
)
from fintekkers.requests.transaction.query_transaction_response_pb2 import (
    QueryTransactionResponseProto,
)

from fintekkers.requests.transaction.create_transaction_request_pb2 import (
    CreateTransactionRequestProto,
)
from fintekkers.requests.transaction.create_transaction_response_pb2 import (
    CreateTransactionResponseProto,
)

from fintekkers.models.portfolio.portfolio_pb2 import PortfolioProto
from fintekkers.models.security.security_pb2 import SecurityProto
from fintekkers.models.security.security_type_pb2 import SecurityTypeProto

from fintekkers.models.util.local_date_pb2 import LocalDateProto
from fintekkers.models.position.field_pb2 import FieldProto
from fintekkers.models.position.measure_pb2 import DIRECTED_QUANTITY
from fintekkers.models.position.position_pb2 import PositionTypeProto
from fintekkers.models.position.position_util_pb2 import (
    EQUALS,
    FieldMapEntry,
    MeasureMapEntry,
)

from fintekkers.models.transaction.transaction_pb2 import TransactionProto
from fintekkers.models.transaction.transaction_type_pb2 import TransactionTypeProto
from fintekkers.models.security.security_pb2 import SecurityProto
from fintekkers.models.portfolio.portfolio_pb2 import PortfolioProto

from fintekkers.wrappers.models.position import Position
from fintekkers.wrappers.models.util.date_utils import get_date
from fintekkers.wrappers.requests.transaction import (
    CreateTransactionRequest,
    QueryTransactionRequest,
)
from fintekkers.wrappers.models.util.date_utils import (
    get_date_proto,
    get_date,
    get_date_from_proto,
)

from securities import get_security_by_id
from portfolios import PORTFOLIO
from positions import get_position

portfolio: PortfolioProto = PORTFOLIO.proto


def is_supported(data: dict):
    return True


def search_transaction(
    portfolio: PortfolioProto, security: Security, trade_date: LocalDateProto = None
) -> list[TransactionProto]:
    query_fields: dict = {
        FieldProto.PORTFOLIO: portfolio,
        FieldProto.SECURITY: security.proto,
    }

    if trade_date is not None:
        query_fields[FieldProto.TRADE_DATE] = get_date_proto(trade_date)

    request: QueryTransactionRequestProto = (
        QueryTransactionRequest.create_query_request(query_fields)
    )

    responses = transaction_service.Search(request)

    transactions = []

    try:
        while not responses._is_complete():
            response: QueryTransactionResponseProto = responses.next()

            for transaction in response.transaction_response:
                transactions.append(transaction)
    except StopIteration:
        pass
    except Exception as e:
        print(e)

    responses.cancel()
    return transactions


def did_SOMA_buy(data: RawAuctionData, security: Security):
    if security is None:
        print("Security missing: " + data.cusip)
        return False

    if data.get_price_paid_by_soma() is None:
        return False

    quantity = data.get_quantity_bought_by_soma()

    if quantity is None or quantity == 0:
        return False

    return True


def upsizeQuantity_experiementalFeature(data: RawAuctionData):
    issue_date = data.get_issue_date()
    dated_date = data.get_dated_date()

    year = issue_date.year if issue_date is not None else None
    if year is None:
        year = dated_date.year if dated_date is not None else None

    quantity = data.get_quantity_bought_by_soma()
    if quantity % 10 != 0:
        quantity = quantity * 1000
        print(
            f"Upsizing quantity, probably an old auction ({year}), "
        )  # + data.to_json())
    return quantity


def upload_transaction(data: RawAuctionData, portfolio: PortfolioProto):
    security = get_security_by_id(
        identifier=data.cusip, identifier_type=IdentifierTypeProto.CUSIP
    )

    if len(security) > 1:
        return ReconResult(
            ReconResultStatus.RECON_UNKNOWN,
            data.cusip,
            "Multiple securities for this id. Logic required here. Halting creation.",
            transaction_created=False,
        )

    if len(security) == 0 and data.get_issue_date() > date.today():
        return ReconResult(
            ReconResultStatus.RECON_UNKNOWN,
            data.cusip,
            "Future issued security, may not have auction date",
            transaction_created=False,
        )

    security = security[0]

    if security.get_security_type() != SecurityTypeProto.BOND_SECURITY:
        return ReconResult(
            ReconResultStatus.RECON_GOOD,
            data.cusip,
            f"Security Type not supported: {security.get_security_type()}.",
            transaction_created=False,
        )

    # Step 1: Load auction results, and check that SOMA did buy
    did_SOMA_buy_in_auction = did_SOMA_buy(data, security)
    if not did_SOMA_buy_in_auction:
        return ReconResult(
            ReconResultStatus.RECON_GOOD,
            data.cusip,
            f"SOMA did not buy this security.",
            transaction_created=False,
        )

    price = data.get_price_paid_by_soma()

    # Step 2: Get existing transaction, to check if it was already uploaded

    # Step 3: Override the template with items from the auction results
    # A) We're booking a BUY for the amount the SOMA Accepted amount, and the purchase price as the high price
    # i) Get the portfolio by name
    # ii) Get the security by the ID
    # iii) Chcek if it already exists

    quantity = upsizeQuantity_experiementalFeature(data)

    issue_date = get_date_proto(data.get_issue_date())

    return create_transaction(
        portfolio=portfolio,
        security=security,
        quantity=quantity,
        trade_date=issue_date,
        price=price,
    )


def create_transaction(
    portfolio: PortfolioProto,
    security: Security,
    quantity: float,
    price: float,
    trade_date: LocalDateProto,
) -> ReconResult:
    response: list[TransactionProto] = search_transaction(
        portfolio, security, trade_date
    )

    def filter_txns(transaction: TransactionProto):
        return float(transaction.quantity.arbitrary_precision_value) == quantity

    response = list(filter(filter_txns, response))
    # TODO: Filter if the quanity is different, meaning this is not a duplicate....

    if len(response) == 0:
        return _create_transaction(portfolio, security, quantity, price, trade_date)
    if len(response) > 0:
        matching_txns = search_transaction(
            portfolio=portfolio, security=security, trade_date=trade_date
        )

        if len(matching_txns) == 0:
            print("Interesting")

        return ReconResult(
            ReconResultStatus.RECON_GOOD,
            security.get_security_id().get_identifier_value(),
            "Transaction already exists",
            False,
        )


def _create_transaction(
    portfolio: PortfolioProto,
    security: Security,
    quantity: float,
    price: float,
    trade_date: LocalDateProto,
) -> ReconResult:
    quantity = float(quantity)
    direction: TransactionTypeProto = TransactionTypeProto.BUY

    if quantity < 0:
        direction = TransactionTypeProto.SELL
        quantity = abs(quantity)

    if quantity < 0:
        print("Received a trade with a negative quantity")

    as_of: date

    trade_date_dt: datetime = get_date_from_proto(trade_date)
    if trade_date_dt > datetime.now():
        as_of = datetime.now()
    else:
        as_of = trade_date_dt

    request: CreateTransactionRequestProto = (
        CreateTransactionRequest.create_transaction_request(
            security=security.proto,
            portfolio=portfolio,
            transaction_type=direction,
            price=price,
            trade_date=trade_date,
            settlement_date=trade_date,
            quantity=quantity,
            as_of=as_of,
        ).proto
    )

    response: CreateTransactionResponseProto = transaction_service.CreateOrUpdate(
        request=request
    )

    if response.transaction_response is not None:
        if len(response.transaction_response.childTransactions) == 0:
            print(
                f"Children transactions were not created by the ledger for {response.transaction_response.uuid}: {response.transaction_response.childTransactions}"
            )
        return ReconResult(
            ReconResultStatus.RECON_GOOD,
            security.get_security_id().get_identifier_value(),
            f"Transaction was created {response.transaction_response.uuid}",
        )
    else:
        return ReconResult(
            ReconResultStatus.RECON_UNKNOWN,
            security.identifier,
            f"Problem with transaction: {request}",
        )


def process_file(file: str) -> ReconResult:
    json_str = open("data/raw_json/" + file).read()
    data = RawAuctionData.from_json(json_str)

    if data.cusip != "912797GK7":
        return None

    result: ReconResult = upload_transaction(data, portfolio=portfolio)

    security: list[SecurityProto] = get_security_by_id(
        identifier=data.cusip, identifier_type=IdentifierTypeProto.CUSIP
    )

    if len(security) == 0 and data.get_issue_date() > date.today():
        # Security hasn't been issued yet
        return ReconResult(
            ReconResultStatus.RECON_UNKNOWN,
            IdentifierProto(
                identifier_type=IdentifierTypeProto.CUSIP, identifier_value=data.cusip
            ),
            f"Future dated security: {data.cusip}. {data.issue_date}",
            transaction_created=False,
        )

    security:SecurityProto = security[0]

    issue_date = get_date_proto(data.get_issue_date())

    map_entry: FieldMapEntry = FieldMapEntry(
        field=FieldProto.TRADE_DATE,
        field_value_packed=Position.pack_field(issue_date),
        operator=EQUALS,
    )

    positions: list[Position] = get_position(
        security=security,
        portfolio=portfolio,
        measures=[DIRECTED_QUANTITY],
        position_type=PositionTypeProto.TRANSACTION,
        fields=[FieldProto.PORTFOLIO, FieldProto.SECURITY, FieldProto.TRADE_DATE],
        additional_filters=[map_entry],
    )

    transaction_response: list[TransactionProto] = search_transaction(
        portfolio=portfolio, security=security, trade_date=issue_date
    )

    def check_position_dupes(txns: list[Position]):
        dupe_map: dict[float, Position] = {}
        measure: MeasureMapEntry = MeasureMapEntry(measure=DIRECTED_QUANTITY)

        for position in txns:
            position: Position

            if position.get_measure(measure) not in dupe_map:
                dupe_map[position.get_measure(measure)] = position
            else:
                print("Found a dupe")
                return position

        return None

    # TODO: If transaction response or position response has multiple entries, check whether there are duplicate entries.
    def check_dupes(txns: list[TransactionProto]):
        dupe_map: dict[float, TransactionProto] = {}

        for txn in txns:
            txn: TransactionProto

            if txn.quantity.arbitrary_precision_value not in dupe_map:
                dupe_map[txn.quantity.arbitrary_precision_value] = txn
            else:
                print("Found a dupe")
                return txn

        return None

    dupe_result = check_position_dupes(positions)

    dupe_result = check_dupes(transaction_response)

    if dupe_result is not None:
        print("Stop")

    if result.result == ReconResultStatus.RECON_GOOD:
        # Can't remember why we had this
        # if "did not buy" not in result.message:
        #     if len(transaction_response) == 0:
        #         print("Should always have found a transaction with this trade date")
        #     if len(positions) == 0:
        #         print("Should always have found a position with this trade date")

        return result
    else:
        additional_message: str = result.message

        if len(transaction_response) == 0:
            additional_message += " No transaction. "
        if positions is None or len(positions) == 0:
            additional_message += "No position. "

        additional_message += json.dumps(data)

        return ReconResult(result.result, result.security_id, additional_message)


if __name__ == "__main__":
    print(os.getcwd())
    import timeit

    start_time = timeit.default_timer()

    files = os.listdir("data/raw_json")

    import random

    random.shuffle(files)

    for file in files:
        try:
            result: ReconResult = process_file(file)

            if result is None:
                print("")

            if ReconResultStatus.RECON_GOOD != result.result:
                print(
                    f"""{result.result.name}: ID[{result.security_id.identifier_value}]: {result.message}"""
                )
        except Exception as e:
            print(f"Error processing file {file}: {e}")

    # p = mp.Pool(processes=5)
    # p.map(process_file, files)

    elapsed = timeit.default_timer() - start_time

    print(f"Time taken: {elapsed}")
