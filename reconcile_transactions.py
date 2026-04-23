from typing import Tuple
from uuid import UUID
from datetime import date, datetime

from google.protobuf.any_pb2 import Any

from portfolios import PORTFOLIO
from recon_result import ReconResult, ReconResultStatus
from securities import get_security_by_id, get_field_values
from positions import get_position

from transactions import create_transaction
from soma import get_soma_holdings

from fintekkers.models.portfolio.portfolio_pb2 import PortfolioProto
from fintekkers.models.position.field_pb2 import FieldProto

from fintekkers.models.util.local_date_pb2 import LocalDateProto
from fintekkers.models.position.position_util_pb2 import MeasureMapEntry
from fintekkers.models.position.position_util_pb2 import FieldMapEntry
from fintekkers.models.position.position_util_pb2 import LESS_THAN, LESS_THAN_OR_EQUALS
from fintekkers.models.position.measure_pb2 import DIRECTED_QUANTITY
from fintekkers.models.position.position_pb2 import TRANSACTION, PositionProto
from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
from fintekkers.models.security.identifier.identifier_type_pb2 import IdentifierTypeProto
from fintekkers.models.security.security_pb2 import SecurityProto
from fintekkers.models.security.security_type_pb2 import SecurityTypeProto
from fintekkers.models.transaction.transaction_type_pb2 import TransactionTypeProto

from fintekkers.wrappers.models.security import Security
from fintekkers.wrappers.models.position import Position
from fintekkers.wrappers.models.util.serialization import ProtoEnum, ProtoSerializationUtil
from fintekkers.wrappers.models.util.date_utils import get_date
from fintekkers.wrappers.services.security import SecurityService

portfolio: PortfolioProto = PORTFOLIO.proto

tsy_cusip_holdings_url_template = "https://markets.newyorkfed.org/api/soma/tsy/get/cusip/{}.json"


def get_ledger_positions(security: Security,
                         as_of_date: date = datetime.combine(date=date.today(), time=datetime.min.time()),
                         fields=None):
    if fields is None:
        fields = [FieldProto.PORTFOLIO, FieldProto.SECURITY, FieldProto.TRANSACTION_TYPE,
                  FieldProto.TRADE_DATE]
    trade_date_proto = ProtoSerializationUtil.serialize(as_of_date.date())
    trade_date_packed: Any = Any()
    trade_date_packed.Pack(msg=trade_date_proto)

    filters = [
        FieldMapEntry(field=FieldProto.TRADE_DATE,
                      field_value_packed=trade_date_packed,
                      operator=LESS_THAN_OR_EQUALS)
    ]

    trades = get_position(
        security=security,
        portfolio=portfolio,
        measures=[DIRECTED_QUANTITY],
        fields=fields,
        position_type=TRANSACTION,
        # as_of=datetime.now(),
        additional_filters=filters
    )

    if trades is None:
        return None

    def filter_maturing_transactions(position: Position):
        field: FieldMapEntry = FieldMapEntry(field=FieldProto.TRANSACTION_TYPE)
        transaction_type: ProtoEnum = position.get_field(field)

        return transaction_type.enum_value != TransactionTypeProto.MATURATION \
               and transaction_type.enum_value != TransactionTypeProto.MATURATION_OFFSET

    trades = list(filter(filter_maturing_transactions, trades))

    def get_key(position: Position):
        position_proto: PositionProto = position.positionProto
        for field in position_proto.fields:
            if field.field == FieldProto.TRADE_DATE:
                local_date: LocalDateProto = LocalDateProto.FromString(field.field_value_packed.value)
                return local_date.year * 10000 + local_date.month * 100 + local_date.day

        raise ValueError("No trade date available!")

    trades = sorted(trades, key=get_key)

    return trades


def get_ledger_position(security: Security,
                        as_of_date: date = datetime.combine(date=date.today(), time=datetime.min.time())) -> float:
    """
        Gets positions from the ledger based on transaction, but filters out MATURATION transactions.

        TODO: Convert the filtering to use additional filters to only get what we want.
    """
    trades = get_ledger_positions(security, as_of_date)

    total_amount: float = 0.0

    for trade in trades:
        trade: Position
        measure: MeasureMapEntry = MeasureMapEntry(measure=DIRECTED_QUANTITY)
        amount = float(trade.get_measure(measure))
        total_amount = total_amount + amount

    return total_amount


def get_security_cusip(security_id: UUID) -> Tuple[SecurityProto, str]:
    security: Security = SecurityService().get_security_by_uuid(security_id)

    if security is None:
        print("Can't find security or there are multiple: " + str(security_id))
        return None, None

    security_cusip = security.proto.identifier_value
    return security, security_cusip


def recon_psuedocode_new_version(soma_holdings: list, security: Security, portfolio: PortfolioProto):
    '''
    ## ALTERNATIVE LOGIC: ## 1/ Load the SOMA holdings. For each: ## 2/ Check the SOMA trade date of the position in
    the ledger, if it's equal then we're okay. If it's not then we need to create the gap transaction. Restart the
    reconcilation ## 3/ If the amount at the SOMA trade date is the same, then move to the next by issuing a new
    recon call
    '''

    if soma_holdings is None:
        print(
            f"{security.proto.identifier.identifier_value} Need to check for the case where the SOMA report shows no "
            f"transactions, but we have them in the ledger")
        return 0

    for index, soma_holding in enumerate(soma_holdings.to_dict('records')):
        as_of_date: date = get_date(soma_holding['asOfDate'])
        as_of_date = datetime.combine(as_of_date, datetime.max.time())

        position_amount: float = get_ledger_position(security, as_of_date)

        soma_quantity = soma_holding['parValue']
        if soma_quantity == position_amount:
            continue
        else:
            difference = soma_quantity - position_amount

            soma_date: datetime = get_date(soma_holding['asOfDate'])

            create_transaction(portfolio=portfolio, security=security, quantity=difference, price=100.0,
                               trade_date=soma_date)
            recon_psuedocode_new_version(soma_holdings[index:], security=security, portfolio=portfolio)
            return


def reconcile_security(security_cusip: str) -> ReconResult:
    security: list[Security] = get_security_by_id(security_cusip, IdentifierTypeProto.CUSIP)

    if len(security) == 0:
        raise ValueError("No securities found with CUSIP " + security_cusip)
    if len(security) > 1:
        print("Multiple securities")

    security: Security = security[0]

    # 1 Get SOMA report
    is_tips = security.get_security_type() == SecurityTypeProto.TIPS
    soma_holdings, num_soma_holdings = get_soma_holdings(security_cusip, is_tips=is_tips)
    # ledger_holdings, num_ledger_holdings = get_ledger_holdings_old(security_cusip)

    if num_soma_holdings == 0:
        return ReconResult(ReconResultStatus.RECON_GOOD, security.get_security_id().get_identifier_value(),
                           "No soma holdings to recon")

    recon_psuedocode_new_version(soma_holdings, security, portfolio)

    ledger_holdings = get_ledger_positions(
        security, fields=[FieldProto.PORTFOLIO, FieldProto.SECURITY, FieldProto.TRANSACTION_TYPE, FieldProto.TRADE_DATE]
    )

    num_ledger_holdings = len(ledger_holdings)

    # 2 Query transactions for security and portfolio

    if same_number_holdings(num_soma_holdings, num_ledger_holdings):
        return ReconResult(ReconResultStatus.RECON_GOOD, security.get_security_id().get_identifier_value(),
                           f"Recon good for {security_cusip}, equal number of holdings in ledger ({num_ledger_holdings}"
                           f" and soma ({num_soma_holdings}))")
    if is_soma_missing_recent_auction(num_soma_holdings, ledger_holdings, num_ledger_holdings):
        return ReconResult(ReconResultStatus.RECON_GOOD, security.get_security_id().get_identifier_value(),
                           f"""{security_cusip}: There is one more position in the ledger, from a recent auction that 
                           hasn't shown up in the holdings report yet.""")
    if ledger_has_extra_transaction_due_to_auction(soma_holdings, num_soma_holdings, ledger_holdings,
                                                   num_ledger_holdings):
        return ReconResult(ReconResultStatus.RECON_GOOD, security.get_security_id().get_identifier_value(),
                           f"""{security_cusip}: The ledger has one extra transaction, but same overall directed 
                           quantity. Likely due to SOMA operations on the same week as an auction""")

    # Need to query 'asOf'. Query position view with TradeDate <= 'yyyy-mm-dd'
    if num_ledger_holdings > num_soma_holdings:
        latest_trade_date = ledger_holdings[-1].get_field(FieldMapEntry(field=FieldProto.TRADE_DATE))
        days_since_last_download = 45  # We need to run the download and convert xml to json to have the latest data
        if (num_soma_holdings + 1) == num_ledger_holdings and (
                datetime.now().date() - latest_trade_date).days < days_since_last_download:
            # This means the latest transaction (likely from an auction) and the data will come in from a subsquent
            # filing
            return ReconResult(ReconResultStatus.RECON_GOOD, security.identifier,
                               f"""There was an extra entry in the ledger with a recent trade date that likely hasn't 
                               appeared in the soma repot yet""",
                               False)

        cant_process_recon(security_cusip)

        return ReconResult(ReconResultStatus.RECON_BAD, security.identifier,
                           f"""{security_cusip}: The ledger ({num_ledger_holdings}) has more trades than the SOMA 
                           report ({num_soma_holdings}). 
               Reconciliation not supported. This can happen when:
                There  was an auction but the position was sold before the subsequent SOMA holding publication
                ???""")
    else:
        print("Recon process starting: " + security_cusip, flush=True)

    if num_soma_holdings == 0:
        return ReconResult(ReconResultStatus.RECON_GOOD, security.identifier,
                           "CHECK THIS LOGIC: No soma holdings to recon: " + security_cusip)

    return ReconResult(ReconResultStatus.RECON_UNKNOWN, security.identifier,
                       "CHECK THIS LOGIC: Unknown outcome: " + security_cusip)


def ledger_has_extra_transaction_due_to_auction(soma_holdings, num_soma_holdings, ledger_holdings, num_ledger_holdings):
    measure: MeasureMapEntry = MeasureMapEntry(measure=DIRECTED_QUANTITY)
    return num_ledger_holdings > num_soma_holdings and \
           soma_holdings['parValueDiff'].sum() == float(
        sum(position.get_measure(measure) for position in ledger_holdings))


def same_number_holdings(num_soma_holdings, num_ledger_holdings):
    return num_ledger_holdings == num_soma_holdings


def is_soma_missing_recent_auction(num_soma_holdings, ledger_holdings, num_ledger_holdings):
    field = FieldMapEntry(field=FieldProto.TRADE_DATE)
    return num_ledger_holdings == num_soma_holdings + 1 and \
           (date.today() - ledger_holdings[-1].get_field(field)).days < 7


def cant_process_recon(security_cusip):
    print(
        f"{security_cusip}: This can happen because we upload data from the auction data, and then when we reconcile we " +
        "accidentally create another. We should use an as-of query when reconciling so that can handle cases " +
        "where the soma holdings report has an asof date later in the week of the auction data. Ideally we " +
        "change the query to be as-of, so that transactions created earlier in the week include.")

    return ReconResultStatus.RECON_BAD


def reconcile_totals():
    # 1. Query total from ledger, aggregate by type

    # 2. Query total from SOMA
    # https://markets.newyorkfed.org/api/soma/summary.json

    # 3. print differences
    pass


if __name__ == "__main__":
    import timeit

    start_time = timeit.default_timer()

    # 1. Get all securities in the Fed Reserve portfolio
    identifiers = get_field_values(FieldProto.IDENTIFIER)

    import queue
    import threading
    import random

    random.shuffle(identifiers)


    def process_item(item) -> ReconResult:
        try:
            print("Running recon: " + item)
            return reconcile_security(item)
        except Exception as e:
            return ReconResult(ReconResultStatus.RECON_BAD,
                               IdentifierProto(identifier_type=IdentifierTypeProto.CUSIP, identifier_value=item[0]),
                               f"Failure to process {item[0]} with error {e}")


    def worker(_work_queue: queue.Queue, _result_queue: queue.Queue):
        while True:
            item = _work_queue.get()
            if item is None:  # Exit thread if sentinel value is received
                _work_queue.task_done()
                return
            result: ReconResult = process_item(item)
            _result_queue.put(result)

            _work_queue.task_done()


    def print_worker(_result_queue: queue.Queue):
        while True:
            item: ReconResult = _result_queue.get(timeout=60)

            if ReconResultStatus.RECON_GOOD != item.result:
                print(f"""{item.result.name}: ID[{item.security_id.identifier_value}]: {item.message}""")

            _result_queue.task_done()


    work_queue = queue.Queue()
    result_queue = queue.Queue()

    threads = []
    WORKERS = 5
    for _ in range(WORKERS):  # Number of worker threads
        t = threading.Thread(target=worker, args=(work_queue, result_queue))
        t.start()
        threads.append(t)

    for _ in range(1):  # Number of worker threads
        t = threading.Thread(target=print_worker, args=(result_queue,))
        t.start()
        threads.append(t)

    for id in identifiers:
        cusip = id.proto.identifier_value
        work_queue.put(cusip)
        continue

    for _ in range(WORKERS):
        work_queue.put(None)

    work_queue.join()  # Wait for all items to be processed
    for t in threads:
        t.join()  # Wait for all worker threads to exit

    reconcile_totals()

    elapsed = timeit.default_timer() - start_time
    print(f"Time taken: {elapsed}")
