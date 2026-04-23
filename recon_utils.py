from datetime import datetime, time

import pandas as pd
import requests

# Define the REST endpoint URL
from fintekkers.models.position.field_pb2 import FieldProto
from fintekkers.models.position.measure_pb2 import MeasureProto
from fintekkers.models.position.position_pb2 import PositionTypeProto
from fintekkers.models.position.position_util_pb2 import FieldMapEntry, PositionFilterOperator
from fintekkers.models.security.security_pb2 import SecurityProto
from fintekkers.wrappers.models.position import Position
from fintekkers.wrappers.models.security import Security
from fintekkers.wrappers.models.util.date_utils import get_date_proto

from portfolios import PORTFOLIO
from positions import get_position


def get_trade_date_filter(tmp: datetime):
    return FieldMapEntry(
        field=FieldProto.TRADE_DATE,
        field_value_packed=Position.pack_field(get_date_proto(tmp)),
        operator=PositionFilterOperator.LESS_THAN_OR_EQUALS)


def print_mismatches_between_ledger_and_soma(recon_security: Security):
    recon_cusip = recon_security.get_security_id().get_identifier_value()
    url = f"https://markets.newyorkfed.org/api/soma/tsy/get/cusip/{recon_cusip}.json"

    # Make a GET request to the endpoint
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Extract the JSON data from the response
        data = response.json()

        # Use json_normalize to flatten the data and load it into a DataFrame
        sigle_security_df = pd.json_normalize(data['soma']['holdings'])
    else:
        sigle_security_df = None
    # print(sigle_security_df)

    for i in range(0, len(sigle_security_df)):
        row = sigle_security_df.iloc[i, :]

        cusip, soma_quantity, asOfDate = row['cusip'], float(row['parValue']), row['asOfDate']

        tmp_as_of = datetime.strptime(asOfDate, "%Y-%m-%d")
        specific_time = time(23, 59, 59)

        tmp_as_of = datetime.combine(tmp_as_of.date(), specific_time)

        recon_position = get_position(recon_security, PORTFOLIO.proto, [MeasureProto.DIRECTED_QUANTITY], \
                                      PositionTypeProto.TRANSACTION,
                                      additional_filters=[get_trade_date_filter(tmp_as_of)])

        recon_position = recon_position[0]

        ledger_quantity = recon_position.get_measure_value(MeasureProto.DIRECTED_QUANTITY)

        if (ledger_quantity - soma_quantity) == 0:
            # print(f"{asOfDate} match: {ledger_quantity}")
            continue
        else:
            print(f"{asOfDate} mismatch: Ledger: {ledger_quantity}. SOMA: {soma_quantity}")
