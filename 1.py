from fintekkers.models.position.field_pb2 import FieldProto
from fintekkers.wrappers.models.portfolio import Portfolio
from fintekkers.wrappers.requests.portfolio import QueryPortfolioRequest
from fintekkers.wrappers.services.portfolio import PortfolioService

from portfolios import PORTFOLIO
PORTFOLIO.proto

from fintekkers.wrappers.services.util.Environment import get_channel
get_channel()._channel.target()


import requests as r
from securities import create_security, get_security_by_id, get_security_ids, get_field_values, upload_security_from_data_dict
from positions import get_position, create_dataframe_from_response
from soma import check_errors_in_soma
from reconcile_transactions import reconcile_security, get_soma_holdings
from datetime import datetime, date, time
from recon_utils import get_trade_date_filter

from fintekkers.models.position.measure_pb2 import MeasureProto
from fintekkers.models.position.field_pb2 import FieldProto
from fintekkers.models.security.identifier.identifier_type_pb2 import IdentifierTypeProto
from fintekkers.models.position.position_pb2 import PositionTypeProto, PositionViewProto
from fintekkers.models.position.position_util_pb2 import FieldMapEntry, PositionFilterOperator
from fintekkers.models.security.security_pb2 import SecurityProto

from fintekkers.wrappers.models.security import Security
from fintekkers.wrappers.models.position import Position
from fintekkers.requests.security.create_security_response_pb2 import CreateSecurityResponseProto
from fintekkers.wrappers.models.util.date_utils import get_date_proto

from pprint import pprint
import numpy as np

import plotly.offline as plotly

import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
pd.options.display.float_format = '{:,.2f}'.format

# Set notebook mode to work in offline
# plotly.init_notebook_mode()
pd.options.plotting.backend = "plotly"



#/datetime(2003,7,9, 23, 59, 59) #
as_of_date = datetime(2024,7,24, 23, 59, 59, 99)#Used for any as of dated analysis. Ideally matches when Fed issues data so recon is easier
# as_of_date = datetime.now()
as_of_date_str = as_of_date.strftime('%Y-%m-%d')
soma_as_of_url = f"https://markets.newyorkfed.org/read?productCode=30&startDt={as_of_date_str}&endDt={as_of_date_str}&query=details&holdingTypes=bills,notesbonds,frn,tips&format=csv"

trade_date_filter:FieldMapEntry = get_trade_date_filter(as_of_date)

positions = get_position(security=None, portfolio=PORTFOLIO.proto,
                            measures=[MeasureProto.DIRECTED_QUANTITY],
                            position_type=PositionTypeProto.TRANSACTION,
                            fields=[
                                FieldProto.IDENTIFIER,
                                FieldProto.TRANSACTION_TYPE,
                                FieldProto.TRADE_DATE,
                                FieldProto.MATURITY_DATE,
                                FieldProto.ISSUE_DATE,
                                FieldProto.PRODUCT_TYPE
                            ],
                             additional_filters=[trade_date_filter],
                        as_of=datetime.now())

if positions is None or len(positions) == 0:
    print("No results found")
else:
    df_txns = create_dataframe_from_response(positions)

    df_txns['IDENTIFIER'] = df_txns['IDENTIFIER'].astype(str)
    ##Filtering out cash USD transactions. TODO: Migrate this into a filter
    df_txns = df_txns[~df_txns["IDENTIFIER"].str.contains("USD")]
    df_txns["TRADE_DATE"] = pd.to_datetime(df_txns["TRADE_DATE"], infer_datetime_format=True)
    df_txns.set_index("TRADE_DATE", inplace=True)

    df_txns.sort_index(inplace=True)



colors = px.colors.qualitative.T10

df_txns['TRANSACTION_TYPE'] = df_txns['TRANSACTION_TYPE'].astype(str)
pivot = pd.pivot_table(data=df_txns, index="TRADE_DATE", columns=['TRANSACTION_TYPE'], values="DIRECTED_QUANTITY", aggfunc='sum')
pivot = pivot[pivot.index > '2022-01-01']
pivot = pivot.resample('W').sum()

# print(f"Columns in results are: {pivot.columns}")

if "MATURATION_OFFSET" in pivot.columns:
#     print("Grouping maturations and maturation offsets")
    #TODO: Need to decide if MATURATION_OFFSET should be an externally visible concept.
    #I think ideally it is not exposed as it requires the user of the system to understand the extra complexity
    pivot['MATURATION'] = pivot['MATURATION'] + pivot["MATURATION_OFFSET"]
    del pivot["MATURATION_OFFSET"]


fig = px.bar(pivot,
             x = pivot.index,
             y = None,
             template = 'plotly_dark',
             color_discrete_sequence = colors,
             title = 'Bond purchases over time',
             labels=dict(value="Face value (billions)", TRADE_DATE="Monthly purchases vs. maturing bonds",
                         variable="Transaction type")
             )


fig.add_bar(x = pivot.index, y = pivot['BUY'], name = "Purchases")

if "SELL" in pivot.columns:
    fig.add_bar(x = pivot.index, y = pivot['SELL'], name = "Sales")

if "MATURATION" in pivot.columns:
    fig.add_bar(x = pivot.index, y = pivot['MATURATION'], name = "Maturation")

fig.update_layout(xaxis_tickangle=-45)

fig.update_layout(
    yaxis_title="Face value in $"#, yaxis_title="7 day avg"
)

# fig.show()
fig.write_image("1.png")
