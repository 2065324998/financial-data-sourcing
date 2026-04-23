from datetime import datetime
import pandas as pd

from env import CHANNEL

from fintekkers.services.position_service.position_service_pb2_grpc import PositionStub

position_service: PositionStub = PositionStub(CHANNEL)

from fintekkers.models.position.field_pb2 import FieldProto
from fintekkers.models.position.measure_pb2 import MeasureProto
from fintekkers.models.position.position_filter_pb2 import PositionFilterProto
from fintekkers.models.position.position_pb2 import PositionTypeProto, PositionViewProto
from fintekkers.models.position.position_util_pb2 import FieldMapEntry, MeasureMapEntry

from fintekkers.requests.position.query_position_request_pb2 import (
    QueryPositionRequestProto,
)
from fintekkers.requests.position.query_position_response_pb2 import (
    QueryPositionResponseProto,
)

from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto
from fintekkers.models.security.security_pb2 import SecurityProto
from fintekkers.models.portfolio.portfolio_pb2 import PortfolioProto
from fintekkers.models.util.local_timestamp_pb2 import LocalTimestampProto

from fintekkers.wrappers.models.security import Security
from fintekkers.wrappers.models.position import Position
from fintekkers.wrappers.models.util.serialization import ProtoSerializationUtil

from google.protobuf.any_pb2 import Any


def get_position(
    security: Security,
    portfolio: PortfolioProto,
    measures: list[MeasureProto],
    position_type: PositionTypeProto,
    fields: list[FieldProto] = [FieldProto.PORTFOLIO, FieldProto.SECURITY],
    additional_filters: list[FieldMapEntry] = [],
    as_of=datetime.now(),
) -> list[Position]:

    filters = []

    if security is not None:
        id_proto = IdentifierProto(
            identifier_value=security.get_security_id().get_identifier_value(),
            identifier_type=security.get_security_id().get_identifier_type(),
        )
        security_id_packed: Any = Any()
        security_id_packed.Pack(msg=id_proto)

        filters.append(
            FieldMapEntry(
                field=FieldProto.IDENTIFIER, field_value_packed=security_id_packed
            )
        )

    if portfolio is not None:
        filters.append(
            FieldMapEntry(
                field=FieldProto.PORTFOLIO_NAME,
                string_value=portfolio.portfolio_name,
            )
        )

    if additional_filters is not None and len(additional_filters) > 0:
        for tmp_filter in additional_filters:
            filters.append(tmp_filter)

    filter_fields: PositionFilterProto = PositionFilterProto(filters=filters)

    as_of_proto: LocalTimestampProto = ProtoSerializationUtil.serialize(as_of)

    request: QueryPositionRequestProto = QueryPositionRequestProto(
        position_type=position_type,
        position_view=PositionViewProto.DEFAULT_VIEW,
        fields=fields,
        measures=measures,
        filter_fields=filter_fields,
        as_of=as_of_proto,
    )

    responses = position_service.Search(request)
    positions = []

    try:
        while not responses._is_complete():
            response: QueryPositionResponseProto = responses.next()

            for positionProto in response.positions:
                positions.append(Position(positionProto=positionProto))
    except StopIteration:
        pass
    except Exception as e:
        print(e)
        raise e

    responses.cancel()
    return positions


def create_dataframe_from_response(positions: list[Position]):
    dict_list = []

    if positions is None or len(positions) == 0:
        print("No response from server, cannot create df")
        return

    for position in positions:
        dictionary = {}

        for field in position.get_fields():
            field: FieldMapEntry
            dictionary[FieldProto.DESCRIPTOR.values_by_number[field.field].name] = (
                position.get_field_display(field)
            )

        for measure in position.get_measures():
            measure: MeasureMapEntry
            dictionary[
                MeasureProto.DESCRIPTOR.values_by_number[measure.measure].name
            ] = position.get_measure(measure)

        dict_list.append(dictionary)

    return pd.DataFrame(dict_list)


if __name__ == "__main__":

    from fintekkers.models.security.identifier.identifier_type_pb2 import (
        IdentifierTypeProto,
    )

    from portfolios import PORTFOLIO

    portfolio_name = "Federal Reserve SOMA Holdings"
    portfolio = PORTFOLIO.proto

    from securities import get_security_by_id

    recon_security = get_security_by_id(
        identifier_type=IdentifierTypeProto.CUSIP, identifier="912796Y37"
    )[0]

    as_of_date = datetime(
        2022, 11, 16
    )  # Used for any as of dated analysis. Ideally matches when Fed issues data so recon is easier
    as_of_date_str = as_of_date.strftime("%Y-%m-%d")

    trades = get_position(
        security=None,
        portfolio=portfolio,
        measures=[MeasureProto.DIRECTED_QUANTITY],
        position_type=PositionTypeProto.TRANSACTION,
        fields=[
            # HAD TO COMMENT OUT EXTRA FIELDS DUE TO SIZE LIMITATIONS
            FieldProto.PORTFOLIO,
            FieldProto.TRADE_DATE,
            FieldProto.IDENTIFIER,
            FieldProto.TRANSACTION_TYPE,
            FieldProto.PRODUCT_TYPE,
        ],
        as_of=as_of_date,
    )

    transaction_view = create_dataframe_from_response(trades)
    transaction_view.sort_values("TRADE_DATE", inplace=True)

    transaction_view[(transaction_view.TRANSACTION_TYPE.str.contains("BUY"))][
        "DIRECTED_QUANTITY"
    ].sum()

    from portfolios import PORTFOLIO
    portfolio = PORTFOLIO.proto

    response = get_position(
        security=None,
        portfolio=portfolio,
        measures=[MeasureProto.DIRECTED_QUANTITY],
        position_type=PositionTypeProto.TAX_LOT,
        fields=[
            FieldProto.PORTFOLIO,
            FieldProto.SECURITY_DESCRIPTION,
            FieldProto.PORTFOLIO_NAME,
            FieldProto.TAX_LOT_OPEN_DATE,
        ],
    )

    df_tax_lots = create_dataframe_from_response(response)

    df_tax_lots["TAX_LOT_OPEN_DATE"] = pd.to_datetime(
        df_tax_lots["TAX_LOT_OPEN_DATE"], infer_datetime_format=True
    )
    df_tax_lots = df_tax_lots[~df_tax_lots.SECURITY_DESCRIPTION.str.contains("USD")]
    df_tax_lots.set_index("TAX_LOT_OPEN_DATE", inplace=True)
    df_tax_lots.sort_index(inplace=True)
    df_tax_lots["CUM_SUM"] = df_tax_lots["DIRECTED_QUANTITY"].cumsum()
    monthly_lots = df_tax_lots.DIRECTED_QUANTITY.resample("MS").sum()
    print(monthly_lots.tail())

    positions = get_position(
        security=None,
        portfolio=portfolio,
        measures=[MeasureProto.DIRECTED_QUANTITY],
        position_type=PositionTypeProto.TRANSACTION,
        fields=[
            # HAD TO COMMENT OUT EXTRA FIELDS DUE TO SIZE LIMITATIONS
            FieldProto.PORTFOLIO,
            FieldProto.IDENTIFIER,
            FieldProto.TRANSACTION_TYPE,
            FieldProto.TRADE_DATE,
            FieldProto.PORTFOLIO_NAME,
            FieldProto.ASSET_CLASS,
        ],
    )

    if positions is None or len(positions) == 0:
        print("No results found")
    else:
        df_txns = create_dataframe_from_response(positions)

        # df_txns['IDENTIFIER'] = df_txns['IDENTIFIER'].astype(str)
        df_txns = df_txns[~df_txns["IDENTIFIER"].str.contains("USD")]
        df_txns["TRADE_DATE"] = pd.to_datetime(
            df_txns["TRADE_DATE"], infer_datetime_format=True
        )
        df_txns.set_index("TRADE_DATE", inplace=True)

        df_txns.sort_index(inplace=True)
