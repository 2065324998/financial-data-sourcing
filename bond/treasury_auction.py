from datetime import date
import json

from fintekkers.models.security.identifier.identifier_type_pb2 import IdentifierTypeProto
from fintekkers.wrappers.models.util.date_utils import get_date
from fintekkers.models.security.security_type_pb2 import BOND_SECURITY, TIPS, FRN


class RawAuctionData:
    def __init__(self, **kwargs) -> None:
        self.maturity_date: str = kwargs.get("maturity_date", "")
        self.issue_date: str = kwargs.get("issue_date", "")
        self.security_type: str = kwargs.get("security_type", "")
        self.auction_type: str = kwargs.get("auction_type", "")
        self.auction_date: str = kwargs.get("auction_date", "")
        self.auction_close_date: str = kwargs.get("auction_close_date", "")
        self.auction_result_date: str = kwargs.get("auction_result_date", "")
        self.cusip: str = kwargs.get("cusip", "")
        self.announcement_date: str = kwargs.get("announcement_date", "")
        self.original_issue_date: str = kwargs.get("original_issue_date", None)
        self.dated_date: str = kwargs.get("dated_date", "")
        self.reopening_indicator: str = kwargs.get("reopening_indicator", "")
        self.security_term_week_year: str = kwargs.get("security_term_week_year", "")
        self.security_term_month_year: str = kwargs.get("security_term_month_year", "")
        self.investment_rate: str = kwargs.get("investment_rate", "0.0")
        self.soma_accepted: str = kwargs.get("soma_accepted", "")
        self.total_accepted: str = kwargs.get("total_accepted", "")

        self.low_yield: str = kwargs.get("low_yield", "")
        self.median_yield: str = kwargs.get("median_yield", "")
        self.high_yield: str = kwargs.get("high_yield", "")
        self.low_price: str = kwargs.get("low_price", "")
        self.median_price: str = kwargs.get("median_price", "")
        self.high_price: str = kwargs.get("high_price", "")
        self.bid_to_cover_ratio: str = kwargs.get("bid_to_cover_ratio", "")
        self.tendered: str = kwargs.get("tendered", "")
        self.awarded: str = kwargs.get("awarded", "")
        self.spread: str = kwargs.get("spread", "")
        self.results_pdf_name = kwargs.get("results_pdf_name", "")
        self.accrued_interest = kwargs.get("accrued_interest", "")
        self.currently_outstanding = kwargs.get("currently_outstanding", "")
        self.offering_amount = kwargs.get("offering_amount", "")
        self.mature_security_amount = kwargs.get("mature_security_amount", "")

    @staticmethod
    def from_json(json_string: str) -> "RawAuctionData":
        return json.loads(json_string, object_hook=lambda d: RawAuctionData(**d))

    def to_json(self) -> str:
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def get_issue_date(self) -> date:
        issue_date_str = (
            self.original_issue_date
            if self.original_issue_date is not None
            else self.issue_date
        )
        return get_date(issue_date_str).date()

    def get_announcement_date(self) -> date:
        return get_date(self.announcement_date).date()

    def get_dated_date(self) -> date:
        if self.dated_date:
            return get_date(self.dated_date).date()
        return self.get_issue_date()

    def get_maturity_date(self) -> date:
        return get_date(self.maturity_date).date()

    def get_security_type_proto(self):
        security_type = BOND_SECURITY
        if self.security_type == "TIPS":
            security_type = TIPS
        elif self.security_type == "FRN":
            security_type = FRN

        return security_type

    def get_coupon_rate_float(self):
        import math
        # investment_rate is the authoritative coupon rate when present
        # (bonds: from InterestRate in AuctionResults; bills: from InvestmentRate)
        if self.investment_rate is not None and self.investment_rate != "":
            return float(self.investment_rate)

        # Fallback: derive from high_yield using Treasury's coupon-setting rule —
        # nearest 1/8% (0.125%) BELOW the highest accepted yield.
        if (
            self.high_yield
            and len(self.high_yield) > 0
            and self.high_yield[0] is not None
            and self.high_yield[0] != ""
        ):
            high_yield = float(self.high_yield[0])
            return math.floor(high_yield / 0.125) * 0.125

        return 0.0

    def get_quantity_bought_by_soma(self) -> float:
        if (
            self.soma_accepted
            and len(self.soma_accepted) > 0
            and self.soma_accepted[0] is not None
        ):
            return float(self.soma_accepted[0])

    def get_price_paid_by_soma(self) -> float:
        if (
            self.high_price
            and len(self.high_price) > 0
            and self.high_price[0] is not None
        ):
            return float(self.high_price[0])

        return 100.0

    ## This is the amount offered by the Fed, but not necessarily the total amount
    ## accepted. This is the amount used in announcements to help the market gauge
    ## sizing
    def get_offering_amount(self) -> float:
        if self.offering_amount is not None and self.offering_amount != "":
            return float(self.offering_amount) * 1000000000

    ## This is the amount offered by the Fed, but not necessarily the total amount
    ## accepted. This is the amount used in announcements to help the market gauge
    ## sizing
    def get_total_accepted(self) -> float:
        if (
            self.total_accepted
            and len(self.total_accepted) > 0
            and self.total_accepted[0] is not None
        ):
            return float(self.total_accepted[0])

    # Fed auctions show outstanding quantity as pre-auction quantity, so
    # here we add the total accepted to get to the amount post auction held by SOMA
    def get_post_auction_amount_outstanding(self) -> float:
        if self.currently_outstanding is not None and self.currently_outstanding != "":
            return float(self.currently_outstanding) + self.get_total_accepted()
        else:
            return self.get_total_accepted()

    ####
    # Estimated Amount of Maturing Bills Held by the Public
    def get_maturity_security_amount(self) -> float:
        if (
            self.mature_security_amount is not None
            and self.mature_security_amount != ""
        ):
            return float(self.mature_security_amount)

if __name__ == "__main__":
    entry = {'issue_date': '1998-11-16',
   'maturity_date': '2028-11-15',
   'cusip': '912810FF0',
   'investment_rate': '5.250000000',
   'security_type': 'BOND',
   'accrued_interest': [0],
   'original_issue_date': '1998-11-16',
   'announcement_date': '1998-11-16',
   'dated_date': '1998-11-16'}

    import json
    entry_json = json.dumps(entry)
    data: RawAuctionData = RawAuctionData.from_json(entry_json)
    print(data.get_issue_date())

    from bond.treasury import upload_security_from_data_dict
    upload_security_from_data_dict(data)

    from bond.treasury import get_security_by_id
    security = get_security_by_id(data.cusip, IdentifierTypeProto.CUSIP)
    print(security)

