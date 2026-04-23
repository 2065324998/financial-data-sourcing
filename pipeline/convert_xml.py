import xmltodict

import os as _os, sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".."))
from auction_data import RawAuctionData

import os
import multiprocessing as mp


def get_data(file: str) -> RawAuctionData:
    if ".xml" not in file.lower():
        return None

    xml = ""
    try:
        xml = open(file).read()
    except:
        raise ValueError("?" + file)

    if len(xml) == 0:
        # print("File is empty: " + file)
        return None

    json = None
    try:
        json = xmltodict.parse(xml)
    except Exception as e:
        print("Probably not XML: " + file)
        return None

    output = RawAuctionData()

    root = {}
    if "td:AuctionData" in json:
        root = json["td:AuctionData"]
    elif "bpd:AuctionData" in json:
        root = json["bpd:AuctionData"]
    elif "bpd:AICAuctionData" in json:
        root = json["bpd:AICAuctionData"]
    elif "bpd:GASData" in json:
        return None
    elif "bpd:STRIPSData" in json:
        return None
    elif "bpd:ODMDataFeed" in json:
        return None
    elif "bpd:MSPDData" in json:  # Public debt
        return None
    elif "bpd:RefCPIData" in json:  # Inflation data'bpd:RefCPIData'
        return None
    elif "bpd:RECLASSData" in json:  # No idea what this is
        return None
    elif "SpecialAnnouncement" in json:
        return None
    elif "NCRAuctionData" in json:  # Non competitive results
        return None
    elif (
        "td:PendingAuctionData" in json
    ):  # future auctions that don't have all the details yet
        return None
    elif (
        "buyback" in json
    ):  # future auctions that don't have all the details yet
        # print("Buyback json ; not sure what this is")
        # print(json)
        return None

    else:
        print(json.keys())
        raise ValueError("Doh: " + file)

    announcement = root["AuctionAnnouncement"]

    output.security_type = announcement["SecurityType"]
    output.issue_date = announcement["IssueDate"]
    output.maturity_date = announcement["MaturityDate"]
    output.dated_date = announcement["DatedDate"]
    output.security_term_week_year = announcement["SecurityTermWeekYear"]
    output.cusip = announcement["CUSIP"]
    output.reopening_indicator = announcement["ReOpeningIndicator"]
    output.original_issue_date = announcement["OriginalIssueDate"]
    output.announcement_date = announcement["AnnouncementDate"]

    if (
        "InflationIndexSecurity" in announcement
        and announcement["InflationIndexSecurity"] == "Y"
    ):  #'TIINConversionFactor' in auctionResults:
        # print(f"{output.cusip} appears to be a TIPS instrument")
        output.security_type = "TIPS"

    if "AuctionResults" not in root:
        return None
    if "AuctionResults" in root:
        auctionResults = root["AuctionResults"]

        if "Spread" in auctionResults and auctionResults["Spread"] is not None:
            # print(f"{output.cusip} appears to be a FRN instrument")
            output.security_type = "FRN"
            output.spread = auctionResults["Spread"]

        # InterestRate is the bond coupon rate (set in AuctionResults after clearing).
        # InvestmentRate is used for T-bills. Prefer InterestRate when present.
        interest_rate = auctionResults.get("InterestRate")
        investment_rate = auctionResults.get("InvestmentRate")
        if interest_rate is not None and interest_rate != "":
            output.investment_rate = interest_rate
        elif investment_rate is not None and investment_rate != "":
            output.investment_rate = investment_rate
        else:
            output.investment_rate = None

        output.soma_accepted = (auctionResults["SOMAAccepted"],)
        output.total_accepted = (auctionResults["TotalAccepted"],)

        output.bid_to_cover_ratio = (auctionResults["BidToCoverRatio"],)
        output.low_price = (auctionResults["LowPrice"],)
        output.median_price = (auctionResults["MedianPrice"],)
        output.high_price = (auctionResults["HighPrice"],)
        output.low_yield = (auctionResults["LowYield"],)
        output.median_yield = (auctionResults["MedianYield"],)
        output.high_yield = (auctionResults["HighYield"],)

        if "ResultsPDFName" in auctionResults:
            output.results_pdf_name = (auctionResults["ResultsPDFName"],)

        if (
            "AccruedInterest" in auctionResults
            and auctionResults["AccruedInterest"] is not None
        ):
            output.accrued_interest = float(auctionResults["AccruedInterest"])

        if (
            "CurrentlyOutstanding" in announcement
            and announcement["CurrentlyOutstanding"] is not None
        ):
            output.currently_outstanding = float(announcement["CurrentlyOutstanding"])

        if (
            "OfferingAmount" in announcement
            and announcement["OfferingAmount"] is not None
        ):
            output.offering_amount = float(announcement["OfferingAmount"])

        if (
            "MatureSecurityAmount" in announcement
            and announcement["MatureSecurityAmount"] is not None
        ):
            output.mature_security_amount = float(announcement["MatureSecurityAmount"])

        if (
            "TotalAccepted" in announcement
            and announcement["TotalAccepted"] is not None
        ):
            output.total_accepted = float(auctionResults["TotalAccepted"])

    return output



def convert_xml_to_json(file: str):
    # if "R_20191224_1.XML" not in file:
    #     return

    data = get_data("data/raw_xml/" + file)

    if data is None:
        return

    path = "data/raw_json/" + data.cusip + "_" + file.replace(".xml", ".json").replace(".XML", ".json")

    # Check if file exists
    if os.path.exists(path):
        # Check if file contents have changed
        with open(path, 'r') as existing_file:
            existing_data = existing_file.read()
            if existing_data == data.to_json():
                # print(f"No changes detected, skipping {path}")
                return
            else:
                print(f"Detected changes to {path}")

    # Write to file
    with open(path, 'w') as output:
        print(f"Writing to {path}")
        output.write(data.to_json())


if __name__ == "__main__":
    print(os.getcwd())
    files = os.listdir("data/raw_xml")

    p = mp.Pool(processes=100)
    p.map(convert_xml_to_json, files)
