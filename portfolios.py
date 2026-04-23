from fintekkers.models.portfolio.portfolio_pb2 import PortfolioProto
from fintekkers.requests.portfolio.create_portfolio_response_pb2 import CreatePortfolioResponseProto
from fintekkers.wrappers.models.portfolio import Portfolio
from fintekkers.wrappers.services.portfolio import PortfolioService
from fintekkers.wrappers.requests.portfolio import CreatePortfolioRequest

PORTFOLIO_NAME = "Federal Reserve SOMA Holdings"

# Will create or update
portfolioService = PortfolioService()
request = CreatePortfolioRequest.create_portfolio_request(PORTFOLIO_NAME)
response: CreatePortfolioResponseProto = portfolioService.create_or_update(request)

PORTFOLIO: Portfolio = None

if response.portfolio_response is not None:
    PORTFOLIO = Portfolio(response.portfolio_response[0])
else:
    print("Could not get, nor create portfolio: " + PORTFOLIO_NAME)

if __name__ == "__main__":
    print(PORTFOLIO.get_name())
