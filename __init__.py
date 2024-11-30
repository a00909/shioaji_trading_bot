from quote import QuoteManager
from utils import get_api


def main():
    api = get_api(simulation=True)
    quote = QuoteManager(api)
    quote.subscribe_stk_tick(["2330"])



if __name__ == "__main__":
    main()