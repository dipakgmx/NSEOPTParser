from URLParser import URLParser


def main():
    index = URLParser('BANKNIFTY')
    index.get('14JAN2021')
    index.get('21JAN2021')
    index.get('28JAN2021')
    index.get('4FEB2021')


if __name__ == "__main__":
    main()