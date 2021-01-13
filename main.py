from URLParser import URLParser


def main():
    index = URLParser('BANKNIFTY')
    handle14Jan = index.get('14JAN2021')
    handle21Jan = index.get('21JAN2021')
    handle28Jan = index.get('28JAN2021')
    handle4Feb = index.get('4FEB2021')

    handle14Jan.join()
    handle21Jan.join()
    handle28Jan.join()
    handle4Feb.join()


if __name__ == "__main__":
    main()