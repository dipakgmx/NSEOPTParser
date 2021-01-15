from URLParser import URLParser


def main():
    index = URLParser('BANKNIFTY')

    handle21Jan = index.get('21JAN2021')
    handle28Jan = index.get('28JAN2021')
    handle4Feb = index.get('4FEB2021')
    handle11Feb = index.get('11FEB2021')
    handle18Feb = index.get('18FEB2021')
    handle25Feb = index.get('25FEB2021')
    handle4Mar = index.get('4MAR2021')
    handle10Mar = index.get('10MAR2021')
    handle25Mar = index.get('25MAR2021')

    handle21Jan.join()
    handle28Jan.join()
    handle4Feb.join()
    handle11Feb.join()
    handle18Feb.join()
    handle25Feb.join()
    handle4Mar.join()
    handle10Mar.join()
    handle25Mar.join()


if __name__ == "__main__":
    main()
