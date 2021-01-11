from URLParser import URLParser


def main():
    index = URLParser('TCS')
    index.get('28JAN2021')
    index.get('25FEB2021')
    index.get('25MAR2021')


if __name__ == "__main__":
    main()