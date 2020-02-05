from src.sparkhelpers import spark_start
from src.sparkhelpers import clean_data

def main():
    ss = spark_start()
    df = ss.read.csv('Hidden-Gems/test/amsterdam-accommodation.csv', header='true')
    df.show(2)

    clean_data(ss, 'Hidden-Gems/test/amsterdam-accommodation.csv')

if __name__ == "__main__":
    main()