from src.sparkhelpers import spark_start

def main():
    ss = spark_start()
    df = ss.read.csv('Hidden-Gems/test/amsterdam-accommodation.csv')
    df.show(2)

if __name__ == "__main__":
    main()