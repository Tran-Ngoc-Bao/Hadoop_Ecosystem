import pandas
import os

def check_leap_year(y):
    yi = int(y)
    if yi % 4:
        return False
    if yi % 400 == 0:
        return True
    if yi % 100 == 0:
        return False
    return True

def increase_time(y, m, d):
    yi = int(y)
    mi = int(m)
    di = int(d)
    if m == "12" and d == "31":
        return str(yi + 1) + " 1 1"
    if d == "31":
        return y + " " + str(mi + 1) + " 1"
    if m == "2" and (d == "29" or (check_leap_year(yi) == False and d == "28")):
        return y + " 3 1"
    if d == "30" and mi in [4, 6, 9, 11]:
        return y + " " + str(mi + 1) + " 1"
    return y + " " + m + " " + str(di + 1)

def solution():
    f = open("./airflow/source/time.txt", "r")
    s = f.read().split(" ")
    f.close()
    year = s[0]
    month = s[1]
    day = s[2]

    df = pandas.read_parquet("./airflow/source/flight_data/" + year + "/" + month + "/" + day + ".parquet")
    num_of_rows = len(df)
    keys = []
    head = int(year) * 10000000000 + int(month) * 100000000 + int(day) * 1000000
    for i in range(num_of_rows):
        keys.append(str(head + i))
    keys_df = pandas.DataFrame(keys, columns=["id"])
    result = pandas.concat([df, keys_df], axis=1)

    if not os.path.exists("./airflow/source/flight_data/added_key/" + year):
        os.makedirs("./airflow/source/flight_data/added_key/" + year)
    if not os.path.exists("./airflow/source/flight_data/added_key/" + year + "/" + month):
        os.makedirs("./airflow/source/flight_data/added_key/" + year + "/" + month)

    result.to_parquet("./airflow/source/flight_data/added_key/" + year + "/" + month + "/" + day + ".parquet")

    f = open("./airflow/source/time.txt", "w")
    f.write(increase_time(year, month, day))
    f.close()

if __name__ == "__main__":
    flag = True
    while flag:
        try:
            solution()
        except:
            print("Don't worry about this error")
            flag = False
    # solution()
