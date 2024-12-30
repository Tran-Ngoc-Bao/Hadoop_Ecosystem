from confluent_kafka import Producer
import requests
import json

def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12}".format(topic=msg.topic(), key=msg.key().decode('utf-8')))

def solution():
    global year
    global month

    config = {'bootstrap.servers': 'kafka:9092', 'acks': 'all'}
    producer = Producer(config)

    topic = f'flight_data_{year}'
    url = 'http://service:5000/api/get_data'
    params = {'year': year, 'month': month, 'offset': 0, 'limit': 100}

    while True:
        try:
            r = requests.get(url=url, params=params)
            data = r.json()

            if data['status'] == 'error':
                break

            if data['status'] == 'success' or data['status'] == 'complete':
                key = str(year) + '_' + str(month) + '_' + str(params['offset'])
                print(key)
                value = json.dumps(data['data'])
                producer.produce(topic, value, key, callback=delivery_callback)
                
                if data['status'] == 'complete':
                    break

                params['offset'] += 100
        except Exception as e:
            print(e)
            break

    producer.flush()

    if month == 12:
        year += 1
        month = 1
    else:
        month += 1

if __name__ == "__main__":
    year = 2018
    month = 1

    # flag = True
    # while flag:
    #     try:
    #         solution()
    #     except:
    #         print("Don't worry about this error", year, month)
    #         flag = False
    solution()
    