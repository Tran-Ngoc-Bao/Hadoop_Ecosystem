from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

@app.route('/api/get_data', methods=['GET'])
def get_data():
    try:
        year = request.args.get('year')
        month = request.args.get('month')
        offset = int(request.args.get('offset'))
        limit = int(request.args.get('limit'))

        df = pd.read_csv(f'/data/Flights_{year}_{month}.csv')
        cnt = len(df)

        if offset + limit >= cnt:
            result = df.iloc[offset:cnt-offset]
            return jsonify({'status': 'complete', 'data': result.to_dict(orient='records')})

        result = df.iloc[offset:offset+limit]
        return jsonify({'status': 'success', 'data': result.to_dict(orient='records')})
    except Exception as e:
        print(e)
        return jsonify({'status': 'error', 'data': 'error'})

if __name__ == '__main__':
    app.run(debug=True, port=5000)