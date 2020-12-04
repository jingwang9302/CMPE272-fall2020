from client_producer import Client
from flask import Flask, request

app = Flask(__name__)
client = Client()


@app.route('/api/put', methods=['PUT'])
def put_data():
    num = request.args['num']
    res = client.put_data(
        {'key': f'key-{num}', 'value': f'value-{num}', 'op': 'PUT'})
    return res


@app.route('/api/getone', methods=['GET'])
def get_one():
    num = request.args['num']
    res = client.get_one_data(
        {'key': f'key-{num}', 'value': '', 'op': 'GET_ONE'})
    return res


@ app.route('/api/getall', methods=['GET'])
def get_all():
    res = client.get_all_data()
    return res


@ app.route('/api/deletenode', methods={'PUT'})
def delete_node():
    port = request.args['port']
    res = client.delete_node(port)
    return res


@ app.route('/api/addnode', methods={'PUT'})
def add_node():
    port = request.args['port']
    res = client.add_node(port)
    return res


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000)
