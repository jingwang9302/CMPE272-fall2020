from flask import Flask, request, abort, jsonify, send_file, Response
from sqlitedict import SqliteDict
import qrcode
import webbrowser
import uuid

mydict = SqliteDict('./my_db.sqlite', autocommit=True)

app = Flask(__name__)
app.config["DEBUG"] = True


@app.route("/")
def home():
    return "<h1>Bookmarks Home Page</h1>"


@app.route("/api/bookmarks/all")
def api_all():
    result = []
    for key, value in mydict:
        print(key)
        # result += key
    return 'hello'


@app.route("/api/bookmarks", methods=['POST'])
def createBookmark():
    id = str(uuid.uuid4())
    bookmark = request.json

    for value in mydict.values():
        if (value['url'] == bookmark['url']):
            return 'The given URL already existed in the system.', 400
    bookmark['count'] = 0
    bookmark['id'] = id
    mydict[id] = bookmark
    return id, 201


@app.route("/api/bookmarks/<bookmarkId>", methods=['GET'])
def getBookmark(bookmarkId):
    bookmark = mydict.get(bookmarkId)
    if bookmark is None:
        return '', 404

    bookmark['count'] += 1
    mydict[bookmark['id']] = bookmark
    print(bookmark)
    return bookmark


@app.route("/api/bookmarks/<bookmarkId>", methods=['DELETE'])
def deleteBookmark(bookmarkId):
    mydict.pop(bookmarkId)
    return '', 204


@app.route("/api/bookmarks/<id>/qrcode", methods=['GET'])
def getQrcode(id):
    url = mydict[id]['url']
    img = qrcode.make(url)
    filename = './' + id + '.png'
    img.save(filename)
    return send_file(filename, mimetype='image/gif')


@app.route("/api/bookmarks/<id>/stats", methods=['GET'])
def getBookmarkStats(id):
    etag = request.headers.get('Etag')
    bookmark = mydict.get(id)
    etag_db = bookmark['count']
    res = Response()
    res.headers['ETag'] = etag_db
    if bookmark is None:
        return '', 404
    if str(etag_db) == str(etag):
        return res, 304
    return res, 200


app.run()
