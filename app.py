from flask import Flask, render_template, jsonify
from pymongo import MongoClient
from config import Config
import json
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def get_data():
    client = MongoClient(Config.MONGO_URI)
    db = client[Config.MONGO_DB]
    collection = db[Config.MONGO_COLLECTION]
  #  data = list(collection.find({}, {"_id": 0}))
    data = list(collection.find({}))

    d = json.dumps(data) # json dump string
    d = d.replace("NaN", "null")
    data = json.loads(d) # json load stri

    client.close()
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)