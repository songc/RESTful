from flask import Flask, jsonify, request
from elasticsearch import Elasticsearch
from flask_sqlalchemy import SQLAlchemy
from heapq import nlargest
import time
import elasticsearch.helpers
import spacy


app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = "mysql://root:455@localhost:3306/flaskdb"

db = SQLAlchemy(app)

es = Elasticsearch()

nlp = spacy.load('en_vectors_web_lg')


class User(db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(64), unique=True)
    password = db.Column(db.String(64))
    email = db.Column(db.String(64), unique=True)
    sex = db.Column(db.String(8))
    address = db.Column(db.String(200))

    def __init__(self, username, password, email, sex, address):
        self.username = username
        self.password = password
        self.email = email
        self.sex = sex
        self.address = address

    def __repr__(self):
        return '<User %r>' % self.username

    def to_dict(self):
        user_dict = dict()
        user_dict['id'] = self.id
        user_dict['username'] = self.username
        user_dict['email'] = self.email
        user_dict['sex'] = self.sex
        user_dict['address'] = self.address
        return user_dict


class UserVotes(db.Model):
    __tablename__ = 'user_votes'
    user_id = db.Column(db.Integer, primary_key=True)
    service_id = db.Column(db.String(64), primary_key=True)
    created_date = db.Column(db.DateTime)

    def __index__(self, user_id, service_id, created_date):
        self.user_id = user_id
        self.service_id = service_id
        self.created_date = created_date

    def __repr__(self):
        return '<Votes %d  %r>' % (self.user_id, self.service_id)


db.create_all()


@app.route('/register', methods=['POST'])
def register():
    form = request.get_json()
    user = User(
        form.get('username'),
        form.get('password'),
        form.get('email'),
        form.get('sex'),
        form.get('address')
    )
    db.session.add(user)
    db.session.commit()
    return jsonify(user.to_dict())


@app.route('/login', methods=['POST'])
def login():
    form = request.get_json()
    user = User.query.filter_by(username=form['username'], password=form['password']).first()
    return jsonify(user.to_dict())


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/query', )
def query():
    search_word = request.args.get('q')
    size = request.args.get('size')
    num = request.args.get('from')
    body = {
        "query": {
            "multi_match": {
                "query": search_word,
                "fields": ["keywords", "clean_doc", 'topics']
            }
        }
    }
    res = es.search(index='myrestful', doc_type='rest', body=body, from_=num, size=size)
    return jsonify(res)


@app.route('/hottopic')
def hot_topic():
    size = request.args.get('size')
    body = {
        "query": {
            "function_score": {
                "script_score": {
                    "script": {
                        "inline": "(doc['votes'].value+2.0)/Math.pow((params.time-doc['creation_date'].value)/3600,2)",
                        "params": {
                            "time": time.time()
                        }
                    }
                }
            }
        }
    }
    res = es.search(index='myrestful', doc_type='rest', body=body, from_=0, size=size)
    return jsonify(res)


@app.route('/sim', methods=['GET'])
def sim():
    id = request.args.get('id')
    size = int(request.args.get('size'))
    body = {
        "query": {
            "terms": {
                "_id": [id]
            }
        },
        "_source": "topics_and_keywords"
    }
    res = es.search(index='myrestful', doc_type='rest', body=body)
    words = res['hits']['hits'][0]['_source']['topics_and_keywords']
    doc_target = nlp(' '.join(words))

    all_sim=[]
    query = {
        "query": {
            "match_all": {}
        },
        "_source": "topics_and_keywords",
        "size": 100
    }
    for hit in elasticsearch.helpers.scan(es, index='myrestful', doc_type='rest', query=query):
        service = dict()
        service['_id'] = hit['_id']
        doc_temp = nlp(' '.join(hit['_source']['topics_and_keywords']))
        service['sim'] = doc_target.similarity(doc_temp)
        all_sim.append(service)
    top = nlargest(size, all_sim, key=lambda s:s['sim'])

    body = {
        "query": {
            "terms": {
                "_id": [x['_id'] for x in top]
            }
        },
    }
    res = es.search(index='myrestful', doc_type='rest', body=body)
    return jsonify(res)


if __name__ == '__main__':
    app.run()
