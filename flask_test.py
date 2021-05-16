from flask import Flask, render_template, make_response, jsonify, request
from py2neo import Graph,Node
from config import neo_uri, neo_user, neo_pwd

app = Flask(__name__)

PORT = 3200

@app.route("/")
def home():
   return "<h1 style='color:red'>This is home!</h1>"

@app.route("/qstr")
def qs():
    if request.args:
        req = request.args
        res = {}
        for key, value in req.items():
            res[key] = value
        res = make_response(jsonify(res), 200)
        return res

    res = make_response(jsonify({"error": "No Query String"}), 404)
    return res

@app.route("/neo4j_qry")
def qs1():
    if request.args:
        req = request.args
        res = {}
        for key, value in req.items():
            res[key] = value

        query=res["query"]
        uri=neo_uri
        user=neo_user
        pwd=neo_pwd

        graph = Graph(uri, auth=(user, pwd), port=7474)
        result=graph.run(query).data()

        res = make_response(result, 200)

        return res

    res = make_response(jsonify({"error": "No Query String"}), 404)
    return res


if __name__ == "__main__":
    print("Server running in port %s"%(PORT))
    app.run(host='0.0.0.0', port=PORT)