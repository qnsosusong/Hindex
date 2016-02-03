from flask import render_template, jsonify, request
from app import app
from cassandra.cluster import Cluster
import re


# setting up connections to cassandra
cluster = Cluster(['54.35.154.160', '52.89.50.90', '52.89.58.183', '52.89.60.119'])
session = cluster.connect('test2')

@app.route('/')
@app.route('/index')
def index():
   user = { 'nickname': 'Miguel' } # fake user
   array = [1,2,3,4]
   return render_template("index.html", title = 'Home', user = user, array=array)

@app.route('/api/<email>/<date>')
def get_email(email, date):
	stmt = "SELECT * FROM email WHERE id=%s and date=%s"
	response = session.execute(stmt, parameters=[email, date])
        response_list = []
        for val in response:
             response_list.append(val)
        jsonresponse = [{"first name": x.fname, "last name": x.lname, "id": x.id, "message": x.message, "time": x.time} for x in response_list] 
	print jsonresponse
        return jsonify(emails=jsonresponse)

@app.route('/api/<author>')
def get_author_publications(author):
        stmt = "SELECT * FROM explode_author3 WHERE author=%s"
        response = session.execute(stmt, parameters=[author])
        response_list = []
        for val in response:
             response_list.append(val)
        jsonresponse = [{"name": x.author, "publication": x.cited_id, "creation_time": x.creation_date, "num_cited": x.num_cited} for x in response_list]
        print jsonresponse
#	author = x.author
#	json_text = response[0].name + "=" + jsonresponse
        return jsonify(author=jsonresponse)
#	return jsonify(json_text)

@app.route('/author')
def email():
	return render_template("email.html")

@app.route("/author", methods=['POST'])
def email_post():
    author = request.form["author"]
    print author
    print type(author)
    stmt = "SELECT * FROM explode_author3  WHERE author=%s"
    print stmt 
    response = session.execute(stmt, parameters=[author])
    print author
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"author": x.author, "publication": x.cited_id, "num_cited": x.num_cited, "published_time": x.creation_date} for x in response_list]
    print jsonresponse
    return render_template("emailop.html", output=jsonresponse)

#@app.route("/rank")
#def citation_rank():
    
