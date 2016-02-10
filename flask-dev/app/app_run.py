import flask
from math import pi, sin, cos
import numpy as np
from bokeh.embed import components
from bokeh.plotting import figure
from bokeh.resources import INLINE
from bokeh.templates import JS_RESOURCES, CSS_RESOURCES
from bokeh.util.string import encode_utf8
#from app import app
from cassandra.cluster import Cluster
import re

 # setting up connections to cassandra
cluster = Cluster(['54.35.154.160', '52.89.50.90', '52.89.58.183', '52.89.60.119'])
session = cluster.connect('test2')

app = flask.Flask(__name__)

js_resources = JS_RESOURCES.render(
    js_raw=INLINE.js_raw,
    js_files=INLINE.js_files
)

css_resources = CSS_RESOURCES.render(
    css_raw=INLINE.css_raw,
    css_files=INLINE.css_files
)

plot_scripts = ["", "", ""]

@app.route('/api/<author>')
def get_author_publications(author):
	 stmt = "SELECT * FROM explode_author3 WHERE author=%s"
	 response = session.execute(stmt, parameters=[author])
	 response_list = []
	 for val in response:
		  response_list.append(val)
	 jsonresponse = [{"name": x.author, "publication": x.cited_id, "creation_time": x.creation_date, "num_cited": x.num_cited} for x in response_list]
	# print jsonresponse
	 return jsonify(author=jsonresponse)

# @app.route("/author", methods=['POST'])
# def email_post():
    # author = request.form["author"]
    # stmt = "SELECT * FROM explode_author3  WHERE author=%s"
    # response = session.execute(stmt, parameters=[author])
    # response_list = []
    # x = []
    # y = []
    # for val in response:
        # response_list.append(val)
    # jsonresponse = [{"publication": x.cited_id, "num_cited": x.num_cited, "published_time": x.creation_date} for x in response_list]
# #    for x in response_list:

@app.route("/")
def root():
    global plot_scripts
    html = flask.render_template(
        'layout.html', 
        plot_script=plot_scripts[0],
        plot_div=plot_scripts[1],
        plot_div2=plot_scripts[2],
        js_resources=js_resources,
        css_resources=css_resources,
    )
    return encode_utf8(html)

def plot_data(author):
    global plot_scripts
    # get citation data
    num_cited = []
#    with open("test2.csv", "r") as f:
#        f.readline()
#        while True:
#            line = f.readline()
#            if len(line) == 0:
#                break
#            items = line.split(',')
#            num_cited.append(int(items[0]))
    
    stmt = "SELECT * FROM explode_author3  WHERE author=%s"
    response = session.execute(stmt, parameters=[author])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"publication": x.cited_id, "num_cited": x.num_cited, "published_time": x.creation_date} for x in response_list]
    for x in response_list:
             response_list.append(val)
    jsonresponse = [{"author": x.author, "publication": x.cited_id, "num_cited": x.num_cited, "published_time": x.creation_date} for x in response_list]

    num_cited = []
    num_cited = [x.num_cited for x in response_list]
    
    edges = range(len(num_cited)+1)
    num_cited.sort()
    fig = figure(title="Citations", toolbar_location=None, plot_width=500, plot_height=500)
    fig.quad(top=num_cited, bottom=0, left=edges[:-1], right=edges[1:],fill_color="blue", line_color="black")
    fig.xaxis.axis_label = 'Publication'
    fig.yaxis.axis_label = 'Number of Citations'
    
    # get collaborator data
    collaborator = {}
#    with open("test1.csv", "r") as f:
#        f.readline()
#        while True:
#            line = f.readline()
#            if len(line) == 0:
#                break
#            items = line.split(',')
#            collaborator[items[1].strip()] = int(items[2])

    stmt2 = "SELECT * FROM author_collab_2  WHERE author=%s"
    response = session.execute(stmt, parameters=[author])
    response_list = []
    for val in response:
        response_list.append(val)
            
    for x in response_list:
	collaborator[x.collaborator] = x.num_collab

    n = len(collaborator)
    collaborator_x, collaborator_y = np.zeros(n), np.zeros(n)
    edges_x, edges_y = [], []
    for i in range(n):
        angle = i*2*pi/n
        collaborator_x[i] = cos(angle)
        collaborator_y[i] = sin(angle)
        edges_x.append([0, cos(angle)])
        edges_y.append([0, sin(angle)])
    fig2 = figure(title="Collaborators", toolbar_location=None, plot_width=500, plot_height=500)
    fig2.grid.grid_line_color = None
    fig2.axis.visible = None
    fig2.multi_line(xs=edges_x, ys=edges_y, color='red')
    fig2.circle(0, 0, radius=0.2, color='blue')
    fig2.circle(collaborator_x, collaborator_y, radius=0.05, color='blue')
    fig2.text(0.0, 0.0, ["Roberts Michael D."], text_align="center")
    fig2.text(1.1*collaborator_x, 1.1*collaborator_y, collaborator.keys(), text_align="center", text_baseline='middle')
    
    script, (div, div2) = components((fig, fig2), INLINE)
    plot_scripts = [script, div, div2]

    
@app.route('/evaluate', methods = ['POST'])
def evaluate():
    author = flask.request.form['author']
    plot_data(author)
    url = flask.url_for('root', _anchor='service')
#    print url, "  aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    return flask.redirect(url)

if __name__ == "__main__":
    app.run(debug=True)
