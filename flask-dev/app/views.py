import re
import flask
import numpy as np
from math import pi, sin, cos
from bokeh.embed import components
from bokeh.plotting import figure
from bokeh.resources import INLINE
from bokeh.templates import JS_RESOURCES, CSS_RESOURCES
from bokeh.util.string import encode_utf8
from app import app
from cassandra.cluster import Cluster

# setting up connections to cassandra
cluster = Cluster(['54.35.154.160', '52.89.50.90', '52.89.58.183', '52.89.60.119'])
session = cluster.connect('test2')

#app = flask.Flask(__name__)

js_resources = JS_RESOURCES.render(
    js_raw=INLINE.js_raw,
    js_files=INLINE.js_files
)

css_resources = CSS_RESOURCES.render(
    css_raw=INLINE.css_raw,
    css_files=INLINE.css_files
)

@app.route('/api/<author>')
def get_author_publications(author):
	stmt = "SELECT * FROM explode_author3 WHERE author=%s"
	response = session.execute(stmt, parameters=[author])
	response_list = []
	for val in response:
		response_list.append(val)
	jsonresponse = [{"name": x.author, "publication": x.cited_id, "creation_time": x.creation_date, "num_cited": x.num_cited} for x in response_list]
	print jsonresponse
	return jsonify(author=jsonresponse)

@app.route("/")
def root():
    html = flask.render_template(
        'layout.html', 
        # plot_script=script,
        # plot_div=div,
        # plot_div2=div2,
        js_resources=js_resources,
        css_resources=css_resources,
    )
    return encode_utf8(html)

def plot_data(author):

    # get hindex
    stmt_h = "SELECT * FROM hindex_t05  WHERE author=%s"
    response_h = session.execute(stmt_h, parameters=[author])
    response_list = []
    for val in response_h:
        response_list.append(val)
    if len(response_list) > 0 :
	h_index = response_list[0].hindex
    else:
    	h_index = 0
    # get citation data

    stmt = "SELECT * FROM explode_author3  WHERE author=%s"
    response = session.execute(stmt, parameters=[author])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"publication": x.cited_id, "num_cited": x.num_cited, "published_time": x.creation_date} for x in response_list]

    num_cited = []
    num_cited = [x.num_cited for x in response_list]    
    
    n = len(num_cited)
    edges = range(n+1)
    colors = range(n)

    if n == 0: 
	m = 220
    else :
	m = n
    step = 220/m

    for i in range(len(num_cited)):
        colors[i] = '#' + hex(int(i*step)*65792+int(i*100.0/n)+155)[2:].zfill(6)
    num_cited.sort(reverse=True)
    figtitle = author.split(",")[1] + author.split(",")[0] + ": H-index = " + str(h_index)
    if h_index == 0:
	figtitle = " Could you please check the spelling?"
    fig = figure(title=figtitle, toolbar_location="below", tools="pan,wheel_zoom,box_zoom,reset,resize",
            plot_width=500, plot_height=500)
    fig.quad(top=num_cited, bottom=0, left=edges[:-1], right=edges[1:],fill_color=colors)
    fig.xaxis.axis_label = 'Publication'
    fig.yaxis.axis_label = 'Number of Citations'
    
    # get collaborator data
    collaborator = {}

    stmt2 = "SELECT * FROM author_collab_2  WHERE author=%s"
    response2 = session.execute(stmt2, parameters=[author])
    response2_list = []
    for val in response2:
        response2_list.append(val)
    print response2_list

    stmt3 = "SELECT * FROM author_collab_2  WHERE collaborator=%s ALLOW FILTERING"
    response3 = session.execute(stmt3, parameters=[author])
    response3_list = []
    for val in response3:
        response3_list.append(val)
    
    for x in response3_list:
        collaborator[x.author] = x.num_collab
          
    n = len(collaborator)
    collaborator_x, collaborator_y = np.zeros(n), np.zeros(n)
    edges_x, edges_y = [], []
    collaborator_name, colors = [], []
    if n != 0:
	    step = 220.0/n
    else:
	    step = 1

    for i, name in enumerate(sorted(collaborator, key=collaborator.get, reverse=True)):
        angle = i*pi*53/180 + 0.35 
        dist = 0.5 + i*2.0/n
        xx, yy = dist*cos(angle), dist*sin(angle)
        collaborator_x[i] = xx
        collaborator_y[i] = yy
        edges_x.append([0, xx])
        edges_y.append([0, yy])
        colors.append('#' + hex(int(i*step)*65792+int(i*100.0/n)+155)[2:].zfill(6))
        collaborator_name.append(name)
    fig2 = figure(title="Collaborators", toolbar_location="below", tools="pan,wheel_zoom,box_zoom,reset,resize",
            plot_width=500, plot_height=500)
    fig2.grid.grid_line_color = None
    fig2.axis.visible = None
    fig2.multi_line(xs=edges_x, ys=edges_y, color=colors)
    fig2.circle(0, 0, radius=0.2, color='#000090')
    fig2.circle(collaborator_x, collaborator_y, radius=0.05, color=colors)
    fig2.text(0.0, 0.0, [author], text_align="center", text_color='red', text_font_style='bold')
    fig2.text(1.1*collaborator_x, 1.1*collaborator_y, collaborator_name, text_align="center", text_baseline='middle', text_color=colors)
    
    script, (div, div2) = components((fig, fig2), INLINE)
    return flask.jsonify(plotdiv=script+div, plotdiv2=div2)
    
@app.route('/evaluate')
def evaluate():
    author = flask.request.args.get('author', "")
    return plot_data(author)
