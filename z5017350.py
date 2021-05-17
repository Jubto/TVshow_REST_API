from flask import Flask, request, send_file, jsonify
from flask_restx import Resource, Api, reqparse, fields 
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np
import sqlite3
import pandas as pd
import requests
import os
import re

PORT = 5000 # Deafult for REST
app = Flask(__name__)
api = Api(app,
          default="TVshow",  
          title="TVshow database",  
          description="Use this service to store your favorite TV shows and retrevie useful statistics on them!") 
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///z5017350.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
db = SQLAlchemy(app)
base = f'http://127.0.0.1:{PORT}/tv-shows/'

# ================================== database models ==================================
class TVshow_table(db.Model):
	id = db.Column(db.Integer, primary_key=True)
	tvmaze_id = db.Column(db.Integer, nullable=False)
	name = db.Column(db.String, nullable=False)
	last_updated = db.Column(db.DateTime, nullable=False)
	Type = db.Column(db.String, nullable=True)
	language = db.Column(db.String, nullable=True)
	genres = db.Column(db.JSON, nullable=True)
	status = db.Column(db.String, nullable=True)
	runtime = db.Column(db.Integer, nullable=True)
	premiered = db.Column(db.Date, nullable=True)
	officialSite = db.Column(db.String, nullable=True)
	schedule = db.Column(db.JSON, nullable=True)
	rating = db.Column(db.JSON, nullable=True)
	weight = db.Column(db.Integer, nullable=True)
	network = db.Column(db.JSON , nullable=True)
	summary = db.Column(db.String, nullable=True)

	def __repr__(self):
		return f"TVmaze_shows(name = {self.name}, id = {self.id}, tvmaze_id = {self.tvmaze_id})"


if not os.path.exists('z5017350.db'):
	db.create_all() # Initalise database only if it doesn't exist
else:
	pass

# ================================== api model ==================================

schedule = api.model('schedule', {
	'time': fields.DateTime(example="12:30", description="HH:MM"),
	'days': fields.List(fields.String(example="Monday"), title="weekday")
})


rating = api.model('rating', {
    'average': fields.Float(example=8.2)
})


country = api.model('country', {
    'name': fields.String(example="Australia"),
    'code': fields.String(example="AUS"),
    'timezone': fields.String(example="Australia/Sydney") 
})


network = api.model('network', {
    'id': fields.Integer(example=2),
    'name': fields.String(example="ABC"),
    'country': fields.Nested(country) 
})

# The following is the schema of Book
TVshow_model = api.model('TVshow_table', {
    'tvmaze_id': fields.Integer(example=32345),
    'name': fields.String(example="TV show name"),
    'Type': fields.String(example="scripted"),
    'language': fields.String(example="English"),
    'genres': fields.List(fields.String(example="Thriller"), title="Genres"),
    'status': fields.String(example="Running"),
    'runtime': fields.Integer(example=80),
    'premiered': fields.Date(example="2017-02-26"),
    'officialSite': fields.Url(example="https://www.nbc.com/show-name"),
	'schedule': fields.Nested(schedule),
    'rating': fields.Nested(rating),
    'weight': fields.Integer(example=100),
    'network': fields.Nested(network),
    'summary': fields.String(example="Summary of the show"),
})


# ================================== Helper functions ==================================

def find_row(self_id, final_id, direction):
	'''Given an ID, returns '''

	check_id = self_id
	while True:
		check_id = check_id + direction # direction can be 1 or -1 
		if check_id < 0:
			return None # This means there's no previous
		elif check_id > final_id:
			return None # This means there's no next
		if TVshow_table.query.filter_by(id=check_id).first():
			return check_id # This means id was in db, the first() method returns None if not in db

def generate_href(self_id):
	'''Generates hrefs'''

	final_id = TVshow_table.query.all()[-1].id  # Get final ID
	neighbours = {'previous': -1, 'next': 1} # As of python 3.6, dictionary objects retain their order
	links = {'self' : {'href' : base + str(self_id)}}

	for neighbour in neighbours:
		neighbours[neighbour] = find_row(self_id, final_id, direction=neighbours[neighbour])
		if neighbours[neighbour]:
			links[neighbour] = {'href' : base + str(neighbours[neighbour])}

	return links

def generate_response(model, update=None):
	'''Generates response json'''

	if update:
		last_update = datetime.today().replace(microsecond=0)
	else:
		last_update = model.last_updated

	return {'id': model.id,
			'last-update': str(last_update),
			'tvmaze-id': model.tvmaze_id,
			'_links': {'self' : {'href' : base + str(model.id)}}} # Response 


def parse_order_by_param(value):
	attributes = []
	for attribute in value.split(','): # If there's no ',' then it will return the whole string as 1 element of list
		attribute = attribute.strip() # remove any leading or trailing white spaces
		if re.search('^[+-].+', attribute): # each arg must start with + or -
			if attribute[1:] in order_by_attributes:
				if attribute[1:] in attributes:
					raise Exception(f"Duplicate entry of '{attribute}'.")
				attributes.append(attribute)
			else:
				raise Exception(f"Attribute '{attribute}' is not supported for this parameter.")
		else:
			raise Exception(f"Argument '{attribute}' has to start with + or -.")
	return attributes


def parse_filter_by_param(value):
	attributes = []
	for attribute in value.split(','):
		attribute = attribute.strip() # remove any leading or trailing white spaces
		if attribute in filter_by_attributes:
			if attribute in attributes:
				raise Exception(f"Duplicate entry of '{attribute}'.")
			attributes.append(attribute)
		else:
			raise Exception(f"Attribute '{attribute}' is not supported for this parameter.")
	return attributes


# ================================== API endpoint resources and methods ==================================

TVshow_import_args = reqparse.RequestParser()
TVshow_import_args.add_argument("name", type=str, help="Name of TV show is required", required=True)

@api.route('/tv-shows/import')
@api.param('name', 'Name of TV show')
class TVshow_import(Resource):
	@api.response(201, 'TV show added to database')
	@api.response(400, 'Invalid request')
	@api.response(404, 'TV show not found')
	@api.response(409, 'TV show already in database')
	@api.doc(description="Add a new TV show to the database")
	def post(self):
		query = TVshow_import_args.parse_args()
		show_query = query.get('name')
		if re.search('[^a-zA-Z0-9 \'\-]', show_query): 
			return {"message": f"Invalid characters used in query {show_query}, please use only alphanumeric characters" }, 400
		
		tvmaze_base = 'http://api.tvmaze.com/search/shows?q='
		response = requests.get(tvmaze_base + show_query) # Get response object from querying tvmaze
		results = response.json() # Extract only the json payload of response - list of jsons
		if results:
			exact_matches = []
			similar_matches = []
			for result in results:
				show = result['show'] # show key contains the json object with all the information
				show_name = show['name']
				show_name_without_symbols = show_name.replace('-', ' ').replace('!', '').replace(':', ' ').replace('.', '').replace('"', '')
				show_query_without_dash = show_query.replace('-', ' ') # as per assignment specifications
				tvmaze_id = show['id']
				if re.search('^' + show_name_without_symbols + '$', show_query_without_dash, re.IGNORECASE): # only allowing for identical matches		
					check_row = TVshow_table.query.filter_by(tvmaze_id=tvmaze_id).first() # Returns None if no result
					if check_row:
						return {"message" : f"The show {show_name} is already stored in this database",
								show_name : {'href' : base + str(check_row.id)}}, 409

					TVshow = TVshow_table(tvmaze_id=tvmaze_id, 
										name=show_name, 
										last_updated=datetime.today().replace(microsecond=0),
										Type=show['type'],
										language=show['language'],
										genres=show['genres'],
										status=show['status'],
										runtime=show['runtime'],
										premiered= datetime.strptime(show['premiered'], "%Y-%m-%d").date(),
										officialSite=show['officialSite'],
										schedule=show['schedule'],
										rating=show['rating'],
										weight=show['weight'],
										network=show['network'],
										summary=show['summary'],
										) 
					exact_matches.append(TVshow) # Store row objects 
				else:
					similar_matches.append(show_name) # Store similar names - will return this if no matches made 
			if exact_matches:
				responses = []
				for TVshow in exact_matches:
					db.session.add(TVshow)
					db.session.commit() # Add TVshow row object into sqlite database
					responses.append(generate_response(TVshow)) # Response 
				if len(responses) > 1:
					return {"message" : f"More than one show matched the name {show_query} exactly", "shows" : responses}, 201
				else:
					return {"message" : f"{show_query} imported", show_query : responses[0]}, 201
			elif similar_matches:
				return {"message": f"Show {show_query} not found, however similar shows were found", "similar" : similar_matches}, 404
			return {"message": f"Show {show_query} not found"}, 404
		return {"message": f"Show {show_query} not found"}, 404


order_by_attributes = ["id","name","runtime","premiered","rating-average"]
filter_by_attributes = ["tvmaze_id" ,"id" ,"last-update" ,"name" ,"type" ,"language" ,"genres" ,"status", 
						"runtime" ,"premiered" ,"officialSite" ,"schedule" ,"rating" ,"weight" ,"network" ,"summary"]

TVshows_parser = reqparse.RequestParser()
TVshows_parser.add_argument("order_by", type=parse_order_by_param, help="Use comma seperated attributes starting with + or -:", default=["+id"])
TVshows_parser.add_argument("filter", type=parse_filter_by_param, help="Use comma seperated attributes:", default=["id","name"])
TVshows_parser.add_argument("page", type=int, help="Enter positive integer only.", default=1)
TVshows_parser.add_argument("page_size", type=int, help="Enter positive integer only.", default=100)

@api.route('/tv-shows')
@api.param('order_by', 'Sort in acending order using +, decending using -, append symbol to start of attribute')
@api.param('filter', 'Filter supported attributes, comma seperated')
@api.param('page', 'The page number to display from the query')
@api.param('page_size', 'The size of each page generated from the query')
@api.doc(description="Specify parameters to return a sorted/filterd list of avaliable shows\n\n"
					"--order_by: accepts: [id,name,runtime,premiered,rating-average], usage: +runtime,-id,+name\n\n"
					"--filter:  accepts: [tvmaze_id ,id ,last-update ,name ,type ,language ,genres ,status ,runtime ,"
					"premiered ,officialSite ,schedule ,rating ,weight ,network ,summary], useage: id,name,summary\n\n"
					"--page: accepts any positive integer\n\n"
					"--page_size: accepts any positive integer")
class TVshows(Resource):
	@api.response(200, 'successful ordering and filtering of database')
	@api.response(400, 'Invalid request')
	@api.response(409, 'Database is empty')
	def get(self):
		args = TVshows_parser.parse_args()
		order_by_attributes = args['order_by']
		filter_by_attributes = args['filter']
		page_no = args['page']
		page_size = args['page_size']

		if len(TVshow_table.query.all()) == 0:
			return {"message": f"Database is empty, please add some entries before using the get method"}, 409 

		number_of_pages = TVshow_table.query.paginate(per_page=page_size, page=1).pages # paginate method automatically parses any parameter named 'page' hence, setting have to set to 1
		if number_of_pages < page_no:
			return {"message": f"Pagination error, page {page_no} was requested, but only {number_of_pages} pages exist"}, 400 

		rows = TVshow_table.query # return basequery object 

		# ordering
		for order_by_attribute in order_by_attributes:
			direction = order_by_attribute[:1]
			attribute = order_by_attribute[1:]
			if attribute == 'rating-average':
				attribute = 'rating'
			if direction == '+':
				rows = rows.order_by(getattr(TVshow_table, attribute).asc()) # order_by basequery method requires a column like object
			else:
				rows = rows.order_by(getattr(TVshow_table, attribute).desc()) 

		# pagination
		rows = rows.paginate(per_page=page_size, page=page_no) # Again, don't need to specify page=page_no since paginate method auto parses 'page' param, just being explicit

		page_of_shows = {"page":page_no, "page-size":page_size, "tv-shows":[], "_links":{}}
		for show in rows.items:
			filtered_show = {}
			for attribute in filter_by_attributes:
				if attribute == 'last-update':
					attribute = 'last_updated'
					filtered_show[attribute] = str(getattr(show, attribute)) # Convert datetime object to string
					continue
				elif attribute == 'premiered':
					filtered_show[attribute] = str(getattr(show, attribute)) # Convert datetime object to string
					continue
				elif attribute == 'rating':
					filtered_show['average ' + attribute] = getattr(show, attribute)['average']
					continue
				elif attribute == 'type':
					attribute = 'Type'
				filtered_show[attribute] = getattr(show, attribute)
			page_of_shows["tv-shows"].append(filtered_show)
		
		def make_href(sign):
			return {"href" : base[:-1] + f'?order_by={",".join(order_by_attributes)}&page={page_no + sign}&page_size={page_size}&filter={",".join(filter_by_attributes)}'}

		# hrefs
		page_of_shows["_links"]["self"] = make_href(0)
		if page_no > 1:
			page_of_shows["_links"]["previous"] = make_href(-1)
		if page_no < rows.pages:
			page_of_shows["_links"]["next"] = make_href(1)
		return page_of_shows, 200


TVshow_statistics_parser = reqparse.RequestParser()
TVshow_statistics_parser.add_argument("format", type=str, help="Accepts only: json, image", required=True)
TVshow_statistics_parser.add_argument("by", type=str, help="Accepts only: language, genres, status, type", required=True)


@api.route('/tv-shows/statistics')
@api.param('format', 'The type of format this method will return, select either "json" or "image"')
@api.param('by', 'The attribute to have its freqency in the database determined. Supported attributes are: language, genres, status, type')
@api.doc(description="Use the get operation on this resource to learn about statistics of your database\n\n"
					"--format: enter 'json' to return a json object of the statistics, or 'image' to return a graph"
					"--by: enter one of the following [language, genres, status, type], returns the percentage of shows having the specified attributes")
class TVshow_statistics(Resource):
	@api.response(200, 'successful return of statistics')
	@api.response(400, 'Invalid request')
	@api.response(409, 'Database is empty')
	def get(self):		
		args = TVshow_statistics_parser.parse_args()
		output = args['format']
		attribute = args['by']
		# error checking
		if output not in ['json', 'image']:
			return {"message": f"Output parameter accepts only 'json' or 'image'"}, 400 
		if attribute not in ['language', 'genres', 'status', 'type']:
			return {"message": f"'by' parameter accepts only [language, genres, status, type] attributes"}, 400
		if len(TVshow_table.query.all()) == 0:
			return {"message": f"Database is empty, please add some entries before using the get method"}, 409 

		# load in database
		conn = sqlite3.connect('z5017350.db')
		df_shows = pd.read_sql_query(f"SELECT * FROM {TVshow_table.__tablename__}", conn) # load all rows from table into dataframe object 
		total = df_shows.shape[0]

		past_24hr = timedelta(hours=24)
		current_time = datetime.now()
		recently_updated_shows = df_shows.last_updated\
								.apply(lambda date: True if datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f") + past_24hr >= current_time else False)\
								.value_counts()[True] # datetime objects seem to have become strings, so convert them back to datetime

		if attribute in ['language', 'status', 'type']:
			attribute = 'Type' if attribute == 'type' else attribute
			df_shows_counts = df_shows.groupby(attribute).id.count().sort_values(ascending=False)
		else:
			df_shows.genres = df_shows.genres.apply(lambda genres: pd.NA if genres == '[]' else genres[1:-1].split(','))
			df_shows.dropna(subset=['genres'], how='all', inplace=True)
			df_shows_counts = df_shows.explode(attribute).groupby(attribute).id.count().sort_values(ascending=False)
			df_shows_counts.index = df_shows_counts.index.str.replace('"', '') # remove quotations from genres index
		
		df_shows_percentage = df_shows_counts.apply(lambda count: round((count / total) * 100))

		if output == 'json':
			response_json = {"total": int(total), "total-updated": int(recently_updated_shows), "values %": {}} # Convert numpy int64 to python int objects 
			for index, row in df_shows_percentage.iteritems():
				response_json["values %"].update({index:str(row) + "%"})
			return response_json, 200
		else:
			fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(10, 7)) #, 
			width = 0.4
			x =  np.arange(len(df_shows_percentage))
			rects = ax.bar(x, df_shows_percentage, width , label=f'{attribute} (with number of occurrences)')
			line2, = plt.plot([], label=f"Total number of shows: {total}", linestyle='')
			line3, = plt.plot([], label=f"Recently updated shows: {recently_updated_shows}", linestyle='')

			ax.legend(fontsize='large', loc='best')
			ax.set_ylabel('Percentage %', fontsize='large')
			ax.set_xlabel(attribute, fontsize='large')
			ax.set_ylim([0, 100])
			ax.set_title(f'Percentage of shows in the database against {attribute}')
			ax.set_xticks(x)
			ax.set_xticklabels(df_shows_percentage.index)
			fig.autofmt_xdate(rotation=45)

			ax.bar_label(rects, labels=df_shows_counts.tolist() , padding=3)
			plt.style.use('seaborn')
			plt.tight_layout()
			image_file = f'{attribute}.png'
			plt.savefig(image_file, facecolor='w', bbox_inches='tight')
			return send_file(image_file, as_attachment=True, attachment_filename=image_file, mimetype='image/png', cache_timeout=0) # Sorry I just can't workout how to make it display on browser 

			 	
 # api.model() does not appear to be able to enforce datetime types, hence using reqparser as well 
schedule_root_parser = reqparse.RequestParser()
schedule_root_parser.add_argument('schedule', type=dict)

schedule_parser = reqparse.RequestParser() # This is required to properly parse the nested key within schedule
schedule_parser.add_argument("time", type=lambda x: datetime.strptime(x, "%H:%M"), help="Invalid parameter for time. Use HH:MM format:", location=('schedule',))

TVshow_args = reqparse.RequestParser()
TVshow_args.add_argument("premiered", type=lambda x: datetime.strptime(x, "%Y-%m-%d"), help="Invalid parameter for premiered. Use YYYY-MM-DD format:", required=False)


@api.route('/tv-shows/<int:id>')
@api.param('id', 'Database ID for TV show')
class TVshow(Resource):
	@api.response(404, 'show ID was not in database')
	@api.response(200, 'Show successfully retrieved')
	@api.doc(description="Get show details by its ID")
	def get(self, id):
		row = TVshow_table.query.filter_by(id=id).first()
		if row:
			links = generate_href(row.id)
			response = {
				'id' : row.id,
				'tvmaze-id' : row.tvmaze_id,
				'name' : row.name,
				'last_updated' : str(row.last_updated),
				'Type' : row.Type,
				'language' : row.language,
				'genres' : row.genres,
				'status' : row.status,
				'runtime' : row.runtime,
				'premiered' : str(row.premiered),
				'officialSite' : row.officialSite,
				'schedule' : row.schedule,
				'rating' : row.rating,
				'weight' : row.weight,
				'network' : row.network,
				'summary' : row.summary,
				'_links' : links
			}
			return response, 200
		return {"message" : f"ID {id} is not present in the database"}, 404


	@api.response(404, 'show ID was not in database')
	@api.response(200, 'Show successfully deleted')
	@api.doc(description="Delete specified show ID from database")
	def delete(self, id):
		if TVshow_table.query.filter_by(id=id).first():
			TVshow_table.query.filter_by(id=id).delete()
			db.session.commit() # Commit the deletetion
			return {"message" : f"The tv show with id {id} was removed from the database!", "id" : id}, 200
		return {"message" : f"ID {id} is not present in the database"}, 404


	@api.response(400, 'Invalid request')
	@api.response(404, 'show ID was not in database')
	@api.response(200, 'Modifications successfully made to show ID in database')
	@api.doc(description="Modify fields of a show stored in the database")
	@api.expect(TVshow_model, validate=True)
	def patch(self, id):
		schedule_arg = schedule_root_parser.parse_args()
		schedule_parser.parse_args(req=schedule_arg) # This manipulates what parser object looks at
		TVshow_args.parse_args()
		row = TVshow_table.query.filter_by(id=id).first()
		if row:
			patch_request = request.json
			for key in patch_request:

				# Checking whether user entered keys are valid. 
				if key not in TVshow_model.keys():
					return {"message" : f"Invalid key {key}, please refer to table model for valid keys"}, 400

				# Checking whether nested keys are valid, and if so, apply modifications to database. 
				if key in ['schedule', 'rating', 'network']:
					model_dict = getattr(row, key)
					modified_dict = model_dict.copy()
					query_dict = patch_request[key]
					for nested_key in query_dict:
						if nested_key not in model_dict.keys():
							return {"message" : f"Invalid key {nested_key}, please refer to table model for valid keys"}, 400
						if nested_key == 'country':
							inner_modified_dict = model_dict[nested_key].copy()
							for nested_nested_key in query_dict[nested_key]:
								if nested_nested_key not in model_dict[nested_key].keys():
									return {"message" : f"Invalid key {nested_nested_key}, please refer to table model for valid keys"}, 400
								inner_modified_dict[nested_nested_key] = query_dict[nested_key][nested_nested_key]
							modified_dict[nested_key] = inner_modified_dict
							continue
						modified_dict[nested_key] = query_dict[nested_key]
					setattr(row, key, modified_dict) # change row objects column attribute to become modified_dict
					continue	

				# Special case, column type is date, user can input date like string, needs to be converted to date object
				if key == 'premiered': 
					setattr(row, key, datetime.strptime(patch_request[key], "%Y-%m-%d").date()) # need to convert user inputted str to date object
					continue

				setattr(row, key, patch_request[key]) # change row objects column attribute to become patch_request[key] 

			row.last_updated = datetime.today().replace(microsecond=0) # Update when table was last updated
			db.session.commit() # Commit any changes 
			return {'message' : f"ID {id} has been patched", id : generate_response(row, update=True)}, 200
		return {"message" : f"ID {id} is not present in the database"}, 404


if __name__ == '__main__':
    app.run(port=PORT) # mainthread stops here