# -*- coding: utf-8 -*-
"""
Created on Sat Dec  3 16:45:38 2016

@author: DanLo1108
"""

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import re
import sklearn
import string as st
import matplotlib.pyplot as plt
import os
import string as st
import time

import sqlalchemy as sa
import yaml



def get_x_pos(x):
	# if x['left']*(94./100) < 47:
	# 	x_pos=x['left']*(94./100)
	# else:
	# 	x_pos=94-x['left']*(94./100)

	return x['top']


def get_y_pos(x):
		
	# if x.x_pos <= 47:
	# 	y_pos=x['top']*(50./100)
	# else:
	# 	y_pos=50-x['top']*(50./100)

	return x['left']


def get_result(x):
	if 'make' in x.text or 'made' in x.text:
		return 'made'
	elif 'miss' in x.text or 'miss' in x.text:
		return 'miss'
	else:
		return None


def get_shot_type(x):
	if 'three point' in x.text:
		return 3
	elif 'free throw' in x.text:
		return 1
	else:
		return 2
	

def get_shot_distance(x):
	if x.shot_type == 1:
		return None
	else:
		return np.sqrt((x['y_pos']-25)**2+(x['x_pos'])**2)
	
def get_shot_angle(x):
	if x.shot_type == 1:
		return None
	else:
		return np.arctan(np.abs(x['y_pos']-25)/(x['x_pos']))

# def get_shot_area(x):
# 	if x.shot_type == 1:
# 		return None
# 	elif x.shot_distance <= 4:
# 		return 'RA'
# 	elif (x.y_pos >= 17 and x.y_pos <= 33) and x.x_pos <= 19:
# 		return 'Paint'
# 	elif x.shot_type == 3 and x.x_pos <= 14:
# 		return 'Crnr3'
# 	elif x.shot_type == 3 and x.x_pos > 14:
# 		return 'AbvBrk3'
# 	else:
# 		return 'MidRng'


def get_shot_area(x):
	if x.shot_type == 1:
		return 'FT'
	elif x.shot_distance <= 4:
		return 'RA'
	elif (x.y_pos >= 17 and x.y_pos <= 33) and x.x_pos <= 15:
		return 'Paint'
	elif x.shot_type == 3 and x.x_pos <= 14:
		return 'Crnr3'
	elif x.shot_type == 3 and x.x_pos > 14:
		return 'AbvBrk3'
	else:
		return 'MidRng'
	
	
def get_shot_distance_class(x):
	if x.shot_type == 1:
		return None
	elif x.shot_distance < 3:
		return '0-3ft'
	elif x.shot_distance >= 3 and x.shot_distance < 10:
		return '3-10ft'
	elif x.shot_distance >= 10 and x.shot_distance < 16:
		return '10-16ft'
	elif x.shot_distance >= 16 and x.shot_type == 2:
		return '16-3pt'
	elif x.shot_type == 3:
		return '3pt'



def append_shot_chart(game_id,engine):
	 
	url='http://www.espn.com/nba/playbyplay?gameId='+str(game_id)

	page = urlopen(url)
	
	
	content=page.read()
	soup=BeautifulSoup(content,'lxml')
	
	scripts=soup.find_all('script')

	results=[script.contents[0] for script in scripts if len(script.contents) > 0 and '"shtChrt"' in script.contents[0]][0]
	results=results[results.index('"shtChrt"'):results.index('"meta":{"stndngs"')-1]
	results=re.sub('false','False',results)
	results=re.sub('true','True',results)
	results=re.sub('null','"Null"',results)
	
	events=eval('{'+results+"}")

	shtchrt=events['shtChrt']
	
	away_team=shtchrt['tms']['away']['abbrev']
	home_team=shtchrt['tms']['home']['abbrev']

	shot_chart=shtchrt['plays']

	sc_dict={
			'shot_id':[],
			'text':[],
			'home_away':[],
			'quarter':[],
			'shooter_id':[],
			'left':[],
			'top':[],
		}
		
	for shot in shot_chart:

		if 'id' in shot:
			sc_dict['shot_id'].append(shot['id'])
		else:
			sc_dict['shot_id'].append(None)

		if 'homeAway' in shot:
			sc_dict['home_away'].append(shot['homeAway'])
		else:
			sc_dict['home_away'].append(None)

		if 'text' in shot:
			sc_dict['text'].append(shot['text'])
		else:
			sc_dict['text'].append(None)

		if 'athlete' in shot:
			sc_dict['shooter_id'].append(shot['athlete']['id'])
		else:
			sc_dict['shooter_id'].append(None)

		if 'quarter' in shot:
			sc_dict['quarter'].append(shot['period']['number'])
		else:
			sc_dict['quarter'].append(None)

		if 'coordinate' in shot:
			if 'isFreeThrow' in shot:
				if shot['isFreeThrow'] is True:
					sc_dict['left'].append(0)
					sc_dict['top'].append(0)
				else:
					sc_dict['left'].append(shot['coordinate']['x'])
					sc_dict['top'].append(shot['coordinate']['y'])
			else:
					sc_dict['left'].append(shot['coordinate']['x'])
					sc_dict['top'].append(shot['coordinate']['y'])
		else:
			sc_dict['left'].append(None)
			sc_dict['top'].append(None)


	shot_chart_df=pd.DataFrame(sc_dict)

	shot_chart_df['game_id']=[game_id]*len(shot_chart_df)

	shot_chart_df['result']=shot_chart_df.apply(lambda x: get_result(x),axis=1)
	shot_chart_df['x_pos']=shot_chart_df.apply(lambda x: get_x_pos(x),axis=1)
	shot_chart_df['y_pos']=shot_chart_df.apply(lambda x: get_y_pos(x),axis=1)

	shot_chart_df['shot_type']=shot_chart_df.apply(lambda x: get_shot_type(x),axis=1)	
	shot_chart_df['shot_distance']=shot_chart_df.apply(lambda x: get_shot_distance(x), axis=1)
	shot_chart_df['shot_angle']=shot_chart_df.apply(lambda x: get_shot_angle(x), axis=1)
	
	shot_chart_df['shot_area']=shot_chart_df.apply(lambda x: get_shot_area(x),axis=1)
	shot_chart_df['shot_distance_class']=shot_chart_df.apply(lambda x: get_shot_distance_class(x),axis=1)
	
	column_order=['shot_id', 'game_id', 'result', 'home_away', 'quarter', 'shooter_id', 'text',
				  'left', 'top', 'x_pos', 'y_pos', 'shot_distance', 'shot_angle', 'shot_type',
				  'shot_area', 'shot_distance_class']
	
	shot_chart_df[column_order].to_sql('shot_chart',
						 con=engine,
						 schema='nba',
						 index=False,
						 if_exists='append',
						 dtype={'shot_id': sa.types.VARCHAR(length=255),
								'game_id': sa.types.INTEGER(),
								'result': sa.types.VARCHAR(length=255),
								'home_away': sa.types.CHAR(length=4),
								'quarter': sa.types.INTEGER(),
								'shooter_id': sa.types.INTEGER(),
								'text': sa.types.VARCHAR(length=255),
								'left': sa.types.FLOAT(),
								'top': sa.types.FLOAT(),
								'x_pos': sa.types.FLOAT(),
								'y_pos': sa.types.FLOAT(),
								'shot_distance': sa.types.FLOAT(),
								'shot_angle': sa.types.FLOAT(),
								'shot_type': sa.types.INTEGER(),
								'shot_area': sa.types.VARCHAR(length=255),
								'shot_distance_class': sa.types.VARCHAR(length=255)})      
	

def get_engine():

	#Yaml stored in directory above script directory (where repository was cloned)
	fp=os.path.dirname(os.path.realpath(__file__))
	yaml_fp=fp[:fp.index('NBA-Database')]

	if os.path.isfile(yaml_fp+'sql.yaml'):
		with open(yaml_fp+"sql.yaml", 'r') as stream:
			data_loaded = yaml.load(stream)
			
			#domain=data_loaded['SQL_DEV']['domain']
			user=data_loaded['BBALL_STATS']['user']
			password=data_loaded['BBALL_STATS']['password']
			endpoint=data_loaded['BBALL_STATS']['endpoint']
			port=data_loaded['BBALL_STATS']['port']
			database=data_loaded['BBALL_STATS']['database']
	
	db_string = "postgres://{0}:{1}@{2}:{3}/{4}".format(user,password,endpoint,port,database)
	engine=sa.create_engine(db_string)
	
	return engine

def get_gameids(engine):
	game_id_query='''
	select distinct
		gs.season
		,gs.game_id
	from
		nba.game_summaries gs
	left join
		nba.shot_chart p on gs.game_id=p.game_id
	left join
		nba.bad_gameids b on gs.game_id=b.game_id and b.table='shot_chart'
	where
		p.game_id is Null
		--and b.game_id is Null
		and gs.status='Final'
		and gs.season in ('2023')
	order by
		gs.season
	'''
	
	game_ids=pd.read_sql(game_id_query,engine)
	
	return game_ids.game_id.tolist()

import time
def update_shot_chart(engine,game_id_list):
	cnt=0
	print('Total Games: ',len(game_id_list))
	for game_id in game_id_list:
		try:
			append_shot_chart(game_id,engine)
			cnt+=1
			if np.mod(cnt,50)==0:
				print(str(round(float(cnt*100.0/len(game_id_list)),2))+'%')
			
		except:
			cnt+=1

			bad_gameid_df=pd.DataFrame({'game_id':[game_id],'table':['team_boxscores']})
			bad_gameid_df.to_sql('bad_gameids',
								  con=engine,
								  schema='nba',
								  index=False,
								  if_exists='append',
								  dtype={'game_id': sa.types.INTEGER(),
										 'table': sa.types.VARCHAR(length=255)})
			cnt+=1
			if np.mod(cnt,100) == 0:
				print(str(round(float(cnt*100.0/len(game_id_list)),2))+'%')
			continue

		time.sleep(1)
		
		
def main():
	engine=get_engine()
	game_ids=get_gameids(engine)
	update_shot_chart(engine,game_ids)
	
	
	
if __name__ == "__main__":
	main()








