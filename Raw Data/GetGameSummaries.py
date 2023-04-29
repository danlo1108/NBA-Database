# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 12:54:45 2016

@author: DanLo1108
"""


#Import packages

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
import string as st
import sqlalchemy as sa
import os
import yaml
from urllib.request import urlopen

#Function which takes a date string and appends game summaries
#to PostGres database
def append_game_summary(date_str,engine):
	
	#Define URL from ESPN
	url='http://www.espn.com/nba/scoreboard/_/date/'+date_str
	
	#Get URL page 
	page = urlopen(url)
	
	#Get content from URL page
	content=page.read()
	soup=BeautifulSoup(content,'lxml')
	
	#Get scripts
	scripts=soup.find_all('script')
	
	#Get results from scripts
	results=[script.contents[0] for script in scripts if len(script.contents) > 0 and 'evts":[' in script.contents[0]][0]
	#results=scripts[9].contents[0]
	results=results[results.index('evts":[')+6:results.index('hideScoreDate":true}]')+21]
	results=re.sub('false','False',results)
	results=re.sub('true','True',results)
	results=re.sub('null','"Null"',results)
	
	events=eval(results)#['evts']
	
	#Iterate through "events" i.e. games
	scoreboard_results=[]
	for event in events:
		game_id=event['id'] #Game ID
		date=date_str[4:6]+'-'+date_str[6:]+'-'+date_str[:4] #Date
		
		if int(date_str[4:6]) < 9:
			season=int(date_str[:4])
		else:
			season=int(date_str[:4])+1
		
		#Get venue/attendance
		if 'vnue' in event:
			venue=event['vnue']['fullName']
			if 'address' in event['vnue']:
				if 'state' in event['vnue']['address'] and 'city' in event['vnue']['address']:
					location=event['vnue']['address']['city']+', '+event['vnue']['address']['state']
				else:
					location=None
			else:
				location=None
			venue_id=event['vnue']['id']
		else:
			venue=None
			location=None
			venue_id=None

		attendance=None

		if 'type' in event['watchListen']['cmpttn']['lg']['season']:
			if event['watchListen']['cmpttn']['lg']['season']['type']['type']==1:
				game_type='Preseason'
			elif event['watchListen']['cmpttn']['lg']['season']['type']['type']==2:
				game_type='Regular Season'
			elif event['watchListen']['cmpttn']['lg']['season']['type']['type']==3:
				game_type='Postseason'
		else:
			game_type=None
		
		#Get long and short headlines for game
		if 'rcpDta' in event:
			if 'text' in event['rcpDta']:
				headline_short = event['rcpDta']['text']
			else:
				headline_short = None

			if 'description' in event['rcpDta']:
				headline_long = event['rcpDta']['description']
			else:
				headline_long = None
		else:
			headline_long=None
			headline_short=None
			
			
		#Get home team details (name, abbreviation, ID, score, WinFLG)
		for competitor in event['competitors']:
            
			if competitor['isHome']:
				
				home_team_id=competitor['id']
				home_team_abbr=competitor['abbrev']
				home_team=competitor['displayName']

				if 'score' in competitor:
					home_team_score=competitor['score']
				else:
					home_team_score = None

				if 'winner' in competitor:
					home_team_winner=True
				else:
					home_team_winner=False

				if 'records' in competitor:
					home_team_overall_record=competitor['records'][0]['summary']
					home_team_home_record=competitor['records'][1]['summary']
					home_team_away_record=None
				else:
					home_team_overall_record=None
					home_team_home_record=None
					home_team_away_record=None
			
			else:

				try:
					away_team_id=competitor['id']
					away_team_abbr=competitor['abbrev']
					away_team=competitor['displayName']
				except:
					away_team_id=None
					away_team_abbr=None
					away_team=None
                    

				if 'score' in competitor:
					away_team_score=competitor['score']
				else:
					away_team_score = None

				if 'winner' in competitor:
					away_team_winner=True
				else:
					away_team_winner=False

				if 'records' in competitor:
					away_team_overall_record=competitor['records'][0]['summary']
					away_team_home_record=None
					away_team_away_record=competitor['records'][1]['summary']
				else:
					away_team_overall_record=None
					away_team_home_record=None
					away_team_away_record=None

			
			
		#Get game statuses - Completion and OT
		status = event['status']['description']
		if 'detail' in event['status']:
			ot_status = event['status']['detail']
		else:
			ot_status = None
			
		series_summary = None
		
		#Append game results to list   
		scoreboard_results.append((game_id,status,ot_status,date,season,home_team,away_team,home_team_score,
								  away_team_score,location,venue,venue_id,attendance,
								  game_type,headline_long,headline_short,
								  home_team_abbr,home_team_id,
								  home_team_winner,away_team_abbr,
								  away_team_id,away_team_winner,series_summary,
								  home_team_overall_record,home_team_home_record,home_team_away_record,
								  away_team_overall_record,away_team_home_record,away_team_away_record))
	
	#Define column names
	col_names=['game_id','status','status_detail','date','season','home_team','away_team','home_team_score','away_team_score',
			  'location','venue','venue_id','attendance','game_type',
			 'headline_long','headline_short','home_team_abbr','home_team_id',
			 'home_team_winner','away_team_abbr','away_team_id',
			 'away_team_winner','playoff_series_summary',
			 'home_team_overall_record','home_team_home_record','home_team_away_record',
			 'away_team_overall_record','away_team_home_record','away_team_away_record']  
	 
	#Save all games for date to DF                           
	scoreboard_results_df=pd.DataFrame(scoreboard_results,columns=col_names)
	
	#Append dataframe results to MySQL database
	scoreboard_results_df.to_sql('game_summaries',
								 con=engine,schema='nba',
								 index=False,
								 if_exists='append',
								 dtype={'game_id': sa.types.INTEGER(),
										'status': sa.types.VARCHAR(length=255),
										'status_detail': sa.types.VARCHAR(length=255),
										'date': sa.types.Date(),
										'season': sa.types.INTEGER(),
										'home_team': sa.types.VARCHAR(length=255),
										'away_team': sa.types.VARCHAR(length=255),
										'home_team_score': sa.types.INTEGER(),
										'away_team_score': sa.types.INTEGER(),
										'location': sa.types.VARCHAR(length=255),
										'venue': sa.types.VARCHAR(length=255),
										'venue_id': sa.types.INTEGER(),
										'attendance': sa.types.INTEGER(),
										'game_type': sa.types.VARCHAR(length=255),
										'headline_long': sa.types.VARCHAR(length=255),
										'headline_short': sa.types.VARCHAR(length=255),
										'home_team_abbr': sa.types.VARCHAR(length=255),
										'home_team_id': sa.types.INTEGER(),
										'home_team_winner': sa.types.BOOLEAN(),
										'away_team_abbr': sa.types.VARCHAR(length=255),
										'away_team_id': sa.types.INTEGER(),
										'away_team_winner': sa.types.BOOLEAN(),
										'playoff_series_summary': sa.types.VARCHAR(length=255),
										'home_team_overall_record': sa.types.VARCHAR(length=255),
										'home_team_home_record': sa.types.VARCHAR(length=255),
										'home_team_away_record': sa.types.VARCHAR(length=255),
										'away_team_overall_record': sa.types.VARCHAR(length=255),
										'away_team_home_record': sa.types.VARCHAR(length=255),
										'away_team_away_record': sa.types.VARCHAR(length=255)}
								 )   
	


#Get credentials stored in sql.yaml file (saved in root directory)
def get_engine():

	#Yaml stored in directory above script directory (where repository was cloned)
	fp=os.path.dirname(os.path.realpath(__file__))
	yaml_fp=fp[:fp.index('NBA-Database')]

	if os.path.isfile(yaml_fp+'sql.yaml'):
		with open(yaml_fp+'sql.yaml', 'r') as stream:
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


#Get max dates of games that were scheduled but not completed
import datetime
from datetime import date
from datetime import timedelta

def get_dates(engine):
	date_query='''

	select 
		max(date) max_date
	from 
		nba.game_summaries
	where
		status='Final'
		and season='2023'

	'''

	#Iterate through date strings to get game summaries for each date

	#start = pd.read_sql(date_query,engine).loc[0]['min_date']
	start_date = pd.read_sql(date_query,engine).loc[0]['max_date'] + timedelta(days=1)

	end_date = datetime.date.today() - timedelta(days=1)

	dates = pd.date_range(start=start_date, end=end_date, freq='D')
	formatted_dates = dates.strftime('%Y%m%d').tolist()

	return formatted_dates


def update_game_summaries(engine,dates): 
	#Iterate through list of dates, appending each days games
	cnt=0
	bad_dates=[]
	for date_str in dates: 
		append_game_summary(date_str,engine)
		try:
			append_game_summary(date_str,engine)
			cnt+=1
			if np.mod(cnt,40) == 0:
				print(str(round(float(cnt*100.0/len(dates)),2))+'%') 
		except:
			bad_dates.append(date_str)
			cnt+=1
			if np.mod(cnt,100) == 0:
				print(str(round(float(cnt*100.0/len(dates)),2))+'%')
			continue
	
 
def drop_sched_rows(engine):
	#Drop old rows from games that were scheduled and now completed or has new metadata
	drop_old_rows_query='''

	delete from
		nba.game_summaries gs
	where 1=1
		and status = 'Scheduled'
		and date < (now() - interval '1 day')

	'''

	engine.execute(drop_old_rows_query)


	
def main():
	engine=get_engine()
	dates_list=get_dates(engine) 
	drop_sched_rows(engine)
	update_game_summaries(engine,dates_list)
	
	
	
if __name__ == "__main__":
	main() 
	
	
#Drop duplicate rows - if necessary
#unique_df=pd.read_sql('select distinct * from nba.game_summaries',engine)
#unique_df.to_sql('game_summaries',con=engine,schema='nba',index=False,if_exists='replace')

