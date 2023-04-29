# -*- coding: utf-8 -*-
"""
Created on Sun Nov 27 21:43:50 2016

@author: DanLo1108
"""


from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
import sqlalchemy as sa
import os
import yaml

from urllib.request import urlopen

#Break FG and FT down into integers
def get_made(x,var):
	x_var=x[var]
	try:
		return int(x_var[:x_var.index('-')])
	except:
		return np.nan
			
def get_attempts(x,var):
	x_var=x[var]
	try:
		return int(x_var[x_var.index('-')+1:])
	except:
		return np.nan
				
				
def append_boxscores(game_id,engine):

	url='http://www.espn.com/nba/boxscore?gameId='+str(game_id)
	
	
	page = urlopen(url)
	
	
	content=page.read()
	soup=BeautifulSoup(content,'lxml')
	
	
	tables=soup.find_all('table')
	
	results_head=[re.sub('\t|\n','',el.string) for el in tables[0].find_all('td')]        
	results_head_split=np.array_split(results_head,len(results_head)/5.)

	for ind in [4,2]:
		results=[el.string for el in tables[ind].find_all('td')]

		try:
			ind_stop=min([i for i in range(len(results)) if pd.notnull(results[i]) and ('DNP-' in results[i] or 'Did not play' in results[i])])
		except:
			ind_stop=min([i for i in range(len(results)) if pd.notnull(results[i]) and '-' in str(results[i]) and results[i] != '+/-' and results[i][0] != '-' and int(results[i].split('-')[1]) >= 55])-1

		try:
			ind_team=min([i for i in range(len(results)) if pd.notnull(results[i]) and results[i] == 'TEAM'])
		except:
			ind_team=ind_stop

		player_stats_df=pd.DataFrame(np.array_split(results[:ind_stop],ind_stop/14.),
						columns=['mp','fg','fg3','ft',
								 'oreb','dreb','reb','ast','stl','blk',
								 'tov','pf','plus_minus','pts'])

		player_stats_df = player_stats_df.drop([0,6])

		for col in player_stats_df:
			try:
				player_stats_df[col]=list(map(lambda x: float(x),player_stats_df[col]))
			except:
				continue

		player_stats_df['player']=[a_tag.text for a_tag in tables[ind-1].find_all('a')][:len(player_stats_df)]
		player_stats_df['player_id']=[a_tag['href'].split('/')[-2] for a_tag in tables[ind-1].find_all('a')][:len(player_stats_df)]


		player_stats_df['position'] = [span.text.strip() for span in tables[ind-1].find_all('span', {'class': 'playerPosition'})][:len(player_stats_df)]



		player_stats_df=player_stats_df.replace('-----','0-0').replace('--',0)
		
		player_stats_df['team_abbr']=results_head_split[int(ind/2-1)][0]
		player_stats_df['game_id']=game_id
				
				
		player_stats_df['fgm']=player_stats_df.apply(lambda x: get_made(x,'fg'), axis=1)
		player_stats_df['fga']=player_stats_df.apply(lambda x: get_attempts(x,'fg'), axis=1)
		
		player_stats_df['fg3m']=player_stats_df.apply(lambda x: get_made(x,'fg3'), axis=1)
		player_stats_df['fg3a']=player_stats_df.apply(lambda x: get_attempts(x,'fg3'), axis=1)
		
		player_stats_df['ftm']=player_stats_df.apply(lambda x: get_made(x,'ft'), axis=1)
		player_stats_df['fta']=player_stats_df.apply(lambda x: get_attempts(x,'ft'), axis=1)
		
		player_stats_df['starter_flg']=[1.0]*5+[0.0]*(len(player_stats_df)-5)

		player_stats_df['dnp_reason']=None
		
		column_order=['game_id','player','player_id','position','team_abbr','starter_flg','mp',
					  'fg','fgm','fga','fg3','fg3m','fg3a','ft','ftm','fta','oreb','dreb',
					  'reb','ast','stl','blk','tov','pf','plus_minus','pts','dnp_reason']
		
		player_stats_df[column_order].to_sql('player_boxscores',
											 con=engine,
											 schema='nba',
											 index=False,
											 if_exists='append',
											 dtype={'game_id': sa.types.INTEGER(),
													'player': sa.types.VARCHAR(length=255),
													'player_id': sa.types.INTEGER(),
													'position': sa.types.CHAR(length=2),
													'team_abbr': sa.types.VARCHAR(length=255),
													'starter_flg': sa.types.BOOLEAN(),
													'mp': sa.types.INTEGER(),
													'fg': sa.types.VARCHAR(length=255),
													'fgm': sa.types.INTEGER(),
													'fga': sa.types.INTEGER(),
													'fg3': sa.types.VARCHAR(length=255),
													'fg3m': sa.types.INTEGER(),
													'fg3a': sa.types.INTEGER(),
													'ft': sa.types.VARCHAR(length=255),
													'ftm': sa.types.INTEGER(),
													'fta': sa.types.INTEGER(),
													'oreb': sa.types.INTEGER(),
													'dreb': sa.types.INTEGER(),
													'reb': sa.types.INTEGER(),
													'ast': sa.types.INTEGER(),
													'stl': sa.types.INTEGER(),
													'blk': sa.types.INTEGER(),
													'tov': sa.types.INTEGER(),
													'pf': sa.types.INTEGER(),
													'plus_minus': sa.types.INTEGER(),
													'pts': sa.types.INTEGER(),
													'dnp_reason': sa.types.VARCHAR(length=255)})    


def get_engine():

	#Yaml stored in directory above script directory (where repository was cloned)
	fp=os.path.dirname(os.path.realpath(__file__))
	yaml_fp=fp[:fp.index('NBA-Database')]
	
	if os.path.isfile(yaml_fp+'sql.yaml'):
		with open(yaml_fp+'sql.yaml', 'r') as stream:
			data_loaded = yaml.load(stream)
			
			
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
		nba.player_boxscores p on gs.game_id=p.game_id 
	left join
		nba.bad_gameids b on gs.game_id=b.game_id and b.table='player_boxscores'
	where
		p.game_id is Null
		--and b.game_id is Null
		and gs.status='Final'
		and gs.season=2023
	order by
		gs.season
	'''
	
	game_ids=pd.read_sql(game_id_query,engine)
	
	return game_ids.game_id.tolist()


import time
def update_player_boxscores(engine,game_id_list):
	cnt=0
	print('Total Games: ',len(game_id_list))
	for game_id in game_id_list:

		try:
			append_boxscores(game_id,engine)
			cnt+=1
			if np.mod(cnt,100)==0:
				print(str(round(float(cnt*100.0/len(game_id_list)),2))+'%')
			
		except:
			print(game_id)
			cnt+=1
			bad_gameid_df=pd.DataFrame({'game_id':[game_id],'table':['player_boxscores']})
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
	update_player_boxscores(engine,game_ids)
	
	
	
if __name__ == "__main__":
	main()


