# -*- coding: utf-8 -*-
"""
Created on Sat Nov 26 12:50:53 2016

@author: DanLo1108
"""


from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from urllib.request import urlopen
import os
import yaml
import string as st
import os
import sqlalchemy as sa
import re


### FUNCTIONS ###
def get_Team(x,ht,at):
	if x.homeAway == 'home':
		return ht
	elif x.homeAway == 'away':
		return at
	else:
		return None


def get_HomeScore(x,ht,at):
	return int(x.score[x.score.index('-')+1:])
		
def get_AwayScore(x,ht,at):
	return int(x.score[:x.score.index('-')-1])


def get_TimeMinutes(x):
	if ':' in x.time:
		return int(x.time[:x.time.index(':')])+int(x.time[x.time.index(':')+1:])/60.
	else:
		return float(x.time)/60.

def get_Score(x):
	return str(x.away_score)+'-'+str(x.home_score)
   

def get_Quarter(x,pbp_df):
	if 'End of the 1st Quarter' in pbp_df.play.tolist():
		q1_ind=pbp_df.play.tolist().index('End of the 1st Quarter')
	elif 'End of the 1st Quarter.' in pbp_df.play.tolist():
		q1_ind=pbp_df.play.tolist().index('End of the 1st Quarter.')
	elif 'Start of the 2nd Quarter' in pbp_df.play.tolist():
		q1_ind=pbp_df.play.tolist().index('Start of the 2nd Quarter')
	elif 'Start of the 2nd Quarter.' in pbp_df.play.tolist():
		q1_ind=pbp_df.play.tolist().index('Start of the 2nd Quarter.')
	else:
		q1_ind=-1
		
	if 'End of the 2nd Quarter' in pbp_df.play.tolist():
		q2_ind=pbp_df.play.tolist().index('End of the 2nd Quarter')
	elif 'End of the 2nd Quarter.' in pbp_df.play.tolist():
		q2_ind=pbp_df.play.tolist().index('End of the 2nd Quarter.')
	elif 'Start of the 3rd Quarter' in pbp_df.play.tolist():
		q2_ind=pbp_df.play.tolist().index('Start of the 3rd Quarter')
	elif 'Start of the 3rd Quarter.' in pbp_df.play.tolist():
		q2_ind=pbp_df.play.tolist().index('Start of the 3rd Quarter.')
	else:
		q2_ind=-1
		
	if 'End of the 3rd Quarter' in pbp_df.play.tolist():
		q3_ind=pbp_df.play.tolist().index('End of the 3rd Quarter')
	elif 'End of the 3rd Quarter.' in pbp_df.play.tolist():
		q3_ind=pbp_df.play.tolist().index('End of the 3rd Quarter.')
	elif 'Start of the 4th Quarter' in pbp_df.play.tolist():
		q3_ind=pbp_df.play.tolist().index('Start of the 4th Quarter')
	elif 'Start of the 4th Quarter.' in pbp_df.play.tolist():
		q3_ind=pbp_df.play.tolist().index('Start of the 4th Quarter.')
	else:
		q3_ind=-1
		
	if 'End of the 4th Quarter' in pbp_df.play.tolist():
		q4_ind=pbp_df.play.tolist().index('End of the 4th Quarter')
	elif 'End of the 4th Quarter.' in pbp_df.play.tolist():
		q4_ind=pbp_df.play.tolist().index('End of the 4th Quarter.')
	elif 'End of the Game' in pbp_df.play.tolist():
		q4_ind=pbp_df.play.tolist().index('End of the Game')
	elif 'Start of the 2nd Quarter.' in pbp_df.play.tolist():
		q4_ind=pbp_df.play.tolist().index('Start of the 2nd Quarter.')
	else:
		q4_ind=-1
		
	if 'End of Game' in pbp_df.play.tolist():
		ge_ind=pbp_df.play.tolist().index('End of Game')
	elif 'End of the Game' in pbp_df.play.tolist():
		ge_ind=pbp_df.play.tolist().index('End of the Game')
	else:
		ge_ind=-1
		
	if 'End of the 1st  Overtime' in pbp_df.play.tolist():
		ot1_ind=pbp_df.play.tolist().index('End of the 1st  Overtime')
	elif 'Start of the 1st Overtime' in pbp_df.play.tolist():
		ot1_ind=pbp_df.play.tolist().index('Start of the 1st Overtime')
	else:
		ot1_ind=-1
		
	if 'End of the 2nd  Overtime' in pbp_df.play.tolist():
		ot2_ind=pbp_df.play.tolist().index('End of the 2nd  Overtime')
	elif 'Start of the 2nd Overtime' in pbp_df.play.tolist():
		ot2_ind=pbp_df.play.tolist().index('Start of the 2nd Overtime')
	else:
		ot2_ind=-1
		
	if 'End of the 3rd  Overtime' in pbp_df.play.tolist():
		ot3_ind=pbp_df.play.tolist().index('End of the 3rd  Overtime')
	elif 'Start of the 3rd Overtime' in pbp_df.play.tolist():
		ot3_ind=pbp_df.play.tolist().index('Start of the 3rd Overtime')
	else:
		ot3_ind=-1
		
	if 'End of the 4th  Overtime' in pbp_df.play.tolist():
		ot4_ind=pbp_df.play.tolist().index('End of the 4th  Overtime')
	elif 'Start of the 4th Overtime' in pbp_df.play.tolist():
		ot4_ind=pbp_df.play.tolist().index('Start of the 4th Overtime')
	else:
		ot4_ind=-1
		
	if 'End of the 5th  Overtime' in pbp_df.play.tolist():
		ot5_ind=pbp_df.play.tolist().index('End of the 5th  Overtime')
	elif 'Start of the 5th Overtime' in pbp_df.play.tolist():
		ot5_ind=pbp_df.play.tolist().index('Start of the 5th Overtime')
	else:
		ot5_ind=-1
			
	if x.name <= q1_ind:
		return '1'
	elif x.name <= q2_ind:
		return '2'
	elif x.name <= q3_ind:
		return '3'
	elif x.name <= q4_ind:
		return '4'
	elif x.name <= ot1_ind:
		return 'OT1'
	elif x.name <= ot2_ind:
		return 'OT2'
	elif x.name <= ot3_ind:
		return 'OT3'
	elif x.name <= ot4_ind:
		return 'OT4'
	elif x.name <= ot5_ind:
		return 'OT5'
	elif x.name == ge_ind:
		return None
	else:
		return None        
			
			
def get_Player(x):
	play=x.play
	keywords=['shooting','misses','missed','miss ','offensive','dunks','three point','made',
		  'defensive','makes','loose',"'s","s' "," 's",'enters','20 Sec',' pass',' controls',
		  'full ','bad ','delay', 'non-unsportsmanlike','steps','step ','team technical',
		  'out ','flagrant','technical','kicked','palming','no turnover','traveling',
		  'illegal','out of','out-of','3 second','possession','hanging on rim','travelling',
		  'inbound','clear path','double','back court','backcourt','punched','disc dribble','violation','lane violation',
		  'discontinu','taunting','possession','double','technical','post ','jump ball',
		  'personal','turnover','lost ball','away from',' foul','ejected','in bound',
		  'no foul','(','alley-oop','5 sec','basket from below','swinging elbows',
		  '20 sec','hanging','official timeout','off foul','off goaltending','no violation','shot clock',
		  'team shot clock','challenge','basket','jumpball','non unsport','end of game',
		  'end of','start of','team violation','punching','goaltending','kick ball','opposite',
		  'deadball','8 second','goal tending',"coach's challenge",'instant']
	#certain statements are not player-specific
	if play.lower()[:8] == 'jumpball'\
		or 'quarter' in play.lower()\
		or 'end game' in play.lower()\
		or 'vs.' in play:
			return None
			
	else:
		start_index=0
		kw_index=100
		for kw in keywords:
			if kw in play.lower():
				#blocked shot syntax treated differently
				if kw in ["'s","s' "," 's"]:
					if 'blocks' in play.lower():
						start_index=play.index('blocks')+7
					elif 'rejects' in play.lower():
						start_index=play.index('rejects')+7
					elif 'swats' in play.lower():
						start_index=play.index('swats')+7
				else:
					start_index=0
					
				if play.lower().index(kw) < kw_index:
					kw_index=play.lower().index(kw)
				
		player=play[start_index:kw_index].strip()
		if any(x.isupper() for x in player):
			return player
		else:
			return None
			
			
		
#Classify play type based on text               
def get_PlayType(x):
	play=x.play.lower()
	if 'vs.' in play:
		return 'Jump Ball'
	if 'makes' in play and 'free throw' not in play:
		return 'Made Field Goal'
	if ('misses' in play or 'blocks' in play) and 'free throw' not in play:
		return 'Missed Field Goal'
	if 'makes' in play and 'free throw' in play:
		return 'Made Free Throw'
	if 'misses' in play and 'free throw' in play:
		return 'Missed Free Throw'
	if 'rebound' in play and 'offensive' in play:
		return 'Offensive Rebound'
	if 'rebound' in play and 'defensive' in play:
		return 'Defensive Rebound'
	if 'enters' in play:
		return 'Substitution'
	if 'timeout' in play:
		return 'Timeout'
	if 'goaltending' in play:
		return 'Goaltending'
	if 'delay of game' in play or 'lane violaton' in play or 'technical' in play:
		return 'violation'
	if ('turnover' in play and 'no turnover' not in play)\
	or 'bad pass' in play or 'steps out' in play\
	or 'travel' in play or 'lost ball' in play \
	or 'backcourt' in play or 'shot clock violation' in play \
	or 'discontinue dribble' in play or 'double dribble' in play \
	or 'out of bounds' in play or 'steps' in play \
	or 'charge' in play or ('foul' in play and 'offensive' in play):
		return 'Turnover'
	if '3 second' in play or 'illegal defense' in play:
		return '3 second/illegal defense'
	if 'foul' in play and 'offensive' not in play:
		return 'Foul'
	if 'kicked ball' in play:
		return 'Kicked Ball'
	if 'start of' in play and 'quarter' in play:
		return 'Quarter Start'
	if 'end of' in play and 'quarter' in play:
		return 'Quarter End'
	if 'end of' in play and 'game' in play:
		return 'Game End'
	if 'start of' in play and 'overtime' in play:
		return 'OT Start'
	if 'end of' in play and 'overtime' in play:
		return 'OT End'
	
	
#Get player responsible for event        
def get_Assistor(x):
	if 'assists' in x.play:
		return x.play[x.play.index('(')+1:x.play.index('assists')].strip()
		 
		 
def get_StolenBy(x):
	try:
		if 'steals' in x.play:
			return x.play[x.play.index('(')+1:x.play.index('steals')].strip()
	except:
		return None
			
def get_BlockedBy(x):
	if 'blocks' in x.play:
		return x.play[:x.play.index('blocks')].strip()
	elif 'rejects' in x.play:
		return x.play[:x.play.index('rejects')].strip()
	elif 'swats' in x.play:
		return x.play[:x.play.index('swats')].strip()
		
def get_Points(x,df):
	if 'makes' in x.play:
		if 'free throw' in x.play:
			return 1
		elif 'three point' in x.play or ('-foot' in x.play and int(x.play[x.play.index('-foot')-2:x.play.index('-foot')]) >= 25):
			return 3
		else:
			return 2
	else:
		return 0
		

def get_SubbedIn(x):
	if 'enters' in x.play:
		return x.play[:x.play.index('enters')].strip()


def get_SubbedOut(x):
	try:
		if x.play=='JR Smith enters the game for':
			return 'Kyle Korver'
		if 'enters' in x.play:
			return x.play[x.play.index('game for ')+9:]
	except:
		return None




def append_pbp(game_id,engine):
	
	url='http://www.espn.com/nba/playbyplay?gameId='+str(game_id)
	
	page = urlopen(url)
	
	content=page.read()
	soup=BeautifulSoup(content,'lxml') 

	scripts=soup.find_all('script')

	results=[script.contents[0] for script in scripts if len(script.contents) > 0 and '"pbp":{"playGrps' in script.contents[0]][0]
	results=results[results.index('"pbp":{"playGrps"'):results.index('shtChrt')-3]
	results=re.sub('false','False',results)
	results=re.sub('true','True',results)
	results=re.sub('null','"Null"',results)
	
	events=eval('{'+results+"}}")

	pbp=events['pbp']
	
	away_team=pbp['tms']['away']['abbrev']
	home_team=pbp['tms']['home']['abbrev']


	pbp_dict={
			'quarter':[],
			'play':[],
			'homeAway':[],
			'away_score':[],
			'home_score':[],
			'time':[],
		}

	for period in pbp['playGrps']:
		for play in period:

			if 'period' in play:
				pbp_dict['quarter'].append(play['period']['number'])
			else:
				pbp_dict['quarter'].append(None)

			if 'text' in play:
				pbp_dict['play'].append(play['text'])
			else:
				pbp_dict['play'].append('')

			if 'homeAway' in play:
				pbp_dict['homeAway'].append(play['homeAway'])
			else:
				pbp_dict['homeAway'].append(None)

			if 'awayScore' in play:
				pbp_dict['away_score'].append(play['awayScore'])
			else:
				pbp_dict['away_score'].append(None)

			if 'homeScore' in play:
				pbp_dict['home_score'].append(play['homeScore'])
			else:
				pbp_dict['home_score'].append(None)

			if 'clock' in play:
				pbp_dict['time'].append(play['clock']['displayValue'])
			else:
				pbp_dict['time'].append(None)

	pbp_df=pd.DataFrame(pbp_dict)
	
	pbp_df['game_id']=[game_id]*len(pbp_df)
	#PlayByPlay['Date']=[x.Date]*len(PlayByPlay)
	
	if len(pbp_df) > 0:
		
		pbp_df['team_abbr']=pbp_df.apply(lambda x: get_Team(x,home_team,away_team),axis=1)
		pbp_df['player_name']=pbp_df.apply(lambda x: get_Player(x),axis=1)
		pbp_df['play_type']=pbp_df.apply(lambda x: get_PlayType(x),axis=1)   
		pbp_df['points']=pbp_df.apply(lambda x: get_Points(x,pbp_df),axis=1)    
		pbp_df['assistor']=pbp_df.apply(lambda x: get_Assistor(x),axis=1)
		pbp_df['stolen_by']=pbp_df.apply(lambda x: get_StolenBy(x),axis=1)   
		pbp_df['blocked_by']=pbp_df.apply(lambda x: get_BlockedBy(x),axis=1) 
		pbp_df['subbed_in']=pbp_df.apply(lambda x: get_SubbedIn(x),axis=1)   
		pbp_df['subbed_out']=pbp_df.apply(lambda x: get_SubbedOut(x),axis=1) 
		pbp_df['score']=pbp_df.apply(lambda x: get_Score(x),axis=1)
	else:
		pbp_df['team_abbr']=[]
		pbp_df['team_score']=[]
		pbp_df['opp_team_score']=[]
		pbp_df['quarter']=[]
		pbp_df['player_name']=[]
		pbp_df['play_type']=[]
		pbp_df['points']=[]
		pbp_df['assistor']=[]
		pbp_df['stolen_by']=[]
		pbp_df['blocked_by']=[]
		pbp_df['subbed_in']=[]
		pbp_df['subbed_out']=[]
		pbp_df['score']=[]
		
	pbp_df['time_minutes']=pbp_df.apply(lambda x: get_TimeMinutes(x),axis=1)
	
	column_order=['game_id','play','score','time','time_minutes',
				  'team_abbr','home_score','away_score','quarter','player_name',
				  'play_type','points','assistor','stolen_by',
				  'blocked_by','subbed_in','subbed_out']
	
	pbp_df[column_order].to_sql('play_by_play',
								con=engine,
								schema='nba',
								index=False,
								if_exists='append',
								dtype={'game_id': sa.types.INTEGER(),
									   'play': sa.types.VARCHAR(length=255),
									   'score': sa.types.VARCHAR(length=255),
									   'time': sa.types.VARCHAR(length=255),
									   'time_minutes': sa.types.FLOAT(),
									   'team_abbr': sa.types.VARCHAR(length=255),
									   'home_score': sa.types.INTEGER(),
									   
									   'away_score': sa.types.INTEGER(),
									   'quarter': sa.types.VARCHAR(length=255),
									   'player_name': sa.types.VARCHAR(length=255),
									   'play_type': sa.types.VARCHAR(length=255),
									   'points': sa.types.INTEGER(),
									   
									   'assistor': sa.types.VARCHAR(length=255),
									   'stolen_by': sa.types.VARCHAR(length=255),
									   'blocked_by': sa.types.VARCHAR(length=255),
									   'subbed_in': sa.types.VARCHAR(length=255),
									   'subbed_out': sa.types.VARCHAR(length=255)})
				 

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



def get_gameids(engine):
	
	game_id_query='''
	select distinct
		gs.season
		,gs.game_id
	from
		nba.game_summaries gs
	left join
		nba.play_by_play p on gs.game_id=p.game_id
	left join
		nba.bad_gameids b on gs.game_id=b.game_id and b.table='play_by_play'
	where
		p.game_id is Null
		and b.game_id is Null
		and gs.status='Final'
		and gs.season in ('2024')
	order by
		gs.season
	'''
	
	game_ids=pd.read_sql(game_id_query,engine)
	
	return game_ids.game_id.tolist()

#2002-2009
import time
def update_play_by_play(engine,game_id_list):
	cnt=0
	print('Total Games: ',len(game_id_list))
	for game_id in game_id_list:

		try:
			append_pbp(game_id,engine)
			cnt+=1
			if np.mod(cnt,50)==0:
				print(str(round(float(cnt*100.0/len(game_id_list)),2))+'%')
			
		except:
			cnt+=1

			bad_gameid_df=pd.DataFrame({'game_id':[game_id],'table':['play_by_play']})
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
	update_play_by_play(engine,game_ids)
	
	
	
if __name__ == "__main__":
	main()       
		









