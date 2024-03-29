# -*- coding: utf-8 -*-
"""
Created on Sat Jan 13 11:03:47 2018

@author: DanLo1108
"""

import numpy as np
import pandas as pd
import sqlalchemy as sa 
import os
import yaml


def get_engine():
	
	#Yaml stored in directory above script directory (where repository was cloned)
	fp=os.path.dirname(os.path.realpath(__file__))
	yaml_fp=fp[:fp.index('NBA-Database')]
	
	#Get credentials stored in sql.yaml file (saved in root directory)
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
	
	

def calculate_player_stats(engine):

	fp=os.path.dirname(os.path.realpath(__file__))
	player_stats_agg_query=open(fp+'/PlayerStatsAgg.sql').read()
	
	player_stats_agg=pd.read_sql(player_stats_agg_query,engine)

	engine.execute("TRUNCATE TABLE nba.player_stats_agg RESTART IDENTITY;")
	player_stats_agg.to_sql('player_stats_agg',
						 con=engine,
						 schema='nba',
						 index=False,
						 if_exists='append',
						 dtype={'player': sa.types.VARCHAR(length=255),
								'player_id': sa.types.INTEGER(),
								'team': sa.types.VARCHAR(length=255),
								'season': sa.types.INTEGER(),
								'game_type': sa.types.VARCHAR(length=255),
								'gp': sa.types.INTEGER(),
								'mp': sa.types.INTEGER(),
								'ppg': sa.types.FLOAT(),
								'fgm': sa.types.FLOAT(),
								'fga': sa.types.FLOAT(),
								'fg_pct': sa.types.FLOAT(),
								'fg3m': sa.types.FLOAT(),
								'fg3a': sa.types.FLOAT(),
								'fg3_pct': sa.types.FLOAT(),
								'ftm': sa.types.FLOAT(),
								'fta': sa.types.FLOAT(),
								'ft_pct': sa.types.FLOAT(),
								'oreb': sa.types.FLOAT(),
								'dreb': sa.types.FLOAT(),
								'reb': sa.types.FLOAT(),
								'ast': sa.types.FLOAT(),
								'stl': sa.types.FLOAT(),
								'blk': sa.types.FLOAT(),
								'tov': sa.types.FLOAT(),
								'pf': sa.types.FLOAT(),
								'raw_plus_minus': sa.types.FLOAT(),
								'pts_36': sa.types.FLOAT(),
								'fgm_36': sa.types.FLOAT(),
								'fga_36': sa.types.FLOAT(),
								'fg3m_36': sa.types.FLOAT(),
								'fg3a_36': sa.types.FLOAT(),
								'ftm_36': sa.types.FLOAT(),
								'fta_36': sa.types.FLOAT(),
								'oreb_36': sa.types.FLOAT(),
								'dreb_36': sa.types.FLOAT(),
								'reb_36': sa.types.FLOAT(),
								'ast_36': sa.types.FLOAT(),
								'stl_36': sa.types.FLOAT(),
								'blk_36': sa.types.FLOAT(),
								'tov_36': sa.types.FLOAT(),
								'pf_36': sa.types.FLOAT(),
								'efg_pct': sa.types.FLOAT(),
								'ts_pct': sa.types.FLOAT(),
								'fg3_rate': sa.types.FLOAT(),
								'ft_rate': sa.types.FLOAT(),
								'oreb_pct': sa.types.FLOAT(),
								'dreb_pct': sa.types.FLOAT(),
								'reb_pct': sa.types.FLOAT(),
								'ast_pct': sa.types.FLOAT(),
								'stl_pct': sa.types.FLOAT(),
								'blk_pct': sa.types.FLOAT(),
								'tov_pct': sa.types.FLOAT(),
								'usg_pct': sa.types.FLOAT(),
								'last_update_dts': sa.types.DateTime()})

def main():
	engine=get_engine()
	calculate_player_stats(engine)
	
	
if __name__ == "__main__":
	main()


