## Created by:  Chris Ward
##				chris.ward@clearlink.com
##
## Last edit:	2013-07-30
##
## Version:		1.0.2

## Updates Myna experiment rewards based on offline activity captured in a local MySQL database.

from __future__ import division

from urllib2 import Request, urlopen
import base64
import json
import string
import gc
import os
import ast
import collections
import datetime

import MySQLdb



def find_key(dic, val):
    # return the key of dictionary dic given the value
    return [k for k, v in dic.iteritems() if v == val][0]

def convert(input):
	if isinstance(input, dict):
	    return {convert(key): convert(value) for key, value in input.iteritems()}
	elif isinstance(input, list):
	    return [convert(element) for element in input]
	elif isinstance(input, unicode):
	    return input.encode('utf-8')
	else:
	    return input

def trunc(f, n):
    #Truncates/pads a float f to n decimal places without rounding
    return ('%.*f' % (n + 1, f))[:-1]


# Myna connection parameters
myna_username = "myna_username"
myna_password = "myna_password"

# DB connection parameters 
db_host = 'localhost'
db_user = 'db_user'
db_pwd = 'db_pwd'
db_db = 'offline_activity_database'


base64string = base64.encodestring('%s:%s' % (myna_username, myna_password)).replace('\n', '')


request = Request("https://api.mynaweb.com/v2/experiment")
request.add_header("Authorization", "Basic %s" % base64string)   
json_data = convert(json.load(urlopen(request)))


## Experiment dictionary layout: {'variation ID':['experiment ID'],[uuid]}

myna_experiments = {}

for i in json_data['results']:
	experiment_id = i['name'].split("#")[1].strip()
	uuid = i['uuid'].strip()

	for j in i['variants']:
		myna_experiments[j['name']] = [experiment_id, uuid]



## Pull the data from offline_activity_database and determine count of offline conversion events that should be rewarded

sql = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pwd, db=db_db)
cursor = sql.cursor()




cursor.execute(" -- Your logic will vary depending on circumstance")


offline_log = cursor.fetchall()

cursor.close()
sql.close()
gc.collect()


# Clean up the response data: keep only the relevant records
cleaned_offline_log = []

for row in offline_log:
	if row[0].count(",") < 1:
		cleaned_offline_log.append(row)



# Find the intersection of Myna and offline events
offline_experiments = {}

# This will vary based on the structure of your offline data
for row in cleaned_offline_log:
	key_val = row[0].translate(None,'"{}').split(":")
	if key_val[1] in myna_experiments:
		# [ experiment ID, variation ID, date, metric1, metric1, metric3, metric4 ]
		offline_experiments[key_val[1]] = [ key_val[1], key_val[0], row[1].strftime("%m-%d-%Y"), int(row[2]), int(row[3]), int(row[4]), int(row[5]) ]


## For each day's variations, calculate reward based on how many completed gross calls, q opps, etc. and send an API call to Myna with the reward and amount.

##	Reward values:
metric1_reward = 0.1
metric2_reward = 0.15
metric3_reward = 0.25
metric4_reward = 0.5


for oe in offline_experiments:
	uuid = myna_experiments[oe][1]
	offline_experiment_id = offline_experiments[oe][1]
	variant = oe
	
	count_events = (offline_experiments[oe][3] + offline_experiments[oe][4] + offline_experiments[oe][5] + offline_experiments[oe][6])
	
	if count_events > 0:
		u = (gc_reward * offline_experiments[oe][3]) + (ac_reward * offline_experiments[oe][4]) + (qo_reward * offline_experiments[oe][5]) + (so_reward * offline_experiments[oe][6])
		reward_amt = u/count_events
	else:
		reward_amt = 0

	if reward_amt > 1:
		reward_amt = 1


	URI = "https://api.mynaweb.com/v2/experiment/%s/record?typename=reward&variant=%s&amount=%f" % (uuid, variant, reward_amt)

	request = Request(URI)
	request.add_header("Authorization", "Basic %s" % base64string)  

	response_code = urlopen(request).getcode()
	if response_code == 200:
		print "Updated Myna experiment %s, variation %s with a reward of %f" % (optimizely_experiment_id, variant, reward_amt)
















