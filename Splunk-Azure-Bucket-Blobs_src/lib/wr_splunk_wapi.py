#!/usr/bin/env python3
#
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
##############################################################################################################
##############################################################################################################

### Imports
import os, re, time, sys, requests, json, urllib.parse, urllib3
from collections import defaultdict

from typing import List, Tuple, Dict

### Globals ###########################################
#os.chdir(os.path.dirname(sys.argv[0]))
urllib3.disable_warnings()

splunk_forwarder_keywords = [\
'/splunkforwarder',\
'\\splunkforwarder',\
'/SplunkUniversalForwarder',\
'\\SplunkUniversalForwarder',\
]
#
splunk_keywords = [\
'/splunk/',\
'\\splunk\\',\
]
#
splunk_default_apps = [\
'alert_logevent',\
'alert_webhook',\
'appsbrowser',\
'framework',\
'gettingstarted',\
'introspection_generator_addon',\
'launcher',\
'learned',\
'legacy',\
'sample_app',\
'search',\
'splunk_archiver',\
'splunk_gdi',\
'splunk_httpinput',\
'splunk_instrumentation',\
'splunk_internal_metrics',\
'splunk_metrics_workspace',\
'splunk_monitoring_console',\
'splunk_rapid_diag',\
'splunk_secure_gateway',\
'SplunkForwarder',\
'SplunkLightForwarder',\
'user-prefs',\
'journald_input',\
]
#
splunk_default_dashboard_apps = [\
'configuration',\
'_admin',\
'system',\
]


### Classes ###########################################
class SplunkService():
	'''
	Wrapper class to call Splunk Web REST API easily and return Splunk information. 
	Create a variable once to hold this object, specify in order host, port, un, pw for splunk)
	A Splunk service needs to be created to login and be specified here:

	e.g. 
		from lib import wr_splunk_wapi as wapi
		splunk_service = wapi.SplunkService('https://127.0.0.1', 8089, 'admin', 'MySecurePassword')

	From there you can call the various functions in here, e.g. 
		app_name_list = splunk_service.getAppNames()


	Originally wrote this wrapper for the Python Splunk SDK but found it wasn't as up to date
	as the web exposed APIs and had to revert to webcalls *sad face*. See wr_splunkapi.py for SDK version

	'''
	# this is the startup script, init?
	def __init__(self, sp_host: str, sp_port: int, sp_uname: str, sp_pword: str):
		self.sp_host = sp_host
		self.sp_port = sp_port
		self.sp_uname = sp_uname
		self.sp_pword = sp_pword

	# a "naive" but readable-friendly code way of checking if a string is in a list, exact or contains options
	def isInList(self, string_to_test:str, list_to_check_against:list, equals_or_contains=True) -> bool:
		for item in list_to_check_against:
			if equals_or_contains:
				if string_to_test == item:
					return(True)
				else:
					continue
			else:
				if string_to_test in item:
					return(True)
				else:
					continue	
	
	# checks a string (should be a Splunk app folder name) to see if its a Splunk default app
	def inDefaultSplunkApps(self, string_to_test:str) -> bool:
		return(self.isInList(string_to_test, splunk_default_apps))

	# make WEB api calls to a splunk instance - NOT using the Splunk SDK for this
	def apiCall(self, api_call: str, output_mode = 'json', verify = False, method='get', add_data={}, count=20000) -> 'requests.models.Response':
		'''
		Functions in this class use this automatically, but you CAN call this directly from your main script by
			using the splunk service class object. e.g:	splunk_service.apiCall('/services/data/inputs/all/')
		'''
		target = (self.sp_host) + ':' + str(self.sp_port) + (api_call) 
		if '?' in target:
			count_str = '&?count=' + str(count)
		else:
			count_str = '?count=' + str(count)
		target = target + count_str
		if method == 'get':
			data = {'output_mode': (output_mode)}
			data.update(add_data)
			response = requests.get( (target), data=(data), auth=(self.sp_uname, self.sp_pword), verify=(verify))
		elif method == 'post':
			data = (add_data)
			response = requests.post( (target), data=(data), auth=(self.sp_uname, self.sp_pword), verify=(verify))
		elif method == 'delete':
			data = (add_data)
			response = requests.delete( (target), data=(data), auth=(self.sp_uname, self.sp_pword), verify=(verify))
		else:
			response = "- WAPI: API request unknown, please use: get, post or delete. -"
		return(response)

#### CALLS - APPS ###
	def getAppNames(self, include_defaults=False) -> str:
		'''
		Return list of installed app names.
		Default is list since its really just a list of installed apps.
		'''
		#response = self.apiCall('/services/apps/local')
		response = self.apiCall('/servicesNS/' + self.sp_uname + '/-/apps/local')
		j_data = json.loads(response.text)
		data_list = []
		for app in j_data['entry']:
			if app['name'] not in data_list:
				if include_defaults:
					data_list.append(app['name'])
				else:
					if self.inDefaultSplunkApps(app['name']):
						continue
					else:
						data_list.append(app['name'])
		return(sorted(data_list))

	def getAppData(self, app_names=[], include_defaults=False) -> list:
		'''
		Returns a list that contains a dict for EACH installed app.
		Each dict contains all meta data for the app.

		Optionally app names specified in a python list to return just requested apps data instead of all.

		Format: dict_keys(['name', 'id', 'updated', 'links', 'acl', 'content'])
		e.g.
			app_data_list = splunk_service.getAppData()  OR specify apps splunk_service.getAppData(['wr_web_enable', 'wr_log_generator'])
			app_data_list[1].keys()
				dict_keys(['name', 'id', 'update', 'links', 'acl', 'content'])
			app_data_list[1]['name']
				'alert_webhook'
		'''
		response = self.apiCall('/services/apps/local')
		j_data = json.loads(response.text)
		if app_names:
			data_list = []
			for an in app_names:
				for app in j_data['entry']:
					if an == app['name']:
						app_dict = {}
						if app['name'] not in data_list:
							app_dict = {'name' : app['name'], 'id' : app['id'], 'updated' : app['updated'], 'links' : app['links'], 'acl' : app['acl'], 'content' : app['content'] }
							data_list.append(app_dict)
					else:
						continue
			return(sorted(data_list))
		else:
			data_list = []
			for app in j_data['entry']:
				app_dict = {}
				if app['name'] not in data_list:
					if not include_defaults:
						if self.inDefaultSplunkApps(app['name']):
							continue
					app_dict = {'name' : app['name'], 'id' : app['id'], 'updated' : app['updated'], 'links' : app['links'], 'acl' : app['acl'], 'content' : app['content'] }
					data_list.append(app_dict)
			return(sorted(data_list))

	def getAppVersions(self, include_defaults=False):
		'''
		Returns a simple dict of app names string(keys) and versions float(values).
		0 version means no version specified in metadata for app.
		{'scma': '1.0.0', 'Splunk-RemoteStore-Base-GCS': 0, 'Splunk-WR-InputGen': '1.0.0', 'Splunk-WR-SSLChecker': 0, 'TA-lm-custom-alert-actions': '1.0.0', 'wr_conf_logger_lin': 0, 'wr_log_generator': 0, 'wr_web_enable': 0}
		'''
		app_state_list = self.getAppData(include_defaults)
		app_versions_dict = {}
		for app_obj in app_state_list:
			try:
				app_versions_dict[app_obj['name']] = app_obj['content']['version']
			except:
				app_versions_dict[app_obj['name']] = 0
		return(app_versions_dict)

	def getAppCount(self, include_defaults=False) -> int:
		'''
		Return a count of apps on this system
		'''
		response = self.apiCall('/services/apps/local')
		j_data = json.loads(response.text)
		if include_defaults:
			return(int(len(j_data['entry'])))
		else:
			tmp_list = j_data['entry']
			final_list = []
			for app in tmp_list:
				if not self.inDefaultSplunkApps(app['name']):
					final_list.append(app)
			return(int(len(final_list)))

#### CALLS - CONFS ####
	def getConfNamesByApp(self, app_name) -> dict:
		'''
		Get a single apps conf file names
		Return dict with app_name and conf filenames as a list
		{'app_name': 'wr_web_enable', 'confs': ['app', 'web']}
		'''
		response = self.apiCall('/servicesNS/-/' + urllib.parse.quote((app_name), safe='') + '/properties')
		j_data = json.loads(response.text)
		data_dict = { 'app_name': (app_name), 'confs': [] }
		data_list = []
		for conf in j_data['entry']:
			data_dict['confs'].append(conf['name'])
		return(data_dict)

	def getAppsConfNames(self, app_names=[], include_defaults=False) -> list:
		'''
		Get multiple apps file names
		Return list of dicts where each dict has an app_name and a list of conf filenames
		Optionally narrow the search to specific app_names in a list
		'''
		app_name_list = self.getAppNames(include_defaults)
		if app_names:
			tmp_list = []
			for app_name in app_name_list:
				if self.isInList(app_name, app_names):
					tmp_list.append(app_name)
			app_name_list = tmp_list
		data_dict_list = []
		for app_name in app_name_list:
			response = self.apiCall('/servicesNS/-/' + urllib.parse.quote((app_name), safe='') + '/properties')
			j_data = json.loads(response.text)
			tmp_dict = { 'app_name': (app_name), 'confs': [] }
			data_list = []
			for conf in j_data['entry']:
				tmp_dict['confs'].append(conf['name'])
			data_dict_list.append(tmp_dict)
		return(data_dict_list)

	def getAppTotalConfCounts(self, app_names=[], include_defaults=False) -> dict:
		'''
		Return dict of kv pairs, app_name:conf_count
		'''
		apps_confs_list = self.getAppsConfNames(app_names, include_defaults)
		data_dict = {}
		for item in apps_confs_list:
			data_dict[item['app_name']] = len(item['confs'])
		return(data_dict)

	def getTotalConfCount(self, include_defaults=False) -> dict:
		'''
		Return int toalling all conf files in all apps
		'''
		app_conf_counts_dict = self.getAppTotalConfCounts(include_defaults)
		return sum(app_conf_counts_dict.values())

	def getConfCountByName(self, conf_name:str, include_defaults=False) -> int:
		'''
		conf_name should be name of the conf file WITHOUT the extension, e.g.  "inputs"
		Return an int totalling the number of confs of the specified name found
		{'inputs': '4'}
		'''
		apps_confs_list = self.getAppsConfNames(include_defaults)
		conf_count = 0
		for item in apps_confs_list:
			for conf in item['confs']:
				if conf_name in conf:
					conf_count = (conf_count) + 1
		return(conf_count)

	def getUniqueConfNames(self, include_defaults=False) -> list:
		'''
		Return a list of all unique conf file types found on this instance
		'''
		apps_confs_list = self.getAppsConfNames(include_defaults)
		unique_conf_list = []
		for item in apps_confs_list:
			for conf in item['confs']:
				if not self.isInList(conf, unique_conf_list):
					unique_conf_list.append(conf)
		return(sorted(unique_conf_list))

	def getConfCountsByName(self, conf_list=[], include_defaults=False) -> dict:
		'''
		Returns a dict containing a kv pair where the confname is paired with the amount of confs found for that type
		Default pulls all confs, optionally you may specify a specific list of conf names instead to narrow
		'''
		apps_confs_list = self.getAppsConfNames(include_defaults)
		full_unique_conf_list = self.getUniqueConfNames()
		unique_conf_list = []
		if conf_list:
			for uc in full_unique_conf_list:
				if self.isInList(uc, conf_list):
					unique_conf_list.append(uc)
		else:
			unique_conf_list = full_unique_conf_list
		data_dict = {}
		for conf_name in unique_conf_list:
			conf_count = 0
			for item in apps_confs_list:
				for conf in item['confs']:
					if conf_name in conf:
						conf_count = (conf_count) + 1
			data_dict[conf_name] = (conf_count)
		return(data_dict)

	def getConfStanzasByAppNameByConfName(self, app_name, conf_name) -> dict:
		'''
		Returns a dict containing {'app_name': 'wr_web_enable', 'conf_name': 'web', 'stanzas': ['settings']}
		'''
		response = self.apiCall('/servicesNS/' + self.sp_uname + '/' + urllib.parse.quote((app_name)) + '/configs/conf-' + urllib.parse.quote((conf_name)) + '/?search=eai:acl.app=wr_test_update_1')
		j_data = json.loads(response.text)
		data_dict = {}
		data_list = []
		for stanza in j_data['entry']:
			data_list.append(stanza['name'])
		data_dict = {'app_name' : (app_name), 'conf_name' : (conf_name), 'stanzas': (data_list)}
		return(data_dict)

	def getConfStanzaPropertiesByAppNameByConfName(self, app_name, conf_name, format_list=False) -> dict:
		'''
		Returns a dict containing app, conf and all stanzas properties in a list of dicts for each stanza with all its keys and values
		e.g. {'app_name': 'wr_web_enable', 'conf_name': 'web', 'stanza_list': [ {'stanza' : 'settings', 'properties' : [ {'enableSplunkWebSSL' : 'true'}, {'startwebserver' : 'true'} ] ] }
		format_list=False by default if True will give a list of lines that can be printed/written to file as they'd look in a real Splunk config rather than a python dict like above
		'''
		conf_stanzas_by_app = self.getConfStanzasByAppNameByConfName((app_name), (conf_name))
		stanza_dict_property_list = []
		for stanza in conf_stanzas_by_app['stanzas']:
			tmp_stanza_props_dict_list = []
			response = self.apiCall('/servicesNS/' + self.sp_uname + '/' + urllib.parse.quote((app_name), safe='') + '/properties/' + urllib.parse.quote((conf_name), safe='') + '/' + urllib.parse.quote((stanza), safe='') )
			#response = self.apiCall('/servicesNS/' + self.sp_uname + '/' + urllib.parse.quote((app_name)) + '/configs/conf-' + urllib.parse.quote((conf_name)) + '/?search=eai:acl.app=wr_test_update_1')
			j_data = json.loads(response.text)
			for prop in j_data['entry']:
				tmp_prop_dict = {(prop['name']) : (prop['content'])}
				tmp_stanza_props_dict_list.append(tmp_prop_dict)
			tmp_stanza_dict = {'stanza_name' : (stanza), 'property_list': (tmp_stanza_props_dict_list)}
			stanza_dict_property_list.append(tmp_stanza_dict)
		final_conf_dict = {'app_name' : (app_name), 'conf_name' : (conf_name), 'stanza_list' : (stanza_dict_property_list)}
		if not format_list:
			return(final_conf_dict)
		else:
			conf_stanzas_lines = []
			for stanza in conf_stanza_properties_by_app['stanza_list']:
				conf_stanzas_lines.append(stanza['stanza_name'])
				for props in stanza['property_list']:
					for property, value in props.items():
						conf_stanzas_lines.append( (property) + " = " + (value) )
				conf_stanzas_lines.append("\n")
			return(conf_stanzas_lines)

	def getAllConfStanzasByAppNames(self, app_names=[], include_defaults=False) -> dict:
		'''
		Returns a list of dicts, each containing {'app_name': 'wr_web_enable', 'conf_list' : [{'conf_name': 'web', 'stanzas': ['settings'] }] }
		'''
		data_list = []
		apps_confs_list = self.getAppsConfNames(app_names, include_defaults)
		for item in apps_confs_list:
			tmp_conf_list = []
			for conf_name in item['confs']:
				stanza_name_list = []
				conf_stanzas_by_app = self.getConfStanzasByAppNameByConfName((item['app_name']), (conf_name))
				for stanza in conf_stanzas_by_app['stanzas']:
					stanza_name_list.append(stanza)
				tmp_conf_dict = {'conf_name' : (conf_name), 'stanzas' : (stanza_name_list)}
				tmp_conf_list.append(tmp_conf_dict)
			tmp_app_dict = {'app_name' : item['app_name'], 'conf_list' : (tmp_conf_list)}
			data_list.append(tmp_app_dict)
		return(data_list)

	def getAllConfStanzasPropertiesByAppNames(self, app_names=[], include_defaults=False) -> dict:
		'''
		Returns a list of dicts, each containing {'app_name' : 'wr_web_enable', 'conf_list': [{'conf_name' : 'web', 'stanza_list': [{'stanza' : 'settings', 'property_list' : [{'enableSplunkWebSSL' : 'true'}, {'startwebserver' : 'true'}] }] }] }
		'''
		data_list = []
		apps_confs_list = self.getAppsConfNames(app_names, include_defaults)
		for item in apps_confs_list:
			tmp_conf_list = []
			for conf_name in item['confs']:
				conf_stanza_properties_by_app = self.getConfStanzaPropertiesByAppNameByConfName(item['app_name'], (conf_name))
				tmp_stanza_dict_list = conf_stanza_properties_by_app['stanza_list']
				tmp_conf_dict = {'conf_name' : (conf_name), 'stanza_list' : (tmp_stanza_dict_list)}
				tmp_conf_list.append(tmp_conf_dict)
			tmp_app_dict = {'app_name' : item['app_name'], 'conf_list' : (tmp_conf_list)}
			data_list.append(tmp_app_dict)
		return(data_list)

#### CALLS - Dashboards ####
	def getAllDashboards(self, names_only=False, count_only=False, include_defaults=False) -> list:
		'''
		Returns a list of dicts, each containing a dashboard and it's info. 
		Use names_only=True to return just a list of dashboard names
		Use count_only=True to return just a count of all dashboards (names_only must be default / false for this to work)
		'''
		response = self.apiCall('/servicesNS/' + self.sp_uname + '/-/data/ui/views')
		j_data = json.loads(response.text)
		tmp_main_list = []
		tmp_final_list = []
		for d in j_data['entry']:
			tmp_main_list.append(d)
		if not include_defaults:
			for item in tmp_main_list:
				if item['content']['eai:appName']:
					app_name = item['content']['eai:appName']
					if self.inDefaultSplunkApps(app_name) or self.isInList(app_name, splunk_default_dashboard_apps, True):
						continue
					else:
						tmp_final_list.append(item)
		else:
			tmp_final_list = tmp_main_list			
		if names_only:
			name_list = []
			for name in tmp_final_list:
				name_list.append(name['name'])
			return(name_list)
		elif count_only:
			return(len(tmp_final_list))
		else:
			final_dict_list = []
			for dashboard in tmp_final_list:
				if dashboard['name']:
					dashboard_name = dashboard['name']
				else:
					dashboard_name = "unknown"
				if dashboard['content']['eai:appName']:
					app_name = dashboard['content']['eai:appName']
				else:
					app_name = "unknown"
				if dashboard['author']:
					dashboard_author = dashboard['author']
				else:
					dashboard_author = "unknown"
				if dashboard['acl']:
					dashboard_acl = dashboard['acl']
				else:
					dashboard_acl = "unknown"
				if dashboard['content']:
					dashboard_content = dashboard['content']
				else:
					dashboard_content = "unknown"
				tmp_dict = { 'name' :  (dashboard_name), 'app' : (app_name), 'author' : (dashboard_author), 'acl' : (dashboard_acl), 'content' : (dashboard_content) }
				final_dict_list.append(tmp_dict)
			return(final_dict_list)

#### MOD - CONFS ####
	def pushConf(self, *args) -> 'requests.models.Response':
		'''
		input args should be exactly in this order below and THEN as many param/value pairs as you want.
		app_name, conf_name, stanza_name, param_1 = value, param_2 = value
		'''
		data ={}
		app_name = args[0]
		conf_name = args[1]
		data["name"] = args[2]
		for idx, i in enumerate(args):
			if idx <= 2:
				continue
			if '=' not in i:
				continue
			data[ i.split('=')[0].replace(" ", "") ] = i.split('=')[1].replace(" ", "")
		# add new stanza
		response = self.apiCall('/servicesNS/' + self.sp_uname + '/' + (app_name) + '/configs/conf-' + (conf_name), method='post', add_data=(data))
		print(response.text)
		if ('An object with name=' + str(args[2]) +' already exists') in response.text:
			# update existing stanza
			del data['name']
			response = self.apiCall('/servicesNS/' + self.sp_uname + '/' + (app_name) + '/properties/' + (conf_name) + '/' + (args[2]), method='post', add_data=(data))
		return(response)

	def removeStanza(self, app_name:str, conf_name:str, stanza_name:str) -> 'requests.models.Response':
		'''
		Removes entire stanza in specified config (assuming its removable)
		'''
		# purge whole stanza
		response = self.apiCall('/servicesNS/' + self.sp_uname + '/' + (app_name) + '/configs/conf-' + (conf_name) + '/' + (stanza_name), method='delete')
		print(response.text)
		return(response)

#### CLUSTERING ####
	# return IDX peers
	def getIDXClusterPeers(self, guids_only=False) -> list:
		'''
		Return IDX Peer GUIDS - raw=True for full JSON return
		'''
		response = self.apiCall('/services/cluster/master/peers')
		j_data = json.loads(response.text)
		if not guids_only:
			return(j_data)
		else:
			guid_list = []
			for peer in j_data['entry']:
				guid_list.append(peer['name'])
			return(sorted(guid_list))

#https://10.7.16.97:8089/services/shcluster/captain/members
#
#/services/shcluster/status  
#
# /services/shcluster/captain/info
#
# /services/shcluster/member/info
# /services/shcluster/captain/jobs
# /services/shcluster/captain/replications
#
#/services/cluster/master/indexes
#/services/cluster/master/peers
#


#splunk_service = SplunkService('https://10.7.16.97', 8089, 'admin', '**********')
#splunk_service.apiCall('/services/data/inputs/all/')
#app_name_list = splunk_service.getAppNames()
#app_data_list = splunk_service.getAppData(['wr_web_enable'])
#app_version_list = splunk_service.getAppVersions()
#app_count = splunk_service.getAppCount()
#app1_confs = splunk_service.getConfNamesByApp('wr_web_enable')
#apps_confs_list = splunk_service.getAppsConfNames()
#app_conf_counts = splunk_service.getAppTotalConfCounts(['wr_web_enable'])
#total_conf_count = splunk_service.getTotalConfCount()
#inputs_count = splunk_service.getConfCountByName('inputs')
#unique_conf_list = splunk_service.getUniqueConfNames()
#conf_counts_by_name = splunk_service.getConfCountsByName()
#conf_counts_by_name_narrowed = splunk_service.getConfCountsByName(['alert_actions', 'web'])
#conf_stanzas_by_app = splunk_service.getConfStanzasByAppNameByConfName('TA-lm-custom-alert-actions', 'alert_actions')
#conf_stanza_properties_by_app = splunk_service.getConfStanzaPropertiesByAppNameByConfName('wr_log_generator', 'inputs')
#conf_all_stanzas_by_app = splunk_service.getAllConfStanzasByAppNames()
#conf_all_stanzas_properties_by_app = splunk_service.getAllConfStanzasPropertiesByAppNames()
#dashboards = splunk_service.getAllDashboards()

#test2 = splunk_service.pushConf('wr_test_update_1', 'props', 'cert_29', 'delete', 'disabled = true', 'description = my_test99')
#test1 = splunk_service.removeStanza('wr_test_update_1', 'props', 'cert_29')
#
#
#conf_stanzas_by_app = splunk_service.getConfStanzasByAppNameByConfName('wr_test_update_1', 'props')
#
#conf_stanza_properties_by_app = splunk_service.getConfStanzaPropertiesByAppNameByConfName('wr_test_update_1', 'props')