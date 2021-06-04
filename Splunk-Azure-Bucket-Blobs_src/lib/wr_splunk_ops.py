##############################################################################################################
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
##############################################################################################################

### Imports ###########################################
import datetime, time, sys, os

from . import wr_common as wrc
from . import wr_logging as log

### LOGGING CLASS ###########################################
log_file = log.LogFile('wrsops.log', log_folder='./logs/', remove_old_logs=True, log_level=3, log_retention_days=10)

### FUNCTIONS ###########################################

# find the cluster master
def findClusterMasterByFile(splunk_home:str) -> tuple:
	'''
	Tries to find the cluster master and returns status and cluster master URI with port if found
	Returns( True, <URI>:<PORT> ) if found
	'''
	if 'win' in sys.platform:
		apps_folder = 'etc\\apps\\'
		local_folder = 'etc\\system\\local\\'
	else:
		apps_folder = 'etc/apps/'
		local_folder = 'etc/system/local/'
	
	server_conf_locations = ( splunk_home + apps_folder, splunk_home + local_folder )
	found_server_confs = wrc.findFileByName('server.conf', search_in=(server_conf_locations))
	if found_server_confs[0]:
		for server_conf in found_server_confs[1]:
			cluster_master = wrc.findLineInFile(['master_uri', 'manager_uri'], server_conf)
			if cluster_master[0]:
				cluster_master = cluster_master[1].split('=')[1].strip()
				print("- WRSOPS(" + str(sys._getframe().f_lineno) +"): Found IDX Clustermaster: " + cluster_master + " -")
				log_file.writeLinesToFile([str(sys._getframe().f_lineno) + " Found IDX Clustermaster: " + cluster_master] )
				return(True, cluster_master)
	print("\n- WRSOPS(" + str(sys._getframe().f_lineno) +"): Could not find Cluster Master -\n")
	log_file.writeLinesToFile([str(sys._getframe().f_lineno) + " Could not find Cluster Master"] )
	return(False, "")

# find the guid
def findGUIDByFile(splunk_home:str) -> tuple:
	'''
	Tries to find the guid of the current splunk instance
	Returns( True, <guid> ) if found
	'''
	if 'win' in sys.platform:
		etc_folder = 'etc\\'
	else:
		etc_folder = 'etc/'
	
	etc_location = (splunk_home + etc_folder)
	found_cfg = wrc.findFileByName('instance.cfg', search_in=(etc_location))
	if found_cfg[0]:
		for found_file in found_cfg[1]:
			my_guid = wrc.findLineInFile(['guid'], found_file, header='[general]')
			if my_guid[0]:
				my_guid = my_guid[1].split('=')[1].strip()
				print("- WRSOPS(" + str(sys._getframe().f_lineno) +"): Found MY GUID: " + my_guid + " -\n")
				log_file.writeLinesToFile([str(sys._getframe().f_lineno) + " Found MY GUID: " + my_guid] )
				return(True, my_guid)
	print("\n- WRSOPS(" + str(sys._getframe().f_lineno) +"): Could not find my_guid -\n")
	log_file.writeLinesToFile([str(sys._getframe().f_lineno) + " Could not find my_guid"] )
	return(False, "")