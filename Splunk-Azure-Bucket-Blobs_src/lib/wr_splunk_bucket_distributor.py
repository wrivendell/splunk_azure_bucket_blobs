##############################################################################################################
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
##############################################################################################################

### Imports ###########################################
import time, sys, re, threading, pandas, statistics

from pathlib import Path
from collections import OrderedDict
from . import wr_logging as log
from . import wr_splunk_ops as wrsops
from . import wr_splunk_wapi as wapi
from . import wr_thread_queue as wrq
from . import wr_common as wrc

'''
Feed this class a list of lists. Thats ONE list that contains ALL of the buckets needed for download
Each list in the main list will contain details to Splunk buckets for download

From your main script:

from lib import wr_splunk_bucket_distributor as buckets

azure_buckets = buckets.Bucketeer()
	you can feed the list in at this time or later by calling: azure_buckets.addList((list_of_bucket_list_details))

	see __init__ for further details
'''

### CLASSES ###########################################

class Bucketeer():
	# this is the startup script, init?
	def __init__(self, name: str, sp_home:str, sp_uname:str, sp_pword:str, list_of_bucket_list_details=[], sp_idx_cluster_master_uri='', port=8089, main_report_csv='', include_additioanl_list_items_in_csv=True, skip_to_csv_load=False, debug=False):
		'''
		THIS will run on ALL nodes in the indexer cluster. All nodes will process the exact same list and end up with the exact same results.
		Final list of lists will be sorted and contain one list per GUID.
		GUIDS will be sorted by GUID and they will be assigned it's corredponding index in the master list
			GUID 0 get master_list 0
			GUID 1 get master_list 1
			and so on... 

		Optionally, Provide Splunk IDX Cluster Master and API port e.g.  splunk_idx_cluster_master_uri="https://cm1.mysplunk.go_me.com" 
			Port is set to default, port=8089 
			If URI left empty, an attempt will be made to find it via the local file system.

			This is for idx CLUSTERS only. Standalone, just download direct and dont import this module.

		The master list_of_bucket_list_details -> is a list containing lists. Each list item is a single download to be downloaded.
			Each item in the list should START with this exact format:  [<full_path_to_a_bucket_file>, <file_size_bytes>]
			You may add as many additional info items to the END and they will come back sorted with the list as <list>[14]+  etc
			But don't mess with the start!!

			include_additioanl_list_items_in_csv = True - will add the additional csv items to the CSV list at the end in the same order

		You may add the bucket list later when you run START, however if you don't add any buckets, nothing will happen. 
		name is just whatever name you want to give this. 

		After you run start():
			1. you can access THIS nodes list by calling the class and getting this variable's content: self.this_peer_download_list
			2. you can access the full list by calling the class and getting this variable's content: self.final_peer_download_lists

			EXAMPLES: 
				WARM example complete bucket would have multiple list entries for on-prem - stand-alone:
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/1620169246-1620169223-7654661268343386492.tsidx', 136365]
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/bloomfilter, 6395]
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/bucket_info.csv, 67]
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/Hosts.data, 122]
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/optimize.result, 0]
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/Sources.data, 156]
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/SourceTypes.data, 109]
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/Strings.data, 300]
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/rawdata/262780, 22491]
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/rawdata/journal.gz, 57800]
					['/opt/splunk/var/lib/warm/wr_conf_logger/db/db_1620169246_1620169223_130/rawdata/slicesv2.dat, 52]
				
				FROZEN example from Azure BLOB:
					['frozendata/barracuda/frozendb/db_1621748072_1629322094_16_C27CDE8F-2593-4435-8739-B827B7975060/rawdata/journal.gz', 1.757321]
		'''
		########################################### 
		# Log and init start
		########################################### 
		self.log_file = log.LogFile('bucketeer.log', log_folder='./logs/', remove_old_logs=True, log_level=3, log_retention_days=10)
		if not sp_uname:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): No Splunk Username provided, yet Cluster is indicated. I'm not a mind reader! Cluster Master API call not possible. Exiting.")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): No Splunk Username provided, yet Cluster is indicated. I'm not a mind reader! Cluster Master API call not possible. Exiting."] )
			return(False)
		if not sp_pword:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): No Splunk Password provided, yet Cluster is indicated. I'm not a mind reader! Cluster Master API call not possible. Exiting.")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): No Splunk Password provided, yet Cluster is indicated. I'm not a mind reader! Cluster Master API call not possible. Exiting."] )
			return(False)
		self.debug = debug # enable debug printouts from THIS class
		self.name = name # unique thread name
		self.list_of_bucket_list_details = list_of_bucket_list_details # master list of buckets to be downloaded
		self.sp_home = sp_home
		self.sp_uname = sp_uname
		self.sp_pword = sp_pword
		self.sp_idx_cluster_master_uri = sp_idx_cluster_master_uri
		self.port = port
		self.include_additioanl_list_items_in_csv = include_additioanl_list_items_in_csv
		self.skip_to_csv_load = skip_to_csv_load # will skip all sorting and just load the CSV and pass it back to main

		# see if cluster master URI is available
		if not self.sp_idx_cluster_master_uri:
			self.sp_idx_cluster_master_uri = wrsops.findClusterMasterByFile(self.sp_home)
			# remove the port if found in file since its specified already
			if not self.sp_idx_cluster_master_uri[0]:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Couldn't find an IDX Cluster Master URI and non specified. No buckets will be downloaded here. Exiting.")
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Couldn't find an IDX Cluster Master URI and non specified. No buckets will be downloaded here. Exiting."] )
				return(False)
			else:
				self.sp_idx_cluster_master_uri = str(self.sp_idx_cluster_master_uri[1])
				if ':' in self.sp_idx_cluster_master_uri:
					tmp_list = self.sp_idx_cluster_master_uri.split(':')
					self.sp_idx_cluster_master_uri = ','.join(tmp_list[0:2]).replace(',', ':')
		# get my guid
		self.my_guid = wrsops.findGUIDByFile(self.sp_home)
		if not self.my_guid[0]:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Couldn't find this node's GUID. No buckets will be downloaded here. Exiting")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Couldn't find this node's GUID. No buckets will be downloaded here. Exiting"] )
			return(False)
		else:
			self.my_guid = str(self.my_guid[1])
		
		# get peer GUIDs
		self.idx_cluster_peers = self.getPeerGUIDS()

		# initialize main CSV list (csv list will be used for resume) - same name should be used for marking download complete (see sabb.py for example)
		if not main_report_csv:
			self.main_report_csv = 'bucket_sort_status_report.csv'
		else:
			self.main_report_csv = main_report_csv
		'''
		#not used now, maybe later if list creation continues to be a pain
				self.write_list_csv = log.CSVFile('list_report.csv',1, log_folder='./csv_lists/', remove_old_logs=False, log_retention_days=20, prefix_date=False, debug=self.debug) # used to resume WRITING the full list on failures or cancels
				self.write_list_queue = wrq.Queue('list_reporter', 1, debug=self.debug)
		'''
		# create csv handlers and write queues - one list per guid - used to write all lists our to csv "simultaneously" 
		'''
		This doesnt write anything or create anything, it just creates the threads and classes in memory for use and job adds later
		'''
		self.csv_list = []
		self.csv_queues = []
		csv_write_job_dicts = {} # queues write jobs per guid
		for idx, g in enumerate(self.idx_cluster_peers):
			if g == self.my_guid:
				self.this_peer_index = idx = idx
			self.csv_list.append(log.CSVFile(main_report_csv + "_" + g + ".csv", log_folder='./csv_lists/', remove_old_logs=False, log_retention_days=20, prefix_date=False, debug=self.debug)) # PEER download lists csv writer -  used to resume downloads and check already completed
			self.csv_queues.append(wrq.Queue('csv_writer' + "_" + g, 1, inactive_queue_timeout_sec=8, debug=False)) # queues csv writes to master status report
			csv_write_job_dicts[g] = []

		# check if any previous csv exist - 
		'''
		Do this once now as when we process hundreds of thousands of buckets, we only need to check a bool for each and know we can skip scanning ever CSV if they don't even exist
		'''
		self.csv_exists = False
		for guid in self.guid_list:
			csv = self.getPeerCSV(guid)
			if csv.doesLogFileExist():
				self.csv_exists = True
				break

		# start easy timer for the overall operation -
		self.bucketeer_timer = wrc.timer('bucketeer_timer', 0)
		threading.Thread(target=self.bucketeer_timer.start, name='bucketeer_timer', args=(), daemon=False).start()

		########################################### ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		# Log and init start
		########################################### 



	########################################### 
	# General Helper Functions
	########################################### 
	def getElapsedHours(self):
		return(self.bucketeer_timer.elapsed(unit='h'))

	# get peer GUIDS in this idx cluster and SORTS them (very important they are sorted)
	def getPeerGUIDS(self):
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Creating 'wapi' service called: splunk_idx_cm_service -\n")
		self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Creating 'wapi' service called: splunk_idx_cm_service."] )
		self.splunk_idx_cm_service = wapi.SplunkService( self.sp_idx_cluster_master_uri, self.port, self.sp_uname, self.sp_pword )
		self.guid_list = self.splunk_idx_cm_service.getIDXClusterPeers(guids_only=True)
		self.guid_list.sort()
		self.guid_list = tuple(self.guid_list)
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Found the following GUIDS in this order: -")
		for i in self.guid_list:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"):  -" + str(i))
		print("\n")
		return(self.guid_list)
	
	def getPeerCSV(self, guid='') -> 'log.CSVFile':
		if not guid:
			guid = self.my_guid
		for csv in self.csv_list:
			if guid in csv.name:
				return(csv)

	def getPeerCSVQ(self, guid='') -> 'wrq.Queue':
		if not guid:
			guid = self.my_guid
		for queue in self.csv_queues:
			if self.my_guid in queue.name:
				return(queue)

	########################################### ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
	# General Helper Functions
	########################################### 



	########################################### 
	# First bucket details handler - get all the details for a bucket into easy variables
	########################################### 
	# split bucket details out into a tuple with get-able variables
	def splitBucketDetails(self):
		'''
		Break master bucker list into tuples per file
		Returns:
			each tuple: [0] = earliest, [1] = latest, [2] = id, [3] = guid, [4] = standalone bool, [5] = origin or replicated bool, [6] = bucket size in bytes,
						[7] = full bucket path originally, [8] = bucket_state_path, [9] = bucket_index_path, [10] = bucket_db_path, [11] = uid
		Add tuples to new list and sort by GUID
		'''
		bucket_info_tuples_list = []
		for bucket_path in self.list_of_bucket_list_details:
			if self.csv_exists:
				# see if we have this in our lists already
				skip_this = False
				for guid in self.guid_list:
					csv = self.getPeerCSV(guid)
					# check if file is listed there at all
					if csv.valueExistsInColumn('File_Name', bucket_path[0])[0]:
						print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Already on list, skipping -")
						skip_this = True
						break
				if skip_this:
					continue

			# break out the bucket details
			if bucket_path[1] <= 0:
				if not str(bucket_path[0]).endswith(".csv") and not str(bucket_path[0]).endswith(".result") and not str(bucket_path[0]).endswith(".tsidx") and not str(bucket_path[0]).endswith(".bloomfilter") and not str(bucket_path[0]).endswith(".data") and not str(bucket_path[0]).endswith(".journal.gz") and not str(bucket_path[0]).endswith(".dat"):
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Skipping file with 0 byte size: " + str(bucket_path) + " -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Exception: Skipping file with 0 byte size: " + str(bucket_path)] )
					continue
			# get buckets ID  from bucket path tuple
			bucket_id_full = re.search('(db_.+?)((\\|\/)|$)', bucket_path[0], re.IGNORECASE)  # db_ or rb_ ?
			if not bucket_id_full:
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): GET ID: Not a DB bucket, trying RB: " + str(bucket_path)] )
				bucket_id_full = re.search('(rb_.+?)((\\|\/)|$)', bucket_path[0], re.IGNORECASE)
			if not bucket_id_full:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Can't parse bucket_id_full. You sure your feeding your list in as expected? Failed on: " + str(bucket_path) + ". Skipping- ")
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Exception: Can't parse bucket_id_full. You sure your feeding your list in as expected? Failed on: " + str(bucket_path) + ". Skipping." ])
				continue
			else:
				try:
					bucket_id_full = bucket_id_full.group(1)
				except Exception as ex:
					print(ex)
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): FAILED Rgex extract on bucket_id_full: " + bucket_id_full + " Skipping -")
					self.log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): FAILED Rgex extract on bucket_id_full: " + bucket_id_full + " - Skipping."])
					continue

			# get full bucket PATH from from bucket path tuple
			bucket_path_full = re.search('(.+)(db_)', bucket_path[0], re.IGNORECASE)
			if not bucket_path_full:
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): GET FULL PATH: Not a DB bucket, trying RB: " + str(bucket_path)] )
				bucket_path_full = re.search('(.+)(rb_)', bucket_path[0], re.IGNORECASE)
			if not bucket_path_full:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Can't parse bucket_path_full. You sure your feeding your list in as expected? Failed on: " + str(bucket_path) + ". Skipping- ")
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Exception: Can't parse bucket_path_full. You sure your feeding your list in as expected? Failed on: " + str(bucket_path) + ". Skipping." ])
				continue
			else:
				try:
					bucket_path_full = bucket_path_full.group(1)
				except Exception as ex:
					print(ex)
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): FAILED Rgex extract on bucket_path_full: " + bucket_path_full + " Skipping-")
					self.log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): FAILED Rgex extract on bucket_path_full: " + bucket_path_full + " - Skipping."])
					continue

			# get bucket_db_path PATH from from bucket path tuple
			try:
				bucket_db_path = Path(bucket_path_full).parts[-1] # frozendb, colddb, db
			except Exception as ex:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Can't parse bucket_db_path. " + str(bucket_path) + ". Skipping- ")
				print(ex)
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Exception: Can't parse bucket_db_path. Failed on: " + str(bucket_path) + ". Skipping." ])
				continue

			# get bucket_db_path PATH from from bucket path tuple
			try:
				bucket_index_path = Path(bucket_path_full).parts[-2]  # barracuda, mcafee etc
			except Exception as ex:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Can't parse bucket_index_path. " + str(bucket_path) + ". Skipping- ")
				print(ex)
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Exception: Can't parse bucket_index_path. Failed on: " + str(bucket_path) + ". Skipping." ])
				continue

			# get bucket_state_path PATH from from bucket path tuple
			try:
				bucket_state_path = Path(bucket_path_full).parts
				if len(bucket_state_path) > 3:
					bucket_state_path = Path(bucket_path_full).parts[-3] # cold, warm, hot, or if frozen, custom folder
				else:
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Can't find bucket_state_path. Tuple index out of range. Might be internal_db. Adding it: " + str(bucket_path) ])
					if '/' in bucket_path_full:
						bucket_state_path = "/"
					elif '\\' in bucket_path_full:
						bucket_state_path = "\\"
					else:
						bucket_state_path = ""
			except Exception as ex:
				if self.debug:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Can't find bucket_state_path. Tuple index out of range. Might be internal_db. Adding it: " + str(bucket_path))
				print(ex)
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Can't find bucket_state_path. Tuple index out of range. Might be internal_db. Adding it: " + str(bucket_path) ])
				if '/' in bucket_path_full:
					bucket_state_path = "/"
				elif '\\' in bucket_path_full:
					bucket_state_path = "\\"
				else:
					bucket_state_path = ""

			# get bucket_id_guid from from bucket path tuple
			try:
				bucket_id_guid = bucket_id_full.split('/')[0].split('\\')[0]
			except Exception as ex:
				if self.debug:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Can't find bucket_id_guid. Skipping." + str(bucket_id_guid))
				print(ex)
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Can't find bucket_id_guid. Skipping. " + str(bucket_id_guid) ])
				continue
			try:
				bucket_id_guid = bucket_id_guid.split('_')[4] # if ok then its a clustered bucket
				bucket_id_standalone = False
			except:
				if self.debug:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Appears to not be a clustered bucket setting as standalone: " + str(bucket_id_guid))
				bucket_id_guid = 'none'
				bucket_id_standalone = True
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Bucket is standalone and not part of a cluster: " + str(bucket_path) ])

			# get bucket_id_origin from from bucket path tuple
			try:
				if 'rb' in str(bucket_id_full.split('_')[0]):
					bucket_id_origin = False
				elif 'db' in str(bucket_id_full.split('_')[0]):
					bucket_id_origin = True
				else:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Can't determine if replicated or non bucket: " + str(bucket_path) + " Skipping -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Exception: Can't determine if replicated or non bucket: " + str(bucket_path) + " Skipping." ])
					continue
			except Exception as ex:
				print(ex)
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Can't determine if replicated or non bucket: " + str(bucket_path) + " Skipping -")
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Exception: Can't determine if replicated or non bucket: " + str(bucket_path) + " Skipping." ])
				continue

			# get bucket_id_earliest from from bucket path tuple
			try:
				bucket_id_earliest = bucket_id_full.split('_')[1]
			except Exception as ex:
				if self.debug:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Can't find bucket_id_earliest, Skipping: " + str(bucket_path))
				print(ex)
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Can't find bucket_id_earliest, Skipping: " + str(bucket_path) ])
				continue

			# get bucket_id_latest from from bucket path tuple
			try:
				bucket_id_latest = bucket_id_full.split('_')[2]
			except Exception as ex:
				if self.debug:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Can't find bucket_id_latest, Skipping: " + str(bucket_path))
				print(ex)
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Can't find bucket_id_latest, Skipping: " + str(bucket_path) ])
				continue

			# get bucket_id_id from from bucket path tuple
			if not bucket_id_standalone:
				try:
					bucket_id_id = bucket_id_full.split('_')[3] # three number id between latest and GUID (if applicable)
				except Exception as ex:
					if self.debug:
						print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Can't find bucket_id_id, Skipping: " + str(bucket_path))
					print(ex)
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Can't find bucket_id_id, Skipping: " + str(bucket_path) ])
					continue
			else:
				try:
					bucket_id_id = bucket_id_full.split('_')[3].split('/')[0].split('\\')[0]
				except Exception as ex:
					if self.debug:
						print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Can't find bucket_id_id, Skipping: " + str(bucket_path))
					print(ex)
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Can't find bucket_id_id, Skipping: " + str(bucket_path) ])
					continue

			# make final list then convert to tuple for this set -> NOTE additional items that were passed in are tacked on at the end in the same order
			uid = str(bucket_state_path) + "_" + str(bucket_index_path) + "_" + str(bucket_db_path) + "_" + str(bucket_id_origin)
			bid = str(bucket_id_earliest) + "_" + str(bucket_id_latest) + "_" + str(bucket_id_id) + "_" + str(bucket_id_guid)
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Making base list for return." + str(bucket_path) ])
			tmp_bucket_list = [bucket_id_earliest, bucket_id_latest, bucket_id_id,
							bucket_id_guid, bucket_id_standalone, bucket_id_origin, bucket_path[1], bucket_path[0],
							str(bucket_state_path), str(bucket_index_path), str(bucket_db_path), str(uid), str(bid)
							]
			# add additional items not used back to the list - anything the main script passed in here from its list, we will return at the end of ours
			'''
			Because this is modular and can be given a list from Azure, GCP, AWS etc... there may be data the user wants to carry with the bucket details
			i.e in Azure, the Container name is used to download the file later, but we dont need the container here, however it would cost a lot to cycle through
				this final list and tack the container on when the bucket id is found later, so its better to keep it with the data all the time
			'''
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Adding any additional items we received on list back into final base list for return." + str(bucket_path) ])
			if len(bucket_path) > 2:
				for idx, item in enumerate(bucket_path):
					if idx > 1:
						tmp_bucket_list.append(item)
			bucket_tuple = tuple(tmp_bucket_list)
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Adding this bucket final tuple details back to master list." + str(bucket_path) ])
			# add to master list
			bucket_info_tuples_list.append(bucket_tuple)

		if self.debug:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) + "): Converting uid dictionary into unique list." )
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Converting uid dictionary into unique list."])

		if self.debug:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) + "): Generating Master Bucket Dictionary List. This could take some time." )
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Generating Master Bucket Dictionary List."])
		bucket_info_tuples_list.sort(key=lambda x: x[3])
		bucket_dicts_master_list = self.orgnaizeFullListIntoBucketDicts(bucket_info_tuples_list)
		if self.debug:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) + ")" )
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Finished parsing all bucket details, moving onto split and sort of MASTER list." )
		self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Finished parsing all bucket ids."])
		return(bucket_dicts_master_list)
	########################################### ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
	# First bucket details handler - get all the details for a bucket into easy variables
	########################################### 

	########################################### 
	# Sorters and Splitters - master list will be sorted at a micro level and then broken out into multiple lists (depending on peer amount)
	########################################### 
	'''
	Because it can literally take days to sort 300K - 1mn items at this level, we instead "quick sort" using dictionaries
	The end goal is group like buckets together so we can then split them up evenly.
	If we split the master list now, all of "cisco" buckets could end up on a single indexer, where instead we want all cisco frozen db to be split up evenly
		and all cisco warm db to be split up evenly and so on. Granular grouping is by "state, index and then db" 
		-> /warm/cisco/db/, /warm/mcafee/db/ 
		-> /cold/cisco/coldb etc....  
		each of those are then split up evenly amongs the peers
		the format from here starts like this
		{/warm/cisco/db/, [ list from above containing the details of a single bucket file, and another, and another, etc ]}
	'''
	def orgnaizeFullListIntoBucketDicts(self, bucket_info_tuples_list:list):
		self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Sorting bucket files into dictionaries for fast iteration."])
		if self.debug:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) + ")" )
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Sorting bucket files into dictionaries for fast iteration." )
		result = OrderedDict({})
		for bt in bucket_info_tuples_list:
			result.setdefault(bt[11], []).append(bt)
		self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): All UID Dictionaries processed. Total items in list: " + str(len(result))])
		if self.debug:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) + ")" )
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): All UID Dictionaries processed. Total items in list: " + str(len(result)) )
		return(result)

	# split a large list into smaller lists
	'''
	This takes the most amount of time of the entire process next to downloading the actual files.
	All granular lists in the dict format from the function above get processed through here and then distributed across tmp peer lists
	Sub dicts are made for each chunk thats split off (again for performance)
	We dont want to break the final bucket out yet as we still want to balance the lists. Balancing a few thousand dict lists is WAAAAAY faster than
	a million bucket files directly
	'''
	def splitList(self, list_to_split:list, split_by:int) -> list:
		'''
		Split a list into sublists based on split factor specified
		Remainder will be tacked onto the last list if needed
		'''
		self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "):  Splitting list up by this amount: " + str(split_by)])
		master_list_of_sublists = [] # if theres 5 chunks, there will be 5 lists in here

		result = OrderedDict({})
		# split list now into peers then create new dict so they stay with self
		for bt in list_to_split: # convert to dict to maintain smaller tuple list already sorted after size balance
			result.setdefault(bt[12], []).append(bt)

		result_dict_list = list(result.items())
		total_list_item_count = len(result_dict_list)
		# create the empty sublists for each peer to hold
		for x in range(split_by):
			master_list_of_sublists.append([])

		# start splitting list and adding to empty master list of sublists
		if total_list_item_count <= split_by:
			while total_list_item_count > 0:
				for idx, item in enumerate(result_dict_list):
					if idx % 2000 == 0:
						print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"):	   Still working... iteration -> " + str(idx) )
					master_list_of_sublists[idx].append(item)
					total_list_item_count -= 1
			return(master_list_of_sublists)
		else:
			per_chunk_count = int(total_list_item_count / split_by)
			counter = -1
			while total_list_item_count > 0:
				counter += 1
				for m_sub_list in master_list_of_sublists:
					if counter % 2000 == 0:
						print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"):	   Still working... iteration -> " + str(counter))
					m_sub_list.extend(result_dict_list[0:per_chunk_count])
					del result_dict_list[0:per_chunk_count]
					total_list_item_count -= 1
			return(master_list_of_sublists)

	########################################### 
	# Balance list of lists by length and "size" in bytes
	########################################### 
	'''
	Once the dict lists have bee broken up into separate granular dict lists we'll run them through a balancer
	Even if they all have the same amount of jobs, some buckets could weight more than others in terms of disk space.
	We'll total up the size of each and and start moving some around to balance them out. Size is more important than amount in the end ;)
	'''
	def balanceListOfLists(self, master_list_of_lists:list) -> list:
		'''
		There are x amount of list in the master list of lists, where x is the amount of peers to divide by.
		The items in each of those lists are python dictionaries with one kv pair.
		The key is a custom bucket ID string: <earliest_latest_bucketidnum_guid>
		The value is a list of tuples containing all of the files that bucket contains as well as the bucket info and file info, inc byte sizes
		i.e  (obviously with real values though)
		
		123212312_2343432422_343_HSOEF-23fWDF-WEfwefwe-FWEFWE : [ [ <bucket_id_earliest>, <bucket_id_latest>, 454,
																  <bucket_id_guid>, <bucket_id_standalone>, True, <full/path/to/file.gz>, <size of file in bytes>,
																  'frozendata', 'cisco', 'frozendb', <str(uid)>, <str(bid)>],
																  [ <bucket_id_earliest>, <bucket_id_latest>, 454,
																  <bucket_id_guid>, <bucket_id_standalone>, True, <full/path/to/result.csv>, <size of file in bytes>,
																  'frozendata', 'cisco', 'frozendb', <str(uid)>, <str(bid)>]
																]
		This will take only the bytes from the file tuples and add them up to get a total for the dict.
		It then finds the total for all lists and calulates a grand total and average size each list should have. Its not important how many files each node downloads so much as
			how much data they each download is spread out evenly.
		A margin is generated between 1 and 10%. This will move items that are higher on the margin to lower ones. The entire dict is moved so all the files stay with it. This is worlds quicker than trying to move individual files.
		When all lists are even (relatively within the margin) we move on. After this is where the dicts are eliminated and the file tuples are added to the main lists directly.
		'''
		if self.debug:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) + ")" )
		for list_item in master_list_of_lists:
			if not list_item:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Master List of lists was empty, cannot continue -")
				self.bucketeer_timer.stop()
				return(False)		
		# check list byte sizes (MB)
		if self.debug:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) + ")" )
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Attempting to balance buckets by size of files in list. -")
		self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Attempting to balance buckets by size of files in list."])
		size_balanced = False
		total_size = 0 # of all files added up
		total_d_count = 0 # amount of "parents" housing files that need to stay together
		d_size_amt_list = []
		try:
			# get total size of all lists combined
			for lst in master_list_of_lists:
				tmp_size = 0
				for d in lst: # get each tuble containing list of buckets by bucket id
					total_d_count += 1
					for b in d[1]: # get bucket tuples in bid dict
						tmp_size += (b[6]/1024.0**2)
					d_size_amt_list.append(tmp_size)
				total_size += tmp_size
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "):: STARTING List Total Size (mb) " + str(master_list_of_lists.index(lst)) + ": " + str(tmp_size)])
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): STARTING List Total Size (mb) " + str(master_list_of_lists.index(lst)) + ": " + str(tmp_size) )
			# get bid dict size median
			d_size_median = statistics.median(d_size_amt_list)
			# get averages
			average_size_per_bid_dict = total_size / total_d_count
			average_size_per_list = total_size / len(master_list_of_lists) # how many TB/GB etc SHOULD be in each list
			if len(master_list_of_lists) / 2 >= 10:
				self.size_error_margin = 10
			else:
				self.size_error_margin = len(master_list_of_lists) / 2
			self.size_error_margin = self.size_error_margin / 100 # as a percent in decimal
			margin = average_size_per_list * self.size_error_margin # margin total size number
			print("\n- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Total size mb: ", total_size)
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): average_size_per_list: ", average_size_per_list)
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Margin MB: ", margin)
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Margin %: ", self.size_error_margin * 100)
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Total size mb: " + str(total_size)])
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): average_size_per_list: " + str(average_size_per_list)])
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Margin MB: " + str(margin)])
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Margin %: " + str(self.size_error_margin)])
			print("\n")
			above_margin = []
			below_margin = []
			within_margin = []
			for lst in master_list_of_lists:
				tmp_size_total = 0
				for d in lst:
					for b in d[1]:
						tmp_size_total += (b[6]/1024.0**2)
				tmp_diff_from_avg = tmp_size_total - average_size_per_list
				if abs(tmp_diff_from_avg) > margin:
					if tmp_diff_from_avg < 0:
						tmp_diff_from_margin = abs(tmp_diff_from_avg) - margin
						below_margin.append([lst, abs(tmp_diff_from_margin)])
					else:
						tmp_diff_from_margin = tmp_diff_from_avg - margin
						above_margin.append([lst, abs(tmp_diff_from_margin)])
				else:
					within_margin.append([lst, abs(tmp_diff_from_avg)])
			for i in within_margin:
				above_margin.append(i)
				within_margin = []
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Below margin: " + str(len(below_margin)) + " | Above/Within Margin: " + str(len(above_margin)))
			self.log_file.writeLinesToFile([ "(" + str(sys._getframe().f_lineno) + "): Below margin: " + str(len(below_margin)) + " | Above/Within Margin: " + str(len(above_margin)) ])
			for idx, lst1 in enumerate(below_margin):
				receiver_original_ask = lst1[1]
				receivers_total = 0
				for idx_2, lst2 in enumerate(above_margin):
					donor_size_total = 0
					tmp_to_remove = []
					while receivers_total < receiver_original_ask: # if our total take is less than need, keep taking
						if receivers_total >= receiver_original_ask or donor_size_total > lst2[1]:
							break
						for d_idx, d in enumerate(lst2[0]): # for each item in list 2
							if receivers_total >= receiver_original_ask or donor_size_total > lst2[1]: # we have what we need or we have all lst2 can give without going under break out
								break
							tmp_b_size = 0
							for b in d[1]: # total size of the bid dict
								tmp_b_size += (b[6]/1024.0**2)
							donor_size_total += tmp_b_size
							receivers_total += tmp_b_size
							if d_idx % 1000 == 0:
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Above Margin List: " + str(idx_2) + " is giving item to Below Margin List: " + str(idx) + " | Iteration: " + str(d_idx)  )
							lst1[0].append(d)
							tmp_to_remove.append(lst2[0].index(d))
					for i in sorted(tmp_to_remove, reverse=True):
						try:
							del lst2[0][i]
						except Exception as ex:
							continue
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Jobs balanced by size to a margin of: " + str(self.size_error_margin*100) + "%"])
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Jobs balanced by size to a margin of: " + str(self.size_error_margin*100) + "%")
			for lst in master_list_of_lists:
				tmp_size = 0
				for d in lst: # get each tuble containing list of buckets by bucket id
					for b in d[1]: # get bucket tuples in bid dict
						tmp_size = tmp_size + (b[6]/1024.0**2)
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "):: List Total Size (mb) " + str(master_list_of_lists.index(lst)) + ": " + str(tmp_size)])
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): List Total Size (mb) " + str(master_list_of_lists.index(lst)) + ": " + str(tmp_size) )
			time.sleep(10)
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Failed to balance by length by combined file size. -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Jobs per list size: FAILED."])
			print(ex)
		# finish
		return(master_list_of_lists)

	# Wrapper for the "splitlist" function above - see it for more details
	def divideMasterBucketListAmongstPeers(self, peer_list:tuple, bucket_dicts_master:dict):
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Dividing bucket list amongst peers.")
		self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Dividing bucket list amongst peers."])
		peer_num = len(peer_list)
		tmp_peer_download_lists = [] # if theres 5 peers, there  will be 5 lists in here
		for x in range(peer_num):  # create the empty placeholder list of sub lists
			tmp_peer_download_lists.append([])
		try:
			time.sleep(5)
			if self.debug:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) + ")" )
			# split each list of like UIDs among the peers
			if self.debug:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Splitting list up by this amount (this may take some time): " + str(peer_num) )
			length_of_list = len(bucket_dicts_master.items())
			for uid_idx, uid_dict in enumerate(bucket_dicts_master.items()):
				percent = int((uid_idx + 1) / length_of_list * 100)
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Working on UID: " + str(uid_idx + 1) + " / " + str(length_of_list), " | ", str(percent) + "%" )
				tmp_master_list_of_lists = self.splitList(uid_dict[1], peer_num) # this function will take the sublist and divide it among the peers and return it to be added to master ongoing
				for idx, tmp_lst in enumerate(tmp_master_list_of_lists):
					tmp_peer_download_lists[idx].extend(tmp_lst)
				if uid_idx >= length_of_list:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Working on UID: " + str(length_of_list) + " / " + str(length_of_list), " | ", str(100) + "%" )
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Splitting list after peer divide had an issue. -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Exception: Splitting list after peer divide had an issue."])
			print(ex)
		try:
			if self.debug:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) + ")" )
			final_peer_download_dicts = self.balanceListOfLists(tmp_peer_download_lists) # this function will run balance checks and return the final list
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Final tuple extract of lists. -")
			print(ex)
			self.bucketeer_timer.stop()
			return(False)
		if self.debug:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) + ")" )
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): FINISHED Dividing bucket list amongst peers.")
		self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): FINISHED Dividing bucket list amongst peers."])
		return(final_peer_download_dicts)
	########################################### ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
	# Sorters and Splitters - master list will be sorted at a micro level and then broken out into multiple lists (depending on peer amount)
	########################################### 

	########################################### 
	# Final processing of sorted list
	########################################### 
	def verifyBucketList(self, bucket_list:list) -> bool:
		'''
		Verify each item is in the following format [<full_path_to_a_bucket_file>, <file_size_bytes>]
		'''
		if bucket_list:
			for b in bucket_list:
				if not isinstance(b[0], str) and isinstance(b[1], int):
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Bucket list format example found was " + str(b) +" and verification failed, cannot continue, ensure format starts with proper <str>, <bytes> -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Bucket list format example found was " + str(b) +" and verification failed, cannot continue, ensure format starts with proper <str>, <bytes>"])
					self.bucketeer_timer.stop()
					return(False)
			return(True)
		else:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Master List of lists was empty, cannot continue -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Master List of lists was empty, cannot continue."])
			return(False)

	# FINALLY we go through all of the dicts and pull out the nested bucket file details - in the end each list is just a simple list full of bucket detail tuples - sorted of course
	def createMasterTupleListFromDicts(self, final_peer_download_dicts):
		try:
			final_master_download_list_of_lists = []
			for x in range(len(final_peer_download_dicts)):
				final_master_download_list_of_lists.append([])
			for idx, lst in enumerate(final_peer_download_dicts):
				for d in lst:
					for b in d[1]:
						final_master_download_list_of_lists[idx].append(b)
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Amount of peer lists: " + str(len(final_master_download_list_of_lists)) )
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Amount of peer lists: " + str(len(final_master_download_list_of_lists))])
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Failed creating simple list from Tuples. Exiting.")
			print(ex)
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Failed creating simple list from Tuples. Exiting."])
			self.bucketeer_timer.stop()
			return(False)
		return(final_master_download_list_of_lists)
	########################################### ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
	# Final processing of sorted list
	########################################### 
	
	########################################### 
	#  START - All of the above starts here
	########################################### 
	def start(self, bucket_list=[], replace=True):
		'''
		After creating the class in external script for this module, you can start sorting the list by calling this function
		Any buckets here will be ADDED to buckets you added by default!
		Set replace_list to True to replace the original list with the one entered at startup
		'''
		# if user created this class but wants to add more buckets at the start of it, this handles that
		if not self.skip_to_csv_load:
			if bucket_list:
				if replace:
					self.list_of_bucket_list_details = bucket_list
				else:
					self.list_of_bucket_list_details.extend(bucket_list)
				# make sure the bucket list is in the proper format ( bucketfilepath, size)
				if self.verifyBucketList(self.list_of_bucket_list_details):
					# break each bucket into a tuple with multiple elements, each containing a detail about the bucket
					bucket_dicts_master = self.splitBucketDetails() # return final master dict list of buckets
					if bucket_dicts_master: # no csv yet or theres new items in the list to add to csv will make this true
						final_peer_download_dicts = self.divideMasterBucketListAmongstPeers(self.idx_cluster_peers, bucket_dicts_master)
						self.final_peer_download_lists = self.createMasterTupleListFromDicts(final_peer_download_dicts) # create x amount of list full of simple tuples
						try:
							print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Sorting master list of peer lists.")
							self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Sorting master list of peer lists."])
							self.final_peer_download_lists.sort()
							print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Done.")
							self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Done."])
						except Exception as ex:
							print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Sort of master list of lists failed. Exiting.")
							print(ex)
							self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Sort of master list of lists failed. Exiting."])
							self.bucketeer_timer.stop()
							return(False)
						print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Starting CSV build loop on peers...")
						self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Starting CSV build loop on peers..."])
						for idx, p in enumerate(self.idx_cluster_peers): # assign each list to a guid and write out its respective csv (each peer will do some extras when they hit THEIR list)
							print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Getting csv and write queue on peer: " + str(idx) )
							self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "):  Getting csv and write queue on peer: " + str(idx) ])
							if p == self.my_guid:
								self.this_peer_index = idx
							# write lists to csvs
							try:
								guid_queue = self.getPeerCSVQ(p) # the peers queue
								guid_csv = self.getPeerCSV(p) # the peers CSV
							except Exception as ex:
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Failed getting GUID and/csv from queue. Exiting.")
								print(ex)
								self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Failed getting GUID and/csv from queue. Exiting."])
								self.bucketeer_timer.stop()
								return(False)
							print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Done.")
							self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Done."])

							print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Priming CSV build on peer: " + str(idx) )
							self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): priming CSV build on peer: " + str(idx) ])
							try:
								item_count = len(self.final_peer_download_lists[idx][0])
								add_diff = item_count - 13
								# put dummy headers in for the columns we arent gonna need sending back but keep the additional as promised
								header_row = ['rm_1', 'rm_2', 'Bucket_ID', 'rm_3', 'Was_Standalone', 'db_Bucket(not_rb)', 'Expected_File_Size_bytes', 'File_Name', 'rm_4', 'rm_5', 'rm_6', 'rm_7', 'rm_8']
								new_header_row = ['File_Name', 'Expected_File_Size_bytes', 'Expected_File_Size_MB', 'Was_Standalone', 'Bucket_ID', 'db_Bucket(not_rb)', 'Download_Complete', 'Downloaded_File_Size_MB']
								if item_count > 13: # we always break the buckets out into 15 details in the tuple, if there are more items in the tuple, it was additional data the user wanted back
									if self.include_additioanl_list_items_in_csv: # add the additional tuple items to the list and then csv (if the user set the option to do so)
										for x in range(add_diff):
											header_row.append("Additional_" + str(x + 1))
											new_header_row.append("Additional_" + str(x + 1))
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Temp Header Row:", (header_row))
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Final Header Row:", (new_header_row))
								df = pandas.DataFrame(self.final_peer_download_lists[idx],columns=(header_row)) # create data frame with full list
								'''
								Remove the columns we dont need.
								We always write out these columns in this order: 'File_Name', 'Expected_File_Size_bytes', 'Expected_File_Size_MB', 'Was_Standalone', 'Bucket_ID', 'db_Bucket(not_rb)', 'Download_Complete', 'Downloaded_File_Size_MB' + any Additional_x
								'''
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Removing not needed columns.")
								self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Removing not needed columns."])
								for x in ['rm_1', 'rm_2', 'rm_3', 'rm_4', 'rm_5', 'rm_6', 'rm_7', 'rm_8']:
									del df[x]
								df['Expected_File_Size_MB']=''
								df['Download_Complete']='NO'
								df['Downloaded_File_Size_MB']=''
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Dataframe before sort.\n")
								print(df)
								print("\n\n\n")
								time.sleep(5)
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Sorting columns.")
								self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Sorting columns."])
								df = df[new_header_row] # arrange the columns the way we want to send them back
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Dataframe AFTER sort.\n")
								print(df)
								print("\n\n\n")
								time.sleep(5)
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Done. Creating dataframe, writing to CSV NOW." )
								self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Done. Creating dataframe, writing to CSV NOW." + str(idx) ])
								df.to_csv(guid_csv.log_path, index=False)
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Done. Writing dataframe to CSV." )
								self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Done. Writing dataframe to CSV." + str(idx) ])	
							except Exception as ex:
								print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Failed prepping CSV. Exiting.")
								print(ex)
								self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Failed prepping CSV. Exiting."])
								self.bucketeer_timer.stop()
								return(False)
		# next read csv into a dataframe and strip all items that have been downloaded according to the csv leaving us with a new download list -ready for action
		try:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Removing all downloaded items before passing back list. -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Removing all downloaded items before passing back list. "])
			df = pandas.read_csv(self.getPeerCSV().log_path, engine='python')
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): This Peer Dataframe before completed Remove.\n")
			print(df)
			print("\n\n\n")
			time.sleep(5)
			df = df[df.Download_Complete != 'SUCCESS'] # remove ROWS where lines under that HEADER are not "SUCCESS"
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): This Peer Dataframe final.\n")
			print(df)
			print("\n\n\n")
			time.sleep(5)
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Done. -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Done. "])
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Couldn't read csv list to dataframe. Exiting. -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Couldn't read csv list to dataframe. Exiting. "])
			print(ex)
			self.bucketeer_timer.stop()
			return(False)
		try:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Converting data frame to python list for download processing. -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Converting data frame to python list for download processing. "])
			self.this_peer_download_list = df.values.tolist() # set the variable in this class of this peers list (can be accessed from main)
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Number in This Peers Download list is: " + str(len(self.this_peer_download_list)))
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Number in This Peers Download list is: " + str(len(self.this_peer_download_list))])
			if not self.this_peer_download_list:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Appears there are no items to download in this list, perhaps its cmpleted? Exiting.")
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Appears there are no items to download in this list, perhaps its cmpleted? Exiting."])
				time.sleep(5)
				return(False)
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Done. -")
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Couldn't convert dataframe to list. Exiting. -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Couldn't convert dataframe to list. Exiting. "])
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Done. "])
			print(ex)
			self.bucketeer_timer.stop()
			return(False)
		if self.this_peer_download_list: # make sure the list isnt empty and send it back to main for download!
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Bucketeer is done processing. ")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Number in This Peers Download list is: " + str(len(self.this_peer_download_list))])
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Bucketeer is done processing. "])
			self.bucketeer_timer.stop()
			return(True)
		else:
			self.bucketeer_timer.stop()
			return(False)