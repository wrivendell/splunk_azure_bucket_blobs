##############################################################################################################
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
##############################################################################################################

### Imports ###########################################
import time, sys, re, threading, pandas

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
	def __init__(self, name: str, sp_home:str, sp_uname:str, sp_pword:str, list_of_bucket_list_details=[], sp_idx_cluster_master_uri='', port=8089, main_report_csv='', include_additioanl_list_items_in_csv=True, debug=False):
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
			sys.exit()
		if not sp_pword:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): No Splunk Password provided, yet Cluster is indicated. I'm not a mind reader! Cluster Master API call not possible. Exiting.")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): No Splunk Password provided, yet Cluster is indicated. I'm not a mind reader! Cluster Master API call not possible. Exiting."] )
			sys.exit()
		self.debug = debug # enable debug printouts from THIS class
		self.name = name # unique thread name
		self.list_of_bucket_list_details = list_of_bucket_list_details # master list of buckets to be downloaded
		self.sp_home = sp_home
		self.sp_uname = sp_uname
		self.sp_pword = sp_pword
		self.sp_idx_cluster_master_uri = sp_idx_cluster_master_uri
		self.port = port
		self.include_additioanl_list_items_in_csv = include_additioanl_list_items_in_csv

		# see if cluster master URI is available
		if not self.sp_idx_cluster_master_uri:
			self.sp_idx_cluster_master_uri = wrsops.findClusterMasterByFile(self.sp_home)
			# remove the port if found in file since its specified already
			if not self.sp_idx_cluster_master_uri[0]:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Couldn't find an IDX Cluster Master URI and non specified. No buckets will be downloaded here. Exiting.")
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Couldn't find an IDX Cluster Master URI and non specified. No buckets will be downloaded here. Exiting."] )
				sys.exit()
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
			sys.exit()
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
			if bucket_path[1] <= 0 and not str(bucket_path[0]).endswith(".csv") or not str(bucket_path[0]).endswith(".result"):
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
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Appears to not be a clustered bucket setting as standalone" + str(bucket_id_guid))
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
				print(" -" + str( (bucket_id_earliest) ) )
				print(" -" + str( (bucket_id_latest) ) )
				print(" -" + str( (bucket_id_id) ) )

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
						print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"):       Still working... iteration -> " + str(idx) )
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
						print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"):       Still working... iteration -> " + str(counter))
					m_sub_list.extend(result_dict_list[0:per_chunk_count])
					del result_dict_list[0:per_chunk_count]
					total_list_item_count -= 1
			return(master_list_of_sublists)

	########################################### 
	# Balance list of lists by length and "size" in bytes
	########################################### 
	'''
	Once the dict lists have bee broken up into separate granular dict lists we'll run them through a few balancers
	We'll ensure the lists have roughly the same amount of jobs but some buckets could weight more than others in terms of disk space.
	We'll then total up the size of each and and start moving some around to balance them out. Size is more important than amount in the end ;)
	'''
	def balanceListOfLists(self, master_list_of_lists:list) -> list:
		'''
		This will do the following:
		1. Ensure the lists sizes in terms of sheer number of items is as even as can be among them
		2. Check byte sizes of downloads sum and redisctribute when needed / possible
		'''
		if self.debug:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) + ")" )
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Attempting to balance buckets by amount of jobs per peer. -")
		self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Attempting to balance buckets by amount of jobs per peer."])
		for list_item in master_list_of_lists:
			if not list_item:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Master List of lists was empty, cannot continue -")
				sys.exit()
#		master_list_of_lists.sort(key=operator.itemgetter('uid'))
		lowest = 999999999999999999999
		highest = 0
		len_balanced = False
		lowest_lst = []
		highest_lst = []
		margin = 15
		timed_out = False
		try:
			len_list_timeout = wrc.timer('len_list_timeout', 1200) # timeout timer
			threading.Thread(target=len_list_timeout.start, name='len_list_timeout', args=(), daemon=True).start()
			while not len_balanced:
				if len_list_timeout.max_time_reached:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Balancing by size stopped due to timeout and moved on as is. -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Balancing by size stopped due to timeout and moved on as is."])
					timed_out = True
					break
				for lst in master_list_of_lists: # get lowest and highest lists in the master list
					if len(lst) < lowest:
						lowest = len(lst)
						lowest_lst = lst
					elif len(lst) > highest:
							highest = len(lst)
							highest_lst = lst
				high_low_diff = highest - lowest
				if high_low_diff > margin:     # if lists are within margin lengths of each other, consider that fine
					high_low_diff = high_low_diff / 2
					lowest_lst = lowest_lst.extend(highest_lst[0:high_low_diff]) # move half the delta to the lowest from highest
					del highest_lst[0:high_low_diff]
				else:
					len_balanced = True
			len_list_timeout.stop()
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Jobs per peer balance: Finished."])
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Failed to balance by length -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Jobs per peer balance: Failed."])
			print(ex)
		
		# check list byte sizes (MB)
		if self.debug:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) + ")" )
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Attempting to balance buckets by size of files in list. -")
		self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Attempting to balance buckets by size of files in list."])
		size_balanced = False
		total_size = 0
		try:
			# get total size of all lists combined
			for lst in master_list_of_lists:
				tmp_size = 0
				for d in lst: # get each tuble containing list of buckets by bucket id
					for b in d[1]: # get bucket tuples in bid dict
						total_size = total_size + (b[6]/1024.0**2)
						tmp_size = tmp_size + (b[6]/1024.0**2)
			# get average MB per list
			average_size_per = total_size / len(master_list_of_lists)
			if len(master_list_of_lists)/2 >= 10:
				self.size_error_margin = 10
			else:
				self.size_error_margin = len(master_list_of_lists)/2
			self.size_error_margin = self.size_error_margin / 100 # as a percent
			margin = average_size_per * self.size_error_margin # % margin
			size_list_timeout = wrc.timer('size_list_timeout', 1200) # timeout timer
			threading.Thread(target=size_list_timeout.start, name='size_list_timeout', args=(), daemon=True).start()
			while not size_balanced:
				if size_list_timeout.max_time_reached:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Balancing by size stopped due to timeout and moved on as is. -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Balancing by size stopped due to timeout and moved on as is."])
					timed_out = True
					break
				above_margin = []
				below_margin = []
				within_margin = []
				for lst in master_list_of_lists:
					tmp_size_total = 0
					for d in lst:
						for b in d[1]:
							tmp_size_total = tmp_size_total + (b[6]/1024.0**2)
					tmp_diff_from_avg = tmp_size_total - average_size_per
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
				if not below_margin:
					size_balanced = True
					break
				for idx, lst1 in enumerate(below_margin):
					receiver_original_ask = lst1[1]
					for lst2 in above_margin:
						donor_size_total = 0
						tmp_to_remove = []
						if lst2[1] < lst1[1]: # we can only give up to what lst2 can afford cant cover it all
							size1_list_timeout = wrc.timer('size1_list_timeout', 1200) # timeout timer
							threading.Thread(target=size1_list_timeout.start, name='size1_list_timeout', args=(), daemon=True).start()
							while donor_size_total < lst2[1]: # if our total "take" is NOT equal or more than what he had to give, keep adding
								if size1_list_timeout.max_time_reached:
									print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Balancing by size stopped due to timeout and moved on as is. -")
									self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Balancing by size stopped due to timeout and moved on as is."])
									timed_out = True
									break
								for d in lst2[0]: # for each item in list 2
									for b in d[1]:
										if donor_size_total >= lst2[1]:
											break
										donor_size_total = donor_size_total + (b[6]/1024.0**2)
										lst1[1] = lst1[1] + (b[6]/1024.0**2)
										lst1[0].append(d)
										tmp_to_remove.append(lst2[0].index(d))
							size1_list_timeout.stop()
						else:
							size2_list_timeout = wrc.timer('size2_list_timeout', 1200) # timeout timer
							threading.Thread(target=size2_list_timeout.start, name='size2_list_timeout', args=(), daemon=True).start()
							while donor_size_total < receiver_original_ask: # if our total "take" is NOT equal or more than what he had to give, keep adding
								if size2_list_timeout.max_time_reached:
									print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Balancing by size stopped due to timeout and moved on as is. -")
									self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Balancing by size stopped due to timeout and moved on as is."])
									timed_out = True
									break
								for d in lst2[0]: # for each item in list 2
									for b in d[1]:
										if donor_size_total >= receiver_original_ask:
											break
										donor_size_total = donor_size_total + (b[6]/1024.0**2)
										lst1[1] = lst1[1] + (b[6]/1024.0**2)
										lst1[0].append(d)
										tmp_to_remove.append(lst2[0].index(d))
							size2_list_timeout.stop()
						for i in sorted(tmp_to_remove, reverse=True):
							try:
								del lst2[0][i]
							except Exception as ex:
								continue
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Still " + str(len(below_margin)) + " lists below margin, still balancing List: " + str(idx) + "-")
			size_list_timeout.stop()
			if timed_out:
				self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Jobs balanced as best as possible but couldn't hit the specified margin: " + str(self.size_error_margin*100) + "%"])
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Jobs balanced as best as possible but couldn't hit the specified margin: " + str(self.size_error_margin*100) + "%")
			else:
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
			sys.exit()
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
					sys.exit()
			return(True)
		else:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Master List of lists was empty, cannot continue -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Master List of lists was empty, cannot continue."])
			return(False)

	# FINALLY we go through all of the dicts and pull out the nested bucket file details - in the end each list is just a simple list full of bucket detail tuples - sorted of course
	def createMasterTupleListFromDicts(self, final_peer_download_dicts):
		final_master_download_list_of_lists = []
		for x in range(len(final_peer_download_dicts)):
			final_master_download_list_of_lists.append([])
		for idx, lst in enumerate(final_peer_download_dicts):
			for d in lst:
				for b in d[1]:
					final_master_download_list_of_lists[idx].append(b)
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Amount of peer lists: ", len(final_master_download_list_of_lists))
		return(final_master_download_list_of_lists)
	########################################### ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
	# Final processing of sorted list
	########################################### 
	
	########################################### 
	#  START - All of the above starts here
	########################################### 
	def start(self, bucket_list=[], replace = True):
		'''
		After creating the class in external script for this module, you can start sorting the list by calling this function
		Any buckets here will be ADDED to buckets you added by default!
		Set replace_list to True to replace the original list with the one entered at startup
		'''
		# if user created this class but wants to add more buckets at the start of it, this handles that
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
					write_threads = [] # we'll add our csv thread writer queues to this and then kick them off later
					self.final_peer_download_lists.sort()
					for idx, p in enumerate(self.idx_cluster_peers): # assign each list to a guid and write out its respective csv (each peer will do some extras when they hit THEIR list)
						if p == self.my_guid:
							self.this_peer_index = idx
						# write lists to csvs
						guid_queue = self.getPeerCSVQ(p) # the peers queue
						guid_csv = self.getPeerCSV(p) # the peers CSV 
						tmp_list = [] # tmp list for our prune csv list from the main
						for bt in self.final_peer_download_lists[idx]:
							bt_list = [ bt[7], bt[6], (bt[6]/1024.0**2), bt[4], bt[2], bt[5], 'NO', 0]
							header_row = ['File_Name', 'Expected_File_Size_bytes', 'Expected_File_Size_MB', 'Was_Standalone', 'Bucket_ID', 'db_Bucket(not_rb)', 'Download_Complete', 'Downloaded_File_Size_MB']
							if len(bt) > 13: # we always break the buckets out into 13 details int he tuple, if there are more items in the tuple, it was additional data the user wanted back
								if self.include_additioanl_list_items_in_csv: # add the additional tuple items to the list and then csv (if the user set the option to do so)
									bt_list.extend(bt[13:])
									add_diff = len(bt_list) - 8
									for x in range(add_diff):
										header_row.append("Additional_" + str(x + 1))
							tmp_list.append(  bt_list )
						guid_queue.add(guid_csv.writeLinesToCSV, [[(tmp_list), (header_row)]])
						if p == self.my_guid:
							my_queue = guid_queue
						write_threads.append(threading.Thread(target=guid_queue.start, name='guid_queue' + guid_queue.name, args=())) # add this thread to the write queue
					# start all peers csv writes in the background
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Writing peer lists out to csvs in csv_lists folder. <_name_GUID.csv>  - Do NOT rename or edit these!!")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Writing peer lists out to csvs in csv_lists folder. <_name_GUID.csv>  - Do NOT rename or edit these!! "])
					for t in write_threads:
						t.daemon = False
						t.start()
					# since we started the CSV write in a thread we don't want the peer accessing it til its complete, so we wait
					while not len(my_queue.jobs_active) == 0 and not len(my_queue.jobs_waiting) == 0 and not len(my_queue.jobs_completed) > 0: # wait for csv to be written out
						time.sleep(10)
						print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Waiting... for jobs to finish.")
				# next read csv into a dataframe and strip all items that have been downloaded according to the csv leaving us with a new download list -ready for action
				try:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Removing all downloaded items before passing back list. -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Removing all downloaded items before passing back list. "])
					df = pandas.read_csv(self.getPeerCSV().log_path, engine='python')
					df = df[df.Download_Complete != 'SUCCESS']
					# remove headers now
					df = df.iloc[1:]
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Done. -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Done. "])
				except Exception as ex:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Couldn't read csv list to dataframe. Exiting. -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Couldn't read csv list to dataframe. Exiting. "])
					print(ex)
					sys.exit()
				try:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Converting data frame to python list for download processing. -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Converting data frame to python list for download processing. "])
					self.this_peer_download_list = df.values.tolist() # set the variable in this class of this peers list (can be accessed from main)
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Done. -")
				except Exception as ex:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Couldn't convert dataframe to list. Exiting. -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Couldn't convert dataframe to list. Exiting. "])
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Done. "])
					print(ex)
					sys.exit()
				if self.this_peer_download_list: # make sure the list isnt empty and send it back to main for download!
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Number in This Peers Download list is: " + str(len(self.this_peer_download_list)))
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Bucketeer is done processing. ")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Number in This Peers Download list is: " + str(len(self.this_peer_download_list))])
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Bucketeer is done processing. "])
					self.bucketeer_timer.stop()
					return(True)
				else:
					return(False)
