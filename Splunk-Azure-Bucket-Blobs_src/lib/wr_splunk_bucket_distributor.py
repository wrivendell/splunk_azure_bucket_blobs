##############################################################################################################
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
##############################################################################################################

### Imports ###########################################
import datetime, time, sys, os, re

from pathlib import Path

from . import wr_splunk_ops as wrsops
from . import wr_splunk_wapi as wapi

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
	def __init__(self, name: str, sp_home:str, sp_uname:str, sp_pword:str, list_of_bucket_list_details=[], sp_idx_cluster_master_uri='', port=8089, debug=False):
		'''
		Optionally, Provide Splunk IDX Cluster Master and API port e.g.  splunk_idx_cluster_master_uri="https://cm1.mysplunk.go_me.com" 
			Port is set to default, port=8089 
			If URI left empty, an attempt will be made to find it via the local file system.

			This is for idx CLUSTERS only. Standalone, just download direct and dont import this module.

		The master list_of_bucket_list_details -> is a list containing lists. Each list item is a single download to be downloaded.
			Each item in the list should START with this exact format:  [<full_path_to_a_bucket_file>, <file_size_bytes>]
			You may add as many additional info items to the END and they will come back sorted with the list as <list>[11]+  etc
			But don't mess with the start!!

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
		if not sp_uname:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): No Splunk Username provided, yet Cluster is indicated. I'm not a mind reader! Cluster Master API call not possible. Exiting.")
			sys.exit()
		if not sp_pword:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): No Splunk Password provided, yet Cluster is indicated. I'm not a mind reader! Cluster Master API call not possible. Exiting.")
			sys.exit()
		self.debug = debug # enable debug printouts from THIS class
		self.name = name # unique thread name
		self.list_of_bucket_list_details = list_of_bucket_list_details # master list of buckets to be downloaded
		self.sp_home = sp_home
		self.sp_uname = sp_uname
		self.sp_pword = sp_pword
		self.sp_idx_cluster_master_uri = sp_idx_cluster_master_uri
		self.port = port

		# see if cluster master URI is available
		if not self.sp_idx_cluster_master_uri:
			self.sp_idx_cluster_master_uri = wrsops.findClusterMasterByFile(self.sp_home)
			# remove the port if found in file since its specified already
			if not self.sp_idx_cluster_master_uri[0]:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Couldn't find an IDX Cluster Master URI and non specified. No buckets will be downloaded here. Exiting")
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
			sys.exit()
		else:
			self.my_guid = str(self.my_guid[1])
	
	# get peer GUIDS in this idx cluster
	def getPeerGUIDS(self):
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Creating 'wapi' service called: splunk_idx_cm_service -\n")
		self.splunk_idx_cm_service = wapi.SplunkService( self.sp_idx_cluster_master_uri, self.port, self.sp_uname, self.sp_pword )
		self.guid_list = self.splunk_idx_cm_service.getIDXClusterPeers(guids_only=True)
		self.guid_list.sort()
		self.guid_list = tuple(self.guid_list)
		print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Found the following GUIDS in this order: -")
		for i in self.guid_list:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"):  -" + str(i))
		print("\n")
		return(self.guid_list)

	# split bucket details out into a tuple with get-able variables
	def splitBucketDetails(self):
		'''
		Break master bucker list into tuples per file
		Returns:
			each tuple: [0] = earliest, [1] = latest, [2] = id, [3] = guid, [4] = standalone bool, [5] = origin or replicated bool, [6] = bucket size in bytes,
						[7] = full bucket path originally, [8] = bucket_state_path, [9] = bucket_index_path, [10] = bucket_db_path
		Add tuples to new list and sort by GUID
		'''
		bucket_info_tuples_list = []
		for bucket_path in self.list_of_bucket_list_details:
			# break out the bucket details
			print(bucket_path)
			print("hello 0")
			if bucket_path[1] <= 0:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Skipping blod with 0 byte size: " + str(bucket_path) + " -")
				continue
			print("hello 1")
			bucket_id_full = re.search('(db_.+?)((\\|\/)|$)', bucket_path[0], re.IGNORECASE).group(1)
			if not bucket_id_full:
				bucket_id_full = re.search('(rb_.+?)((\\|\/)|$)', bucket_path[0], re.IGNORECASE).group(1)
			if not bucket_id_full:
				print(bucket_id_full)
				print("hello 2")
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Can't parse bucket ID. You sure your feeding your list in as expected? Failed on: " + str(bucket_path) + ". Skipping- ")
				continue
			else:
				bucket_path_full = re.search('(.+)(db_)', bucket_path[0], re.IGNORECASE).group(1)
				if not bucket_path_full:
					bucket_path_full = re.search('(.+)(rb_)', bucket_path[0], re.IGNORECASE).group(1)
				if not bucket_path_full:
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Can't parse bucket ID. You sure your feeding your list in as expected? Failed on: " + str(bucket_path) + ". Skipping- ")
					continue
				else:
					print(bucket_path_full)
					print("hello 3")
					bucket_state_path = Path(bucket_path_full).parts[0] # cold, warm, hot, or if frozen, custom folder
					print(bucket_state_path)
					print("hello 4")
					bucket_index_path = Path(bucket_path_full).parts[1] # barracuda, mcafee etc
					print(bucket_index_path)
					print("hello 5")
					bucket_db_path = Path(bucket_path_full).parts[2] # frozendb, colddb, db
					print(bucket_index_path)
					print("hello 6")
				try:
					bucket_id_guid = bucket_id_full.split('_')[4].split('/')[0]
					print(bucket_index_path)
					print("hello 7")
					bucket_id_standalone = False
					print(bucket_index_path)
					print("hello 8")
				except:
					bucket_id_guid = 'none'
					bucket_id_standalone = True
				if 'rb' in str(bucket_id_full.split('_')[0]):
					bucket_id_origin = False
				else:
					bucket_id_origin = True
				print(bucket_id_origin)
				print("hello 9")
				bucket_id_earliest = bucket_id_full.split('_')[1]
				bucket_id_latest = bucket_id_full.split('_')[2]
				bucket_id_id = bucket_id_full.split('_')[3]
				# make final list then convert to tuple for this set -> NOTE additional items that were passed in are tacked on at the end in the same order
				tmp_bucket_list = [bucket_id_earliest, bucket_id_latest, bucket_id_id,
								bucket_id_guid, bucket_id_standalone, bucket_id_origin, bucket_path[1], bucket_path[0],
								str(bucket_state_path), str(bucket_index_path), str(bucket_db_path)
								]
				if len(bucket_path) > 2:
					for idx, item in enumerate(bucket_path):
						if idx > 1:
							tmp_bucket_list.append(item)
				bucket_tuple = tuple(tmp_bucket_list)
				print(bucket_tuple)
				print("hello 10")
				# add to master list
				bucket_info_tuples_list.append(bucket_tuple)
				print("hello 11")
				bucket_info_tuples_list.sort(key=lambda x: x[1])
		return(bucket_info_tuples_list)

	# organize the master list into lists by state path (cold, warm, or custom folder for frozen)
	def organizeBucketTuplesByStatePath(self, bucket_info_tuples_list:list):
		'''
		Finds all bucket entries in the main tuple list that have the same state path
		Returns: List of lists, each embedded list contains the tuples that have the same state path as each other

		eg.

		[ main list ]            # main list of bucket tuples
			  [ frozendata ]     # sub list by state path
				b1 tuple
				b2 tuple
				b3 tuple
			  [ warm ]
				b1 tuple
				b2 tuple
				b3 tuple

		'''
		bucket_info_tuples_list_by_state_path = []
		unique_state_paths = []
		for i in bucket_info_tuples_list:
			if i[8] not in unique_state_paths: # get unique state paths to a list
				unique_state_paths.append(i[8])
		for state in unique_state_paths:
			tmp_list = []
			for b in bucket_info_tuples_list:
				if state == b[8]:
					tmp_list.append(b)
			bucket_info_tuples_list_by_state_path.append(tmp_list)
		return(bucket_info_tuples_list_by_state_path)
	
	def organizeBucketTuplesByIndexPathFromStatePath(self, bucket_info_tuples_list_by_state_path):
		'''
		Using the orgnized by state path above, we further sort into smaller lists by index inside

		e.g.

		[ main list ]           # main list of bucket tuples
			  [ frozendata ]     # sub list by state path
				[ barracuda ]	  # sub list index path
					b1 tuple
					b2 tuple
					b3 tuple
				[ mcafee ]	   
					b1 tuple
					b2 tuple
					b3 tuple

			  [ warm ]	 
				[ barracuda]	 	 
					b1 tuple
					b2 tuple
					b3 tuple

				[ mcafee ]
					b1 tuple
					b2 tuple
					b3 tuple
		
		'''
		bucket_info_tuples_list_by_state_path_then_index_path = []
		for state_list in bucket_info_tuples_list_by_state_path: # main list contains sub lists by state_path
			unique_index_paths = []
			state_list_by_index_tmp = [] # each index will have a tmp list per iteration that will add back to the master list bucket_info_tuples_list_by_state_path_then_index_path
			for i in state_list:
				if i[9] not in unique_index_paths: # get unique index paths to a list
					unique_index_paths.append(i[9])
			for index in unique_index_paths: # find all buckets in the state list that match this index and add to tmp list
				tmp_list = []
				for b in state_list: # get the actual bucket tuples in side this state_path list
					if index == b[9]:
						tmp_list.append(b)
				state_list_by_index_tmp.append(tmp_list)
			bucket_info_tuples_list_by_state_path_then_index_path.append(state_list_by_index_tmp)
		return(bucket_info_tuples_list_by_state_path_then_index_path)

	def organizeBucketTuplesByDBPathFromIndexPathList(self, bucket_info_tuples_list_by_state_path_then_index_path):
			'''
			Using the orgnized by index path above, we further sort into smaller lists by db inside

			e.g. return

			[ main list ]           	# main list of bucket tuples 
				  [ frozendata ]     	 # sub list by state path     (state)
						[ barracuda ]	  # sub list index path     (index)
							[frozendb]     # sub list db path        (db)
								b1 tuple
								b2 tuple
								b3 tuple
						[ mcafee ]	   
							[frozendb]   
								b1 tuple
								b2 tuple
								b3 tuple
				  [ warm ]	 
						[ barracuda]	 
							[db]	 
								b1 tuple
								b2 tuple
								b3 tuple
						[ mcafee 
							[db]	 
								b1 tuple
								b2 tuple
								b3 tuple
			'''
			bucket_info_tuples_list_by_state_path_then_index_path_then_db_path = []
			for state_list in bucket_info_tuples_list_by_state_path_then_index_path: # main list contains sub lists by state_path, then index_path -> get down to the tuples
				state_index_list_by_db_tmp = [] # each unique db will have a tmp list per iteration that will add back to the master list bucket_info_tuples_list_by_state_path_then_index_path_then_db_path
				for index_list in state_list:
					unique_db_paths = []
					for i in index_list: # get unique db list
						if i[10] not in unique_db_paths:
							unique_db_paths.append(i[10])
					tmp_index_list = []
					for db in unique_db_paths:
						tmp_db_list = []
						for b in index_list:
							if db == b[10]:
								tmp_db_list.append(b)
						tmp_index_list.append(tmp_db_list)
					state_index_list_by_db_tmp.append(tmp_index_list)
				bucket_info_tuples_list_by_state_path_then_index_path_then_db_path.append(state_index_list_by_db_tmp)
			return(bucket_info_tuples_list_by_state_path_then_index_path_then_db_path)

	# do all three of the above in one go
	def organizeMasterListByStateIndexDB(self, bucket_info_tuples_list:list):
		try:
			tmp_list1 = self.organizeBucketTuplesByStatePath(bucket_info_tuples_list)
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Organizing By State Path. -")
			print(ex)
		try:
			tmp_list2 = self.organizeBucketTuplesByIndexPathFromStatePath(tmp_list1)
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Organizing By Index Path. -")
			print(ex)
		try:
			separated_master_bucket_tuple_list = self.organizeBucketTuplesByDBPathFromIndexPathList(tmp_list2)
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Organizing By DB Path. -")
			print(ex)
		return(separated_master_bucket_tuple_list)

	# split a large list into smaller lists
	def splitList(self, list_to_split:list, split_by:int) -> list:
		'''
		Split a list into sublists based on split factor specified
		Remainder will be tacked onto the last list if needed
		'''
		master_list_of_sublists = [] # if theres 5 chunks, there will be 5 lists in here
		total_list_item_count = len(list_to_split)
		for x in range(split_by):
			master_list_of_sublists.append([])
		if total_list_item_count <= split_by:
			while total_list_item_count > 0:
				for idx, item in enumerate(list_to_split):
					master_list_of_sublists[idx].append(item)
					total_list_item_count -= 1
			return(master_list_of_sublists)
		else:
			per_chunk_count = int(total_list_item_count / split_by)
			first_index = 0 - per_chunk_count # minus the total for a negative so first iteration starts at 0
			last_index = 0
			while total_list_item_count > 0:
				for m_sub_list in master_list_of_sublists:
					first_index = first_index + per_chunk_count
					last_index = last_index + per_chunk_count
					tmp_list = list_to_split[first_index:last_index]
					m_sub_list.extend(tmp_list)
					total_list_item_count -= 1
			return(master_list_of_sublists)

	# balance list of lists by length and "size" in bytes
	def balanceListOfLists(self, master_list_of_lists) -> list:
		'''
		This will do the following:
		1. Ensure the lists sizes in terms of sheer number of items is as even as can be among them
		2. Check byte sizes of downloads sum and redisctribute when needed / possible
		3. Ensure all files for buckets are on the same list
		'''
		for list_item in master_list_of_lists:
			if not list_item:
				print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Master List of lists was empty, cannot continue -")
				sys.exit()
		master_list_of_lists.sort()
		lowest = 999999999999999999999
		highest = 0
		len_balanced = False
		restart = False
		lowest_lst = []
		highest_lst = []
		margin = 15
		try:
			while not len_balanced:
				for idx, lst in enumerate(master_list_of_lists): # get lowest and highest lists in the master list
					if len(lst) < lowest:
						lowest = len(lst)
						lowest_lst = lst
					elif len(lst) > highest:
							highest = len(lst)
							highest_lst = lst
					if not idx >= len(master_list_of_lists) - 1: # ensure a full iteration goes through before changing anything
						continue
					elif restart:
							continue
					else:
						high_low_diff = highest - lowest
						if high_low_diff > margin:     # if lists are within margin lengths of each other, consider that fine
							high_low_diff = high_low_diff / 2
							lowest_lst = lowest_lst.extend(highest_lst[0:high_low_diff]) # move half the delta to the lowest from highest
							del highest_lst[0:high_low_diff]
							restart = True # run again to ensure balanced, will repeat process til all within "equal" margin
						else:
							len_balanced = True
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Failed to balance by length -")
			print(ex)
		# check list byte sizes (MB)
		master_list_of_lists.sort()
		lowest = 999999999999999999999 # set this high so the first one is always lower than it and sets the bar
		highest = 0
		size_balanced = False
		restart = False
		lowest_lst = []
		highest_lst = []
		try:
			while not size_balanced:
				for idx, lst in enumerate(master_list_of_lists): # get lowest and highest sizes lists in the master list
					tmp_size_total = 0
					for b in lst:
						tmp_size_total = tmp_size_total + (b[6]/1024.0**2) # convert to mb so we're using smaller nums
					if tmp_size_total < lowest:
						lowest = tmp_size_total
						lowest_lst = lst
					elif tmp_size_total > highest:
						highest = tmp_size_total
						highest_lst = lst
					if not idx >= len(master_list_of_lists) - 1: # ensure a full iteration goes through before changing anything
						continue
					elif restart:
						continue
					else:
						high_low_diff = highest - lowest
						margin_diff = highest / len(master_list_of_lists)
						if high_low_diff > margin_diff:
							high_low_diff = high_low_diff / 2
							tmp_move_list = []
							tmp_move_size = 0
							for b in highest_lst:
								tmp_move_size = tmp_move_size + (b[6]/1024.0**2)
								tmp_move_list.append(b)
								if tmp_move_size >= high_low_diff:
									break
							if tmp_move_list:
								for ml in tmp_move_list:
									lowest_lst.append(ml)
									highest_lst.remove(ml)
							restart = True # run again to ensure balanced
						else:
							size_balanced = True
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Failed to balance by length by combined file size. -")
			print(ex)
		# ensure bucket files didnt get moved away from their friends
		try:
			master_list_of_lists.sort()
			for idx, lst in enumerate(master_list_of_lists):
				for cur_list_b in lst:
					for other_idx, other_lst in enumerate(master_list_of_lists):
						if idx == other_idx: # dont check self since in the same master list as other sub lists
							continue
						for other_list_b in other_lst: # compare item to items in other lists for matching buckets
							if cur_list_b[0] == other_list_b[0] and cur_list_b[1] == other_list_b[1]: #check earliest and latest first
								if cur_list_b[2] == other_list_b[2]: #check ID
									if cur_list_b[5] == other_list_b[5]: #check Replicated status
										if cur_list_b[8] == other_list_b[8] and cur_list_b[9] == other_list_b[9] and cur_list_b[10] == other_list_b[10]: #paths match = same bucket file in other list
											lst.append(other_list_b)
											other_lst.remove(other_list_b)
											continue
									else:
										continue
								else:
									continue
							else:
								continue
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Failed to balance by like bucket files -")
			print(ex)
		return(master_list_of_lists)

	def divideMasterBucketListAmongstPeers(self, peer_list:tuple, separated_master_bucket_tuple_list:list):
		peer_num = len(peer_list)
		final_peer_download_lists = [] # if theres 5 peers, there  will be 5 lists in here
		for x in range(peer_num):  # create the empty placeholder list of sub lists
			final_peer_download_lists.append([])
		try:
			for state_list in separated_master_bucket_tuple_list:
				for index_list in state_list:
					for db_list in index_list: # update ongoing list
						tmp_master_list_of_lists = self.splitList(db_list, peer_num) # this function will take the sublist and divide it among the peers and return it to be added to master ongoing
						for idx, lst in enumerate(tmp_master_list_of_lists):
							final_peer_download_lists[idx].extend(lst)
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Splitting list after peer divide had an issue. -")
			print(ex)
		try:
			final_peer_download_lists = self.balanceListOfLists(final_peer_download_lists) # this function will run balance checks and return the final list
			final_peer_download_lists.sort()
		except Exception as ex:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Exception: Balancing lists. -")
			print(ex)
		return(final_peer_download_lists)

	def verifyBucketList(self, bucket_list:list) -> bool:
		'''
		Verify each item is in the following format [<full_path_to_a_bucket_file>, <file_size_bytes>]
		'''
		if bucket_list:
			for b in bucket_list:
				if not isinstance(b[0], str) and isinstance(b[1], int):
					print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Bucket list format example found was " + str(b) +" and verification failed, cannot continue, ensure format starts with proper <str>, <bytes> -")
					sys.exit()
			return(True)
		else:
			print("- BUCKETEER(" + str(sys._getframe().f_lineno) +"): Master List of lists was empty, cannot continue -")
			return(False)

	def start(self, bucket_list=[], replace = True):
		'''
		After creating the class in external script for this module, you can start sorting the list by calling this function
		Any buckets here will be ADDED to buckets you added by default!
		Set replace_list to True to replace the original list with the one entered at startup
		'''
		if bucket_list:
			if replace:
				self.list_of_bucket_list_details = bucket_list
			else:
				self.list_of_bucket_list_details.extend(bucket_list)
			if self.verifyBucketList(self.list_of_bucket_list_details):
				idx_cluster_peers = self.getPeerGUIDS()
				bucket_info_tuples_list = self.splitBucketDetails()
				if bucket_info_tuples_list:
						separated_master_bucket_tuple_list = self.organizeMasterListByStateIndexDB(bucket_info_tuples_list)
						print("hello 12")
						self.final_peer_download_lists = self.divideMasterBucketListAmongstPeers(idx_cluster_peers, separated_master_bucket_tuple_list)
						print("hello 13")
						for idx, p in enumerate(idx_cluster_peers):
							if p == self.my_guid:
								self.this_peer_download_list = self.final_peer_download_lists[idx]
								self.this_peer_index = idx
								break
						return(True)
			else:
				return(False)

### RUNTIME ###########################################
if __name__ == "__main__":
	azure_buckets = Bucketeer('idx_bucket_sorter', '/opt/splunk/', sp_uname='admin', sp_pword='5up3rn0va', list_of_bucket_list_details=[["cold/barracuda/frozendb/db_1625059460_1632148318_93_6CE6A7CF-AD0F-423B-90DD-D05AF9A3C87C/rawdata/journal.gz", 352252352], ["cold/barracuda/frozendb/db_1625059460_1632148318_93_6CE6A7CF-AD0F-423B-90DD-D05AF9A3C87C/rawdata/journal.gz", 353536], ["cold/barracuda/frozendb/db_1625059460_1632148318_93_6CE6A7CF-AD0F-423B-90DD-D05AF9A3C87C/rawdata/journal.gz", 333566663636], ["cold/mcafee/frozendb/db_1625059460_1632148318_93_6CE6A7CF-AD0F-423B-90DD-D05AF9A3C87C/rawdata/journal.gz", 333566663636], ["cold/mcafee/frozendb/db_1625059460_1632148318_93_6CE6A7CF-AD0F-423B-90DD-D05AF9A3C87C/rawdata/journal.gz", 353536] ])
	if not azure_buckets.standalone:
		idx_cluster_peers = azure_buckets.getPeerGUIDS()
	bucket_info_tuples_list = azure_buckets.splitBucketDetails()
	if bucket_info_tuples_list:
		separated_master_bucket_tuple_list = azure_buckets.organizeMasterListByStateIndexDB(bucket_info_tuples_list)
		final_peer_download_lists = azure_buckets.divideMasterBucketListAmongstPeers(idx_cluster_peers, separated_master_bucket_tuple_list)