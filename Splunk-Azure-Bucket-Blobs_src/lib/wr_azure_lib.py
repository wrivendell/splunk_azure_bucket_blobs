##############################################################################################################
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
#
#   This is a lib to easily access Azure Blob Storage items
#   Requires: pip install azure-storage-blob if running the py script natively - use AIO for no dependencies
##############################################################################################################

### IMPORTS ###########################################
import os, datetime, sys, re
from time import time

from . import wr_logging as log

from pathlib import Path
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

### CLASSES ###########################################

class BlobService():
	'''
	Wrapper class to call Azure Blobs and Containers
	Create a variable once to hold this object, specify connection string)

	e.g. 
		from lib import wr_azure_lib as wazure
		blob_service = wazure.BlobService('DefaultEndpointsProtocol=https;AccountName=splunkcloudblobdownload;AccountKey=YuAaifdfsdfsdfsdfsfsfsdfsHeyZ+jXfKc2zeDu/tTvUE1mDh9dfwf3f323f23r2f2f23f2f2f2m80sA2BwBDXA==;EndpointSuffix=core.windows.net')

	From there you can call the various functions in here, e.g. 
		container_name_list = blob_service.getContainers()

	'''

	def __init__(self, connect_str: str):
		global blob_service_client
		self.connect_str = connect_str
		try:
			print("- WAZURE(" + str(sys._getframe().f_lineno) +"): Azure Blob Storage v" + __version__ + "-")
			blob_service_client = BlobServiceClient.from_connection_string(self.connect_str)
		except Exception as ex:
			print("- WAZURE(" + str(sys._getframe().f_lineno) +"): Exception: -")
			print(ex)
		self.log_file = log.LogFile('wazure.log', log_folder='./logs/', remove_old_logs=True, log_level=3, log_retention_days=10)

	def isInList(self, string_to_test:str, list_to_check_against:list, equals_or_contains=True, string_in_list_or_items_in_list_in_string=True) -> bool:
		'''
		Simple function to check if a string exists in a list either by exact match or contains
		string_in_list_or_items_in_list_in_string when False will check if any of the items in the LIST exists in or equal the string 
		'''
		for item in list_to_check_against:
			if equals_or_contains:
				if string_in_list_or_items_in_list_in_string:
					if string_to_test == item:
						return(True)
					else:
						continue
				else:
					if item == string_to_test:
						return(True)
					else:
						continue
			else:
				if string_in_list_or_items_in_list_in_string:
					if string_to_test in item:
						return(True)
					else:
						continue
				else:
					if item in string_to_test:
						return(True)
					else:
						continue					
		return(False)


	def formatAzureSpecialChars(self, k, v) -> list:
		'''
		Returned format of blob info appears to be in dict format.
		However they are returned as custom Azure classes. Some fields are not formatted
		for proper JSON output. This runs each kv pair through looking for non-JSON
		compatible value formats and "fixes" them before adding them to the final dict list.

		Can be removed if not needed a JSON output later.
		'''
		if k == 'encryption_scope' or k == 'blob_type':
			v = str(v)
			v.replace("'", "")
			v = str(v)
			return(True, v)
		elif isinstance(v, datetime.datetime):
			v = v.strftime("%Y-%m-%d_T%H-%M-%S")
			return(True, v)
		elif k == 'content_md5':
			v = v.hex()
			return(True, v)
		else:
			return(False, v)

	def getContainers(self, names_only=False) -> list:
		'''
		Get a list of containers that the specified connection string has access too.
		Default is a list of dicts, each containing all info of a container.
		Optional names_only=True will return a simple list of container names.
		'''
		try:
			tmp_container_list = []
			containers = list( blob_service_client.list_containers() )
			for container in containers:
				if names_only:
					tmp_container_list.append( container['name'] )
				else:
					# azure leaves datetime native objects in raw data converting them to date formats json can understand
					d_container = dict(container)
					tmp_dict = {}
					for k,v in d_container.items():
						try:
							tmp_sub_dict = {}
							for sub_k, sub_v in v.items():
								need_parse = self.formatAzureSpecialChars(sub_k, sub_v)
								if need_parse[0]:
									tmp_sub_dict[sub_k] = need_parse[1]
								else:
									tmp_sub_dict[sub_k] = sub_v
							v = tmp_sub_dict
						except:
							need_parse = self.formatAzureSpecialChars(k, v)
							if need_parse[0]:
								v = need_parse[1]
						tmp_dict[k] = v
					tmp_container_list.append(tmp_dict)
			return(tmp_container_list)
		except Exception as ex:
			print("- WAZURE(" + str(sys._getframe().f_lineno) +"): Exception: -")
			print(ex)

	def getBlobsByContainer(self, container_name, blob_search_list=[], names_only=False) -> list:
		'''
		Get all blobs in a specified container. Returns a list contianing dicts.
		Default is one dict per blob with all info for that blob file.
		Optional names_only=True will return a simple list of blob file names.
		'''
		try:
			tmp_blob_list = []
			container_client = blob_service_client.get_container_client( (container_name) )
			blob_list = container_client.list_blobs()
#			tmp_blist = list(tmp_blist)
#			print("- WAZURE(" + str(sys._getframe().f_lineno) +"): Amount of BLOBS: " + str(len(tmp_blist)) + " -")
			counter = 0
			for blob in blob_list:
				counter += 1
				if counter >= 1001:
					print("breaking at 1000 for testing")
					break
				if names_only:
					tmp_blob_list.append( blob['name'] )
				else:
					print("- WAZURE(" + str(sys._getframe().f_lineno) +"): Processing BLOB: " + str(blob['name']) + " -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + ") Processing BLOB: " + str(blob['name']) ] )
					if not len(str(blob['name']).rsplit('.', 1)) > 1:
						print("- WAZURE(" + str(sys._getframe().f_lineno) +"): Found no file, just path in BLOB: " + str(blob['name']) + " Skipping. -")
						self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + ") Found no file, just path in BLOB: " + str(blob['name']) + " Skipping." ] )
						continue
					else:
						found = True
						if blob_search_list:
							found = False
							for i in blob_search_list:
								if not i in blob['name']:
									continue
								else:
									found = True
									break
					if not found:
						print("- WAZURE(" + str(sys._getframe().f_lineno) +"): BLOB: " + str(blob['name']) + " Not in search list, skipping. -")
						self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): BLOB: " + str(blob['name']) + " Not in search list, skipping." ] )
						continue
					print("- WAZURE(" + str(sys._getframe().f_lineno) +"): BLOB: " + str(blob['name']) + " ADDED. -")
					self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + ") BLOB: " + str(blob['name']) + " ADDED." ] )						
					d_blob = dict(blob)
					tmp_dict = {}
					for k,v in d_blob.items():
						try:
							tmp_sub_dict = {}
							for sub_k, sub_v in v.items():
								need_parse = self.formatAzureSpecialChars(sub_k, sub_v)
								if need_parse[0]:
									tmp_sub_dict[sub_k] = need_parse[1]
								else:
									tmp_sub_dict[sub_k] = sub_v
							v = tmp_sub_dict
						except:
							need_parse = self.formatAzureSpecialChars(k, v)
							if need_parse[0]:
								v = need_parse[1]
						tmp_dict[k] = v
					tmp_blob_list.append( dict(tmp_dict) )
			return(tmp_blob_list)
		except Exception as ex:
			print("- WAZURE(" + str(sys._getframe().f_lineno) +"): Exception: " + str(blob['name'] + " -"))
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + ") Exception: " + str(blob['name']) ] ) 
			print(ex)

	def getAllBlobsByContainers(self, container_search_list=[], blob_search_list=[]) -> list:
		'''
		Probes all available containers to the provided connectionstring (access to) and gets all container names.
		Probes each container for blobs contained and writes all out to a list of dicts. One dict per container
		containing container info plus an embedded list of dicts continaing all blobs info
		'''
		try:
			tmp_container_blob_dict_list = []
			all_containers_dict_list = self.getContainers()
			for container in all_containers_dict_list:
				print("\n\n\n- WAZURE(" + str(sys._getframe().f_lineno) +"): Processing CONTAINER: " + container['name'] + " -")
				found = True
				if container_search_list:
					found = False
					for i in container_search_list:
						if not i in container['name']:
							continue
						else:
							found = True
							break
				if not found:
					print("- WAZURE(" + str(sys._getframe().f_lineno) +"): " + str(container['name']) + " Not in list, skipping. -")
					self.log_file.writeLinesToFile( ["(" + str(sys._getframe().f_lineno) + ") " + str(container['name']) + " Not in list, skipping."] )
					continue
				else:
					print("- WAZURE(" + str(sys._getframe().f_lineno) +"): " + str(container['name']) + " Added -")
					self.log_file.writeLinesToFile( ["(" + str(sys._getframe().f_lineno) + ") " + str(container['name']) + " Added." ] )
				tmp_blobs_dict_list = self.getBlobsByContainer(container['name'], blob_search_list)
				tmp_container_dict = container
				tmp_container_dict['blobs'] = (tmp_blobs_dict_list)
				tmp_container_blob_dict_list.append(tmp_container_dict)
			return(tmp_container_blob_dict_list)
		except Exception as ex:
			print("- WAZURE(" + str(sys._getframe().f_lineno) +"): Exception: -")
			print(ex)
	
	def downloadBlobByName(self, blob_name:str, expected_blob_size:int, container_name:str, dest_download_loc_root='./blob_downloads/', replace_file_name="", bypass_size_compare=False, timeout=5000) -> list:
		'''
		Downloads a specified blob file from a container locally to wherever this script (by default)
		is run from in the subfolder ./blob_downloads/<container_name>/<rest of path from Azure blob name>

		expected_blob_size is in bytes (as per return from Azure)

		Replace filename will remove the blob name completely so reconstruct it however you want! Leave alone to use blob name as default

		Checks the expected blob size (grabbed from the RAW file info data)
		against the actual downloaded size to confirm completed succesfully.
		Returns list (bool, int) = (success, size)
		Optional: bypass_size_compare=True will return true and just assume download completed ok
		Optional: timeout=50000 can be set to lesser if desired. Azure docs doesn't actually say if this is a kill switch
			for active downloads or a fail after no transfer is done... would hate to kill a legit large download in progress
			so its set high for now. Odds are if you're transferring TBs of data with this in multiple processes, it's ok to
			leave this at 5000 seconds
		'''
		if 'win' in sys.platform:
			slash_direction = "\\"
			dest_download_loc_root = dest_download_loc_root.replace('/','\\')
			if dest_download_loc_root.endswith('\\'):
				download_dir = dest_download_loc_root + (container_name) + '\\'
			else:
				download_dir = dest_download_loc_root + '\\' + (container_name) + '\\'
		else:
			slash_direction = "/"
			if dest_download_loc_root.endswith('/'):
				download_dir = (dest_download_loc_root) + (container_name) + '/'
			else:
				download_dir = (dest_download_loc_root) + '/' + (container_name) + '/'
		if replace_file_name:
			filename_full = (download_dir) + str(replace_file_name) # path to full file
		else:
			filename_full = (download_dir) + str(blob_name) # path to full file
		file_path = str(Path(filename_full).parent) # path to directory holding the final file.whatever
		if re.search('^(.+?)(?:[a-zA-Z0-9.,$;])', filename_full, re.IGNORECASE).group(1):
			file_path = re.search('^(.+?)(?:[a-zA-Z0-9.,$;])', filename_full, re.IGNORECASE).group(1) + file_path
		file_path = file_path + slash_direction
		try:
			os.makedirs(os.path.dirname(file_path), exist_ok=True)
		except Exception as ex:
			print("- WAZURE(" + str(sys._getframe().f_lineno) +"): Exception: Error making/accessing download dir -")
			self.log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + ") Exception: Error making/accessing download dir." ])
			print(ex)
		blob = BlobClient.from_connection_string(conn_str=(self.connect_str), container_name=(container_name), blob_name=(blob_name))
		with open( (filename_full), "wb") as my_blob:
			blob_data = blob.download_blob(validate_content=True, max_concurrency=5, timeout=(timeout))
			downloaded_blob_size = (blob_data.readinto(my_blob))
			if bypass_size_compare:
				return(True, downloaded_blob_size * 1000000, '(MB)')
			else:
				if (downloaded_blob_size) == (expected_blob_size):
					return(True, downloaded_blob_size * 1000000, '(MB)')
				else:
					return(False, downloaded_blob_size * 1000000, '(MB)')

	def downloadAllBlobsFromContainers(self, container_names_list=[], blob_name_ignore_list=[], ignore_list_equals_or_contains=False):
		'''
		Download all blobs in specified container, see comment on downloadBlobByName() function in this class 
		for details on individual blob downloads.
		Optional: blob_name_ignore_list=['someword','another/word'] - list of words to ignore and NOT download if found in blob_name
		Optional: ignore_list_equals_or_contains - False will attempt a contains match of each word, True will need to match the name exactly

		If you want to use this is in a process queue you should write a customized version of this and use downloadBlobByName()
			here as the worker target.

		container_names should be a list of container names to fetch blobs from, empty will fetch from all available containers.
		'''
		all_blobs_by_containers_dict_list = self.getAllBlobsByContainers()
		for container in all_blobs_by_containers_dict_list:
			if container_names_list:
				if not self.isInList(container, container_names_list):
					continue
			for blob in container['blobs']:
				if len(blob_name_ignore_list) > 0:
					if self.isInList(blob['name'], blob_name_ignore_list, ignore_list_equals_or_contains):
						continue
				else:
					return( self.downloadBlobByName( str(container['name']), str(blob['name']), int(blob['size']) ) )




# random examples
#all_blobs_by_container_name = blob_service.getAllBlobsByContainers()
#downloaded_size = blob_service.downloadBlobByName('dfe-test', 'frozendata/barracuda/frozendb/db_1620418303_1620278975_33_6EF43E39-5DA7-4C58-A164-C3D72E145A30/rawdata/journal.gz', 1757321)
#print(downloaded_size)
#blob_service.downloadAllBlobsFromContainers()