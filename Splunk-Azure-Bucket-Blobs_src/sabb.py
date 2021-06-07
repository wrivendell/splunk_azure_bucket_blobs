##############################################################################################################
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
##############################################################################################################

### Imports ###########################################
import datetime, time, threading, sys, os

from lib import wr_arguments as arguments
from lib import wr_thread_queue as wrq
from lib import wr_logging as log
from lib import wr_azure_lib as wazure
from lib import wr_splunk_bucket_distributor as buckets

# Clear Function First
def clearConsole():
	if 'win' in sys.platform:
		os.system('cls')
	else:
		os.system('clear')

### Globals ###########################################
# log files
clearConsole()
if arguments.args.write_out_full_list_only:
	main_log = 'azure_blob_bucket_download_WOFLO.log'
	main_report_csv = 'azure_blob_status_report_WOFLO.csv'

else:
	main_log = 'azure_blob_bucket_download.log'
	main_report_csv = 'azure_blob_status_report.csv'

# create log handlers
log_file = log.LogFile(main_log, remove_old_logs=True, log_level=arguments.args.log_level, log_retention_days=0, debug=arguments.args.debug_modules)
log_csv = log.CSVFile(main_report_csv, remove_old_logs=False, log_retention_days=20, debug=arguments.args.debug_modules)

# Print Console Info
print("\n")
print("- SABB(" + str(sys._getframe().f_lineno) +"): --- Splunk Azure Blob Bucket Downloader ---- \n")
print("- SABB(" + str(sys._getframe().f_lineno) +"):  Main Log Created at: ./logs/" + (main_log) + " -")
print("- SABB(" + str(sys._getframe().f_lineno) +"):  Main CSV Status Report Created at: ./logs/" + (main_report_csv) + " -")
print("\n")

# service class for Azure (wazure)
blob_service = wazure.BlobService((arguments.args.connect_string)) # used to make requests to Azure Blobs
log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Blob interactive service class created: blob_service"])
master_bucket_download_list = []

# service class for Bucket sorter
if not arguments.args.standalone:
	azure_bucket_sorter = buckets.Bucketeer('idx_bucket_sorter', 
											 sp_home=arguments.args.splunk_home, 
											 sp_uname=arguments.args.splunk_username,
											 sp_pword=arguments.args.splunk_password, 
											 sp_idx_cluster_master_uri=arguments.args.cluster_master, 
											 port=arguments.args.cluster_master_port,
											 size_error_margin=arguments.args.size_error_margin,
											 debug=arguments.args.debug_modules)
# Print Console Info
if arguments.args.detailed_output:
	print("- SABB(" + str(sys._getframe().f_lineno) +"):  Blob interactive service class created: blob_service" + " -")
	print("- SABB(" + str(sys._getframe().f_lineno) +"):  Bucket Sorter class created: idx_bucket_sorter" + " -")
log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Bucket Sorter class created: idx_bucket_sorter"])

# create queues
if not arguments.args.write_out_full_list_only:
	wrq_download = wrq.Queue('blob_downloader', (arguments.args.thread_count), debug=arguments.args.debug_modules) # downloads blobs from Azure
else:
	print("- SABB(" + str(sys._getframe().f_lineno) +"):  No DOWNLOAD queue created as Writing out Download List only (WOFLO) is on: -")
wrq_logging = wrq.Queue('parent_logging', 1, debug=arguments.args.debug_modules) # queues log writes to avoid "file already open" type errors
wrq_csv_report = wrq.Queue('parent_csv_reporter', 1, debug=arguments.args.debug_modules) # queues csv writes to master status report

list_index = 0 # starting point for checking finished job queue when updating CSV

# Print Console Info
if arguments.args.detailed_output:
	print("- SABB(" + str(sys._getframe().f_lineno) +"):  Processing Queue Created: -")
	if not arguments.args.write_out_full_list_only:
		print("   Queue class: wrq_download")
	print("   Queue name: blob_downloader")
	print("\n")
log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Processing Queue Created:"])
if not arguments.args.write_out_full_list_only:
	log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Queue class: wrq_download"])
log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Queue name: blob_downloader"])
if arguments.args.detailed_output:
	print("\n")
	print("- SABB(" + str(sys._getframe().f_lineno) +"):  Processing Queue Created: -")
	print("   Queue class: wrq_logging")
	print("   Queue name: parent_logging")
	print("\n")
log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Processing Queue Created:"])
log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Queue class: wrq_logging"])
log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Queue name: parent_logging"])

### Functions ###########################################
def currentDate(include_time=False, raw_or_str=False):
	if raw_or_str:
		return(datetime.datetime.now())
	else:
		if include_time:
			return(datetime.datetime.now().strftime('%Y_%m_%d_T%H_%M_%S.%f'))
		else:
			return(datetime.datetime.now().strftime('%Y_%m_%d'))

def checkAlreadyDownloaded(blob_name) -> bool:
	'''
	Returns True or False, True if already downloaded.
	The return from the csv list should only contain one value so access with <returned>[1][0]
	If it contains more, there are dupes in your CSV. That return is a set with a bool, list
	'''
	check_completed = log_csv.getValueByHeaders('Blob_Path_Name', blob_name, 'Download_Complete')
	if check_completed[0]:
		if check_completed[1]:
			if check_completed[1][0] == 'SUCCESS':
				return(True)
			else:
				return(False)
		else:
			return(False)
	else:
		return(False)

def appendGUIDCheck(bucket_detail_list:list) -> set:
	'''
	If needing a name change,
	Returns a  True, <replacement list with the new bucket download to name as the last element>
	Otherwise a False, ""
	'''
	if bucket_detail_list[4]: # if standalone is true
		try:
			tmp_split = bucket_detail_list[0].split('_' + str(bucket_detail_list[5])) # split original path at the _bucketID
			new_bucket_name = str(tmp_split[0]) + "_" + str(bucket_detail_list[5]) + "_" + str(azure_bucket_sorter.my_guid) + str(tmp_split[1])
			print("- SABB(" + str(sys._getframe().f_lineno) +"): Standalone bucket going to cluster. Appending GUID. New bucket path will be: " + new_bucket_name +"-")
			log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): Standalone bucket going to cluster. Appending GUID. New bucket path will be: " + new_bucket_name])
			del bucket_detail_list[-1]
			del bucket_detail_list[-1]
			bucket_detail_list.append( new_bucket_name )
			return(True, bucket_detail_list)
		except Exception as ex:
			print(ex)
			print("- SABB(" + str(sys._getframe().f_lineno) +"): FAILED appending GUID to stnadalone bucket -")
			log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): FAILED appending GUID to stnadalone bucket."])
	else:
		return(False, "")

# get list of all blobs to download
def makeBlobDownloadList(container_names_to_search_list=[], 
						 container_names_to_ignore_list=[], 
						 blob_names_to_search_list=[], 
						 blob_names_to_ignore_list=[], 
						 container_names_search_list_equals_or_contains=False, 
						 container_names_ignore_list_equals_or_contains=False, 
						 blob_names_search_list_equals_or_contains=False, 
						 blob_names_ignore_list_equals_or_contains=False, 
						 dest_download_loc_root='./blob_downloads'):
	global master_bucket_download_list
	'''
	Generates list items to add to master_bucket_download_list, each item looks like:
	[ <blob_name>, <blob_size>, <container_name>, <download_dest> ]
	You can specify which containers in a list, to search in as well as what blob names in a list to search fore
	Additionally you can enter container and blob names to ignore. Ignores happen AFTER the search for happens... which further narrows the found list
	i.e. search for containers like ["container_name_delta_*", "container_name_alpha_*"] and then ignore ["container_name_delta_3"] 
	Container and blob names can be exact matches or specified contains(False)
	Leaving those lists blank, return all blobs in all containers by default
	'''
	print("- SABB(" + str(sys._getframe().f_lineno) +"):  Attempting to create master blob download list, this could take awhile. -")
	log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): Attempting to create master blob download list, this could take awhile."])
	if not arguments.args.list_create_output:
		print("- SABB(" + str(sys._getframe().f_lineno) +"): You could set -lco to True for more entertaining feedback while you wait. -")
	time.sleep(3)
	try:
		tmp_master_list_log_lines = []
		all_blobs_by_containers_dict_list = blob_service.getAllBlobsByContainers(container_names_to_search_list, blob_names_to_search_list)
		# FEED BACK FOR USER
		# Container filters
		print("- SABB(" + str(sys._getframe().f_lineno) +"): All blobs from all containers found and listed -")
		log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): All blobs from all containers found and listed"])
		print("\n")
		print("- SABB(" + str(sys._getframe().f_lineno) +"): Filtering list based on the following filters -")

		# Container filters
		if len(container_names_to_search_list) > 0:
			log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): Filtering list based on the following filters"])
			if arguments.args.container_search_list_type:
				cfilter_type = "EXACTLY MATCHES"
			else:
				cfilter_type = "CONTAINS"
			for filter in arguments.args.container_search_list:
				print("- SABB(" + str(sys._getframe().f_lineno) +"): Will ONLY download blobs found where container " + cfilter_type + ": " + filter + " -")
				log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):  Will ONLY download blobs found where container " + cfilter_type + ": " + filter])
		if len(container_names_to_ignore_list) > 0:
			if arguments.args.container_ignore_list_type:
				cfilter_type = "EXACTLY MATCHES"
			else:
				cfilter_type = "CONTAINS"
			for filter in arguments.args.container_ignore_list:
				print("- SABB(" + str(sys._getframe().f_lineno) +"): Will NOT download blobs found where container " + cfilter_type + ": " + filter + " -")
				log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):  Will NOT download blobs found where container " + cfilter_type + ": " + filter])
	
		# Blob filters
		if len(blob_names_to_search_list) > 0:
			if arguments.args.blob_search_list_type:
				cfilter_type = "EXACTLY MATCHES"
			else:
				cfilter_type = "CONTAINS"
			for filter in arguments.args.blob_search_list:
				print("- SABB(" + str(sys._getframe().f_lineno) +"): Will ONLY download blobs found where blob_name " + cfilter_type + ": " + filter + " -")
				log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):  Will ONLY download blobs found where blob_name " + cfilter_type + ": " + filter])
		print("\n")
		if len(blob_names_to_ignore_list) > 0:
				if arguments.args.blob_ignore_list_type:
					cfilter_type = "EXACTLY MATCHES"
				else:
					cfilter_type = "CONTAINS"
				for filter in arguments.args.blob_ignore_list:
					print("- SABB(" + str(sys._getframe().f_lineno) +"): Will NOT download blobs found where blob_name " + cfilter_type + ": " + filter + " -")
					log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): Will NOT download blobs found where blob_name " + cfilter_type + ": " + filter])
		
		# Actual running
		if not all_blobs_by_containers_dict_list:
			print("- SABB(" + str(sys._getframe().f_lineno) +"): No Containers Found -")
			return(False)
		for container in all_blobs_by_containers_dict_list:
			if arguments.args.list_create_output:
				print("\n")
				print("- SABB(" + str(sys._getframe().f_lineno) +"): Now processing container: " + container['name'] + " -")
				print("\n")
			log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): Now processing container: " + container['name'] ])
			if len(container_names_to_search_list) > 0:
				if not blob_service.isInList(container['name'], container_names_to_search_list, container_names_search_list_equals_or_contains, False):
					log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): Skipping CONTAINER since not in INCLUDE list: " + container['name'] ])
					if arguments.args.list_create_output:
						print("- SABB(" + str(sys._getframe().f_lineno) +"): Skipping CONTAINER since not in INCLUDE list: " + container['name'] + " -")
					continue
			if len(container_names_to_ignore_list) > 0:
				if blob_service.isInList(container['name'], container_names_to_ignore_list, container_names_ignore_list_equals_or_contains, False):
					log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): Skipping CONTAINER based on EXCLUDE list: " + container['name'] ])
					if arguments.args.list_create_output:
						print("- SABB(" + str(sys._getframe().f_lineno) +"): Skipping CONTAINER based on EXCLUDE list: " + container['name'] + " -")
					continue
			for blob in container['blobs']:
				if len(blob_names_to_search_list) > 0:
					if not blob_service.isInList(blob['name'], blob_names_to_search_list, blob_names_search_list_equals_or_contains, False):
						log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): Skipping BLOB since not in INCLUDE list: " + blob['name'] ])
						if arguments.args.list_create_output:
							print("- SABB(" + str(sys._getframe().f_lineno) +"): Skipping BLOB since not in INCLUDE list: " + blob['name'] + " -")
						continue
				if len(blob_names_to_ignore_list) > 0:
					if blob_service.isInList(blob['name'], blob_names_to_ignore_list, blob_names_ignore_list_equals_or_contains, False):
						log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"): Skipping BLOB based on EXCLUDE list: " + blob['name'] ])
						if arguments.args.list_create_output:
							print("- SABB(" + str(sys._getframe().f_lineno) +"): Skipping BLOB based on EXCLUDE list: " + blob['name'] + " -")
						continue
				if arguments.args.standalone:
					# already downloaded check
					if checkAlreadyDownloaded(str(blob['name'])):
						tmp_master_list_log_lines.append('File appears to already be downloaded, skipping: ' + str(blob['name']) )
						if arguments.args.list_create_output:
							print("- SABB(" + str(sys._getframe().f_lineno) +"): File appears to already be downloaded, skipping: " + str(blob['name']) + " -")
						continue
				tmp_list = [ str(blob['name']), int(blob['size']), str(container['name']), str(dest_download_loc_root) ]
				if arguments.args.list_create_output:
					print("- SABB(" + str(sys._getframe().f_lineno) +"): This blob is being added to the list: " + blob['name'] + " -")
				master_bucket_download_list.append(tmp_list)
		if not arguments.args.standalone:
			# send to bucket sorter for idx cluster distribution
			if not azure_bucket_sorter.start(master_bucket_download_list):
				print("- SABB(" + str(sys._getframe().f_lineno) +"):  FAILED to create sorted peer list, exiting. -")
				sys.exit()
			else:
				# master_bucket_download_list_orig = master_bucket_download_list # uncomment if ever wanting to keep the master list for whatever reason
				master_bucket_download_list = []
				tmp_master_list_log_lines =[]
				if not arguments.args.write_out_full_list_only:
					for i in azure_bucket_sorter.this_peer_download_list:
						if checkAlreadyDownloaded(str(i[7])):
							tmp_master_list_log_lines.append('File appears to already be downloaded, skipping: ' + str(i[7]) )
							if arguments.args.list_create_output:
								print("\n- SABB(" + str(sys._getframe().f_lineno) +"): File appears to already be downloaded, skipping: " + str(i[7]) + " -\n")
							continue
						else:
							# check and see if the bucket came from a standalone and needs a GUID appeneded
							standalone_rename_check = appendGUIDCheck([ i[7], i[6], i[11], i[12], i[4], i[2] ])
							if standalone_rename_check[0]:
								master_bucket_download_list.append(standalone_rename_check[1])
							else:
								master_bucket_download_list.append( [ i[7], i[6], i[11], i[12] ] )
		if tmp_master_list_log_lines:
			wrq_logging.add(log_file.writeLinesToFile, [[(tmp_master_list_log_lines), 3]])
	except Exception as ex:
		print("- SABB(" + str(sys._getframe().f_lineno) +"):  Exception: -")
		print(ex)
		print("- SABB(" + str(sys._getframe().f_lineno) +"):  FAILED to create master blob download list, exiting. -")
		log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):FAILED to create master blob download list, exiting."])
		sys.exit()

# compare a byte size to a file byte size
def compareDownloadSize(expected_size:int, full_path_to_file:str):
	'''
	Returns a set, (True/False, downloaded_size)
	'''
	try:
		downloaded_size = os.path.getsize((full_path_to_file))
		if int(downloaded_size) == int(expected_size):
			return(True, downloaded_size)
		else:
			print("- SABB(" + str(sys._getframe().f_lineno) +"): File Download: FAILED - " + full_path_to_file + " -")
			return(False, downloaded_size)
	except Exception as ex:
		print("- SABB(" + str(sys._getframe().f_lineno) +"):  Exception: -")
		print(ex)
		print("- SABB(" + str(sys._getframe().f_lineno) +"): Verify File Download: FAILED - " + full_path_to_file + " -")
		log_line=['Verify File Download: FAILED - ' + full_path_to_file, 3]
		tmp_log_list = []
		tmp_log_list.append(log_line)
		wrq_logging.add(log_file.writeLinesToFile, (tmp_log_list))
		return(False, 0)

def updateCompletedWRQDownloadJobs():
	'''
	Get a list of rows to be added to the master file
	'''
	global list_index
	global run_me
	while run_me:
		time.sleep(10)
		if wrq_download.inactive_timeout_counter <= 0:
			break
		if not run_me:
			break
		try:
			if len(wrq_download.jobs_completed) > 0:
				last_index = len(wrq_download.jobs_completed)
				tmp_log_lines = []
				tmp_log_lines_jobs = []
				if arguments.args.detailed_output:
					print("\n")
					print("- SABB(" + str(sys._getframe().f_lineno) +"):  Checking for latest completed download jobs -")
				tmp_log_lines.append('Checking for latest completed download jobs')
				for jc in wrq_download.jobs_completed[list_index:last_index]:
					if arguments.args.detailed_output:
						print("   - Found newly completed download job: " + str(jc[0].name))
					tmp_log_lines.append('Found newly completed download job: ' + str(jc[0].name))

				rows_list = []
				print("\n")
				tmp_log_dl_list = []
				for j in wrq_download.jobs_completed[list_index:last_index]:
					if arguments.args.detailed_output:
						print("   - Adding newly completed download job to status report: " + str(j[0].name))
					tmp_log_lines_jobs.append('Adding newly completed download job to status report: ' + str(j[0].name) )
					tmp_row = []
					command_args_list = j[1].replace("'","").replace('"',"").replace("[","").replace("]","").replace(" ","")
					command_args_list = list(command_args_list.split(","))
					file_verify = compareDownloadSize( command_args_list[1], str(command_args_list[3]) + str(command_args_list[2]) + '/' + str(command_args_list[0]) )
					if file_verify[0]:
						status_string = 'SUCCESS'
						tmp_log_dl_list.append('File Download: SUCCESS - ' + str(command_args_list[3]) + str(command_args_list[2]) + '/' + str(command_args_list[0]) )
					else:
						status_string = 'FAILED'
						tmp_log_dl_list.append('File Download: FAILED - ' + str(command_args_list[3]) + str(command_args_list[2]) + '/' + str(command_args_list[0]) )
					# 0 = blob name - 1 = bytes size - 2 = container - 3 = downloaded to path
					tmp_row = [command_args_list[2], command_args_list[3], command_args_list[0], int(command_args_list[1]) / 1000000, file_verify[1], (status_string), currentDate(include_time=True), j[0].name, j[0].ident]
					rows_list.append(tmp_row)
				if run_me:
					wrq_csv_report.add(log_csv.writeLinesToCSV, [[(rows_list), ['Container_Name', 'Downloaded_To', 'Blob_Path_Name', 'Expected_Blob_Size_MB', 'Downloaded_Blob_Size_MB', 'Download_Complete', 'Download_Completed_Date', 'Thread_Name', 'Thread_ID']]])
					wrq_logging.add(log_file.writeLinesToFile, [[(tmp_log_lines)]])
					wrq_logging.add(log_file.writeLinesToFile, [[(tmp_log_lines_jobs), 3]])
					wrq_logging.add(log_file.writeLinesToFile, [[(tmp_log_dl_list), 3]])
				list_index = last_index
		except Exception as ex:
			print("- SABB(" + str(sys._getframe().f_lineno) +"):  Exception: -")
			print(ex)
			print("- SABB(" + str(sys._getframe().f_lineno) +"):  FAILED while attempting to get jobs_completed info -")
			wrq_logging.add(log_file.writeLinesToFile, [[["FAILED while attempting to get jobs_completed info."]]])
			sys.exit(1)
	else:
		if arguments.args.detailed_output:
			print("- SABB(" + str(sys._getframe().f_lineno) +"):  updateCompletedWRQDownloadJobs is completed, thread should stop now. -")

	# fun icon for show only
def spinner(counter):
	chars = ['|', '/', '--', '\\', '|', '/', '--', '\\']
	try:
		return(chars[counter])
	except:
		return(chars[0])

	# this is the last function running in the PARENT - This should be the last of ALL threads to exit
def timeAndCompletionChecker():
	counter = -1
	global run_me
	while run_me:
			if len(wrq_download.jobs_active) <= 0 and len(wrq_logging.jobs_active) <= 0 and len(wrq_csv_report.jobs_active) > 0:
				run_me = False # THIS STOPS THE LAST CSV REPORTER LOOP! DONT DELETE
			if arguments.args.detailed_output:
				time.sleep(3)
				print("\n")
			else:
				time.sleep(1)
			check_time = currentDate(raw_or_str=True)
			elapsed_time = check_time - start_time
			completed_downloads = len(wrq_download.jobs_completed)
			percent_complete = round( float( completed_downloads / start_length_of_download_list ) * 100, 2 )
			bar_amount = int( (22 / 100) * percent_complete )
			bar_chars = '>' * bar_amount
			spacers_amount = 22 - bar_amount
			spacer_chars = ' ' * spacers_amount
			if percent_complete >= 100:
				middle_spinner = spinner(counter)
			else:
				middle_spinner = '|'
			clearConsole()
			print("=========================")
			print("Splunk Azure Bucket Blobs")
			print("=======================" + spinner(counter))
			print('|' + bar_chars + spacer_chars + middle_spinner)
			if percent_complete >=50:
				print("=======================" + spinner(counter))
			else:
				print("=======================|")
			print("\n")
			print("- Start Time: " + str(start_time_str))
			print("- Elapsed Time: " + str(elapsed_time))
			print("- Completed: " + str(percent_complete) + "%")
			print("\n")
			print("WRQ_Downloads------------")
			print("- Downloads Active: " + str(len(wrq_download.jobs_active)))
			print("- Downloads Completed: " + str(len(wrq_download.jobs_completed)))
			print("- Downloads Waiting: " + str(len(wrq_download.jobs_waiting)))
			print("- Average Download Time(min): " + str( round(wrq_download.average_job_time, 2) ) )
			print("- Estimated Finish Time(min): " + str( round(wrq_download.estimated_finish_time, 2) ) )
			print("-------------------------")
			print("\n")
			print("WRQ_Logging--------------")
			print("- Log Jobs Active: " + str(len(wrq_logging.jobs_active)))
			print("- Log Jobs Completed: " + str(len(wrq_logging.jobs_completed)))
			print("- Log Jobs Waiting: " + str(len(wrq_logging.jobs_waiting)))
			print("-------------------------")
			print("\n")
			print("WRQ_CSV_Report ----------")
			print("- CSV Jobs Active: " + str(len(wrq_csv_report.jobs_active)))
			print("- CSV Jobs Completed: " + str(len(wrq_csv_report.jobs_completed)))
			print("- CSV Jobs Waiting: " + str(len(wrq_csv_report.jobs_waiting)))
			print("-------------------------")
			print("\n")
			print("=========================")
			print("Splunk Azure Bucket Blobs")
			print("=========================")
			if arguments.args.detailed_output:
				print("\n")
			if len(wrq_download.jobs_active) > 0 or len(wrq_logging.jobs_active) > 0 or len(wrq_csv_report.jobs_active) > 0:
				# do log write to log less often
				counter += 1
				if counter > 7:
					counter = 0
					tmp_log_lines = []
					tmp_log_lines.append("Elapsed Time: " + str(elapsed_time))
					tmp_log_lines.append("Percent Completed: " + str(percent_complete) + "%")
					wrq_logging.add(log_file.writeLinesToFile, [[(tmp_log_lines)]])
			else:
				print("- SABB(" + str(sys._getframe().f_lineno) +"):  Queues are empty. -")
				log_file.writeLinesToFile(['Queues are empty'])
				while wrq_download.inactive_timeout_counter > 0:
					time.sleep(10)
					print("- SABB(" + str(sys._getframe().f_lineno) +"):  Timing out and exiting if no new jobs are added in (sec): " + str(wrq_download.inactive_timeout_counter) + " -")
				else:
					print("- SABB(" + str(sys._getframe().f_lineno) +"): Exiting Threads Gracefully. -")
					log_file.writeLinesToFile(['Exiting Threads Gracefully.'])
					wrq_csv_report.stop()
					wrq_download.stop()
					wrq_logging.stop()
					thread_logging_parent.join()
					thread_blob_download_parent.join()
					thread_csv_report_parent.join()
					thread_update_completed.join()
					print("- SABB(" + str(sys._getframe().f_lineno) +"): Exiting. -")
					break
	else:
		print("- SABB(" + str(sys._getframe().f_lineno) +"): Goodbye. -")
		#thread_update_status.join()
		sys.exit(0)

### RUNTIME ###########################################
if __name__ == "__main__":
	start_time_str = currentDate(True)
	print("- SABB(" + str(sys._getframe().f_lineno) +"):  Start Time: " + start_time_str + " -")
	log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Start Time: " + currentDate(True)])
	start_time = currentDate(raw_or_str=True)
	
	## Download prep
	# get blobs into a list for download
	makeBlobDownloadList(dest_download_loc_root=arguments.args.dest_download_loc_root, 
						container_names_to_search_list=arguments.args.container_search_list,
						container_names_search_list_equals_or_contains=arguments.args.container_search_list_type,
						blob_names_to_search_list=arguments.args.blob_search_list,
						blob_names_search_list_equals_or_contains=arguments.args.blob_search_list_type,
						container_names_to_ignore_list=arguments.args.container_ignore_list,
						container_names_ignore_list_equals_or_contains=arguments.args.container_ignore_list_type,
						blob_names_to_ignore_list=arguments.args.blob_ignore_list,
						blob_names_ignore_list_equals_or_contains=arguments.args.blob_ignore_list_type)
	# exit if no blobs found to dl
	if not master_bucket_download_list:
		print("- SABB(" + str(sys._getframe().f_lineno) +"):  No Blobs found for download, exiting. -")
		log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):No Blobs found for download, exiting."])
		sys.exit()
	# add download jobs to download queue
	start_length_of_download_list = len(master_bucket_download_list)
#	print("exiting so not to download any real data outside of UK")
#	sys.exit()
	if not arguments.args.write_out_full_list_only:
		wrq_download.add(blob_service.downloadBlobByName, master_bucket_download_list, start_after_add=False)
		print("- SABB(" + str(sys._getframe().f_lineno) +"): Adding download job list to download queue: wrq_download -")
	else:
		print("\n\n\n#######################################################################################")
		print("- SABB(" + str(sys._getframe().f_lineno) +"): Writing download job list to CSV - NO ACTUAL DOWNLOADS WILL HAPPEN -")
		print("#######################################################################################\n\n\n")
		time.sleep(10)
	print("- SABB(" + str(sys._getframe().f_lineno) +"): " + str(len(master_bucket_download_list)) +" is number of items in the list -")
	log_file.writeLinesToFile( [str(len(master_bucket_download_list)) + " is number of items in the list to download"] )
	if not arguments.args.write_out_full_list_only:
		if not arguments.args.standalone:
			print("- SABB(" + str(sys._getframe().f_lineno) +"):  Clustered Env - GUID: " + str(azure_bucket_sorter.my_guid) + " using list number: " + str(azure_bucket_sorter.this_peer_index) + " -")
			log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):- SABB(" + str(sys._getframe().f_lineno) +"):  Clustered Env - GUID: " + str(azure_bucket_sorter.my_guid) + " using list number: " + str(azure_bucket_sorter.this_peer_index) + " -"])
		for i in master_bucket_download_list:
			log_file.writeLinesToFile(['Download - Job Added: ' + str(i) + ' - To Queue: wrq_download - blob_downloader'], 3)

	## Thread prep
	''' 
	Create parent threads
	The following threads will run simultaneously
		thread_logging_parent -> will run ONE single job at a time to ensure no two threads are trying to write to same log
		thread_blob_download_parent -> can run as many consecutive jobs as system can handle, user-specified
		thread_update_completed -> continuously checks job status of download and updates csv status report
		thread_update_status -> simple overall update status printing to log and console
	'''
	print("\n")
	
# CREATE parent threads

	# thread_logging_parent
	if arguments.args.detailed_output:
		print("Creating logging thread parent called: thread_logging_parent")
	log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Creating logging thread parent called: thread_logging_parent"])
	thread_logging_parent = threading.Thread(target=wrq_logging.start, name='logging_parent', args=())
	thread_logging_parent.daemon = True

	if not arguments.args.write_out_full_list_only:
		# thread_csv_report_parent
		if arguments.args.detailed_output:
			print("Creating csv reporter thread (writes lines to csv report in queue) called: thread_csv_report_parent")
		log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Creating logging thread parent called: thread_csv_report_parent"])
		thread_csv_report_parent = threading.Thread(target=wrq_csv_report.start, name='csv_report_parent', args=())
		thread_csv_report_parent.daemon = True
		run_me = True # used for the while loop in this thread and main thread!
	
		# thread_update_completed
		if arguments.args.detailed_output:
			print("Creating csv updater thread parent called: thread_update_completed")
		log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Creating csv updater thread parent called: thread_update_completed"])
		thread_update_completed = threading.Thread(target=updateCompletedWRQDownloadJobs, name='thread_update_completed', args=())
		thread_update_completed.daemon = True
	
		# thread_blob_download_parent
		if arguments.args.detailed_output:
			print("Creating download thread parent called: thread_blob_download_parent")
		log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Creating download thread parent called: thread_blob_download_parent"])
		thread_blob_download_parent = threading.Thread(target=wrq_download.start, name='blob_download_parent', args=())
		thread_blob_download_parent.daemon = True

## Here we go
# START parent threads

	# thread_logging_parent
	print("Starting: thread_logging_parent")
	log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Starting: thread_logging_parent"])
	thread_logging_parent.start()

	# thread_csv_report_parent
	print("Starting: thread_csv_report_parent")
	log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Starting: thread_csv_report_parent"])
	# create csv file and headers
	thread_csv_report_parent.start()

	if not arguments.args.write_out_full_list_only:
		#thread_blob_download_parent
		print("\n")
		print("Starting: thread_blob_download_parent")
		log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Starting: thread_blob_download_parent"])
		thread_blob_download_parent.start()

		# thread_update_completed
		print("Starting: thread_update_completed")
		log_file.writeLinesToFile(["- SABB(" + str(sys._getframe().f_lineno) +"):Starting: thread_update_completed"])
		thread_update_completed.start()


	time.sleep(5)
	if not arguments.args.write_out_full_list_only:
		timeAndCompletionChecker()
	else:
		print("\n\n\n###################################################################################################")
		print("- SABB(" + str(sys._getframe().f_lineno) +"): Starting: Write To CSV at: /logs/" + (main_report_csv) )
		print("#########################################################################################################\n\n\n")
		time.sleep(10)
		periodic_check = 200
		length_of_list = len(master_bucket_download_list)
		wrq_logging.stop()

		rows_list = []
		for idx, i in enumerate(master_bucket_download_list):
			if (idx % periodic_check):
				percent = (idx + 1) / length_of_list
				print("Working on: " + str(idx + 1) + " / " + str(length_of_list), " | ", str(percent) + "%" )
				wrq_csv_report.add(log_csv.writeLinesToCSV, [[(rows_list), ['Container_Name', 'Blob_Path_Name', 'Expected_Blob_Size_MB'] ]])
				rows_list = []
			else:
				rows_list.append([ i[2], i[0], i[1] ])