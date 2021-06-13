#!/usr/bin/env python3
#
##############################################################################################################

### Imports
import argparse

### CLASSES ###########################################

class LoadFromFile (argparse.Action):
	#Class for Loading Arguments in a file if wanted
	def __call__ (self, parser, namespace, values, option_string = None):
		with values as f:
			parser.parse_args(f.read().split(), namespace)

### FUNCTIONS ###########################################

def str2bool(string: str) -> bool:
	#Define common "string values" for bool args to accept "Yes" as well as "True"
	if isinstance(string, bool):
		return(string)
	if string.lower() in ('yes', 'true', 't', 'y', '1'):
		return(True)
	elif string.lower() in ('no', 'false', 'f', 'n', '0'):
		return(False)
	else:
		raise argparse.ArgumentTypeError('Boolean value expected.')

def checkPositive(value: str) -> int:
	ivalue = int(value)
	if ivalue < 0:
		raise argparse.ArgumentTypeError("%s is an invalid (below 0) int value" % value)
	return ivalue

def Arguments():
	# Arguments the app will accept
	global parser
	parser = argparse.ArgumentParser()
	parser.add_argument('--file', type=open, action=LoadFromFile)
	parser.add_argument("-do", "--detailed_output", type=str2bool, nargs='?', const=True, default=False, required=False, help="True to out more verbose console messages. Doesn't affect logging verbosity.")
	parser.add_argument("-lco", "--list_create_output", type=str2bool, nargs='?', const=True, default=True, required=False, help="True to out more verbose info during the list creation portion which can take a long time.")
	parser.add_argument("-cs", "--connect_string", nargs='?', default='', required=True, help="Full connection string to blob storage")
	parser.add_argument("-dl", "--dest_download_loc_root", nargs='?', default='./blob_downloads/', required=False, help="Full path to root location to download all the blobs. Blobs will retain THEIR file structure on top of this root. Default: ./blob_downloads")
	parser.add_argument("-tc", "--thread_count", type=checkPositive, nargs='?', default=10, required=False, help="Amount of download threads to run simultaneously.")
	parser.add_argument("-sa", "--standalone", type=str2bool, nargs='?', const=True, default=False,  required=False, help="True is standalone Splunk, False for idx cluster.")
	parser.add_argument("-sph", "--splunk_home", nargs='?', default='/opt/splunk/', required=False, help="Full path to Splunk's install dir.")
	parser.add_argument("-spu", "--splunk_username", nargs='?', default='', required=False, help="Splunk Username, required for Cluster Environment to make API call to CM")
	parser.add_argument("-spw", "--splunk_password", nargs='?', default='', required=False, help="Splunk Password, required for Cluster Environment to make API call to CM")
	parser.add_argument("-cm", "--cluster_master", nargs='?', default='', required=False, help="Splunk Cluster Master URL, include the https://. If empty, script will attempt to find on its own, usually it does.")
	parser.add_argument("-cmp", "--cluster_master_port", type=checkPositive, nargs='?', default=8089, required=False, help="Custom API port for Cluster Master, usually never used.")
	parser.add_argument("-ll", "--log_level", type=checkPositive, nargs='?', default=1, required=False, help="1-3, 1 being less, 3 being most")
	parser.add_argument('-csl', '--container_search_list', nargs='*', default=[], required=False, help="Values of containers to search in, separated by commas, i.e: 'container1,container2' ")
	parser.add_argument("-cslt", "--container_search_list_type", type=str2bool, nargs='?', const=True, default=False,  required=False, help="True for each item in container_search_list to have to be an exact match, False for contains. If using contains, search in can be lessened to wild cards like 'container' means '*container*' ")
	parser.add_argument('-bsl', '--blob_search_list', nargs='*', default=[], required=False, help="Values of blobs to search in, separated by commas, i.e: 'frozendata/1,frozendata/2' ")
	parser.add_argument("-bslt", "--blob_search_list_type", type=str2bool, nargs='?', const=True, default=False,  required=False, help="True for each item in blob_search_list to have to be an exact match, False for contains. If using contains, search in can be lessened to wild cards like 'frozend' means '*frozend*' ")
	parser.add_argument('-cigl', '--container_ignore_list', nargs='*', default=[], required=False, help="Values of containers to ignore AFTER search in has finished, separated by commas, i.e: 'container1,container2' ")
	parser.add_argument("-ciglt", "--container_ignore_list_type", type=str2bool, nargs='?', const=True, default=False,  required=False, help="True for each item in container_ignore_list to have to be an exact match, False for contains. If using contains, search in can be lessened to wild cards like 'container' means '*container*' ")
	parser.add_argument('-bigl', '--blob_ignore_list', nargs='*', default=[], required=False, help="Values of blobs to ignore AFTER search in has finished, separated by commas, i.e: 'frozendata3,frozendata9' ")
	parser.add_argument("-biglt", "--blob_ignore_list_type", type=str2bool, nargs='?', const=True, default=False,  required=False, help="True for each item in blob_ignore_list to have to be an exact match, False for contains. If using contains, search in can be lessened to wild cards like 'frozenda' means '*frozenda*' ")
	parser.add_argument("-dm", "--debug_modules", type=str2bool, nargs='?', const=True, default=False,  required=False, help="Will enable deep level debug on all the modules that make up the script. Enable if getting errors, to help dev pinpoint.")
	parser.add_argument("-woflo", "--write_out_full_list_only", type=str2bool, nargs='?', const=True, default=False,  required=False, help="True will write out the entire list for all peers to a single CSV and do nothing else.")
	parser.add_argument("-scsv", "--skip_to_csv_load", type=str2bool, nargs='?', const=True, default=False,  required=False, help="If True, it won't attempt to download latest. Will resume from CSV directly.")
	parser.add_argument("-ta", "--test_amount", type=checkPositive, nargs='?', default=0, required=False, help="Throw a number in here and azure scrape will stop in each container at this number (lets you test quickly before running on full amount) 0 for real run.")
############## RUNTIME
Arguments()
args = parser.parse_args()