##############################################################################################################
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
##############################################################################################################

### Imports ###########################################
import datetime, time, sys, os

from . import wr_logging as log

### LOGGING CLASS ###########################################
log_file = log.LogFile('wrc.log', log_folder='../logs/', remove_old_logs=True, log_level=3, log_retention_days=10)

### FUNCTIONS ###########################################

# find and return a specific line in a file by contains or exact
def findLineInFile(string_to_find:list, file_path:str, equals_or_contains=False, use_header=True, header='[clustering]') -> str:
	'''
	Find a line in a file and return the whole line using a list of search terms
	First match found is the one returned
	Can find an exact match (to confirm line exists?)
		OR
	Can find a partial (contains) to get the whole line.
	Optional: use_header -> True will require the header to be specified.
	It will find the NEXT matching line after the header and ignore matches prior
	'''
	with open(file_path) as file:
		lines = file.readlines()
		header_found = False
		first_run = True
		for line in lines:
			if use_header and not header_found:
				if first_run:
					print("\n- WRC: Looking for: " + header + " in file. -")
					log_file.writeLinesToFile([str(sys._getframe().f_lineno) + " Looking for: " + header + " in file."] )
					first_run = False
				if header in line:
					header_found = True
				continue
			for string in string_to_find:
				found = "- WRC: Found: " + string + " in file. -\n"
				log_file.writeLinesToFile([str(sys._getframe().f_lineno) + " Found: " + string + " in file."] )
				if equals_or_contains:
					if string == line:
						print(found)
						return(True, line)
				else:
					if string in line:
						print(found)
						return(True, line)
		return(False, "")

# find a list of occurances of a file by name
def findFileByName(file_name:str, search_in: tuple) -> tuple:
	'''
	Finds occurances of a file in Splunk folders by a name
	Returns a tuple of full paths to various occurances
	search_in may contain a tuple of full paths to search in specifically separated by commas, or one path
	Will search the order given and return the order found
	'''
	found_paths_list = []
	found_one = False
	for folder in search_in:
		for root, dirs, files in os.walk(folder):
			for file in files:
				if file == file_name:
					found_paths_list.append( os.path.join(root, file) )
					found_one = True
					log_file.writeLinesToFile([str(sys._getframe().f_lineno) + " Found: " + file_name + " in file."] )
					continue
				else:
					continue
	if found_one:
		return(True, tuple(found_paths_list))
	else:
		return(False, tuple())