##############################################################################################################
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
##############################################################################################################

### Imports ###########################################
import time, sys, os, shutil

from . import wr_logging as log

### LOGGING CLASS ###########################################
log_file = log.LogFile('wrc.log', log_folder='./logs/', remove_old_logs=True, log_level=3, log_retention_days=10)

### Classes ###########################################
class timer:
	'''
	You should import this file and then create as many variables there as you want timers of this class.
	Then kick off each in a thread in the main script. Refer back to this for status etc
	
	<variable>.elapsed (for elapsed time)
	'''
	# this is the startup script, init?
	def __init__(self, name: str, max_time_sec:int, start_time_sec = 0, start_immediately=False, print_outs=True, print_interval=10):
		self.name = name # timer name
		self.start_time_sec = start_time_sec
		self.max_time_sec = max_time_sec
		self.started = False
		self.current_time_sec = 0
		self.max_time_reached = False
		self.print_outs = print_outs
		self.print_interval = print_interval
		if start_immediately:
			self.start()

	### Functions
	def start(self):
		if self.started:
			print("\n- WRC Timer: "+ self.name + " - Already started, ignoring. -")
		else:
			print("\n- WRC Timer: "+ self.name + " - Started. -")
			self.started = True
			while self.started:
				time.sleep(1)
				self.current_time_sec += 1
				cur_print = round(self.current_time_sec / 60, 2)
				if not self.max_time_sec == 0:
					if self.print_outs:
						if self.current_time_sec % self.print_interval == 0:
							print("\n- WRC Timer: "+ self.name + " - Elapsed (min): " + str(cur_print) + " / " + str( round(self.max_time_sec / 60, 2) ) + " -" )
					if self.current_time_sec >= self.max_time_sec:
						print("\n- WRC Timer: "+ self.name + " - Maxt time reached, stopping timer. -")
						self.max_time_reached = True
						self.stop()
						break
				else:
					if self.print_outs:
						if self.current_time_sec % self.print_interval == 0:
							print("\n- WRC Timer: "+ self.name + " - Elapsed (min): " + str(cur_print) + " -")

	def stop(self):
		if not self.started:
			print("\n- WRC Timer: "+ self.name + " - Not started, ignoring. -")
		else:
			print("\n- WRC Timer: "+ self.name + " - Stopped. -")
			self.started = False

	def reset(self):
		print("\n- WRC Timer: "+ self.name + " - Reset and stopped. -")
		self.started = False
		self.max_time_reached = False
		self.current_time_sec = 0

	def elapsed(self, unit='s'):
		'''
		s = seconds (default)
		m = minutes
		h = hours
		'''
		if unit == 's':
			ret_time = self.current_time_sec
		elif unit == 'm':
			ret_time = round(self.current_time_sec / 60, 2)
		elif unit == 'h':
			ret_time = round(self.current_time_sec / 3600, 2)
		else:
			ret_time =self.current_time_sec
		return(ret_time)

### FUNCTIONS ###########################################

# clear console on OS
def clearConsole():
	if 'win' in sys.platform:
		os.system('cls')
	else:
		os.system('clear')

# pass any path in here, windows or linux and normalize it to whatever OS the script is running on
def normalizePathOS(path:str) -> str:
	'''
	Formats path string to be windows or linux depending on OS
	Always adds the trailing slash!!
	Returns formatted path as a string
	'''
	if 'win' in sys.platform:
		path.replace('/', '\\')
		if not path.endswith('\\'):
			path = path + '\\'
	else:
		path.replace('\\', '/')
		if not path.endswith('/'):
			path = path + '/'
	path.replace('//','/').replace('\\\\','\\')
	return(path)

# a "naive" but readable-friendly code way of checking if a string is in a list, exact or contains options
def isInList(string_to_test:str, list_to_check_against:list, equals_or_contains=True, string_in_list_or_items_in_list_in_string=True) -> bool:
	'''
	Simple function to check if a string exists in a list either by exact match or contains. 
	string_in_list_or_items_in_list_in_string when False will check if any of the items in the LIST exists in or equal the string. 
	True will check if the string is in the list.
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
					log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "):  Looking for: " + header + " in file."] )
					first_run = False
				if header in line:
					header_found = True
				continue
			for string in string_to_find:
				found = "- WRC(" + str(sys._getframe().f_lineno) + "): Found: " + string + " in file. -\n"
				log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "):  Found: " + string + " in file."] )
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
def findFileByName(file_name:str, search_in: tuple, file_search_list=[], file_search_list_type=False, file_ignore_list=[], file_ignore_list_type=True) -> tuple:
	'''
	Finds occurances of a file in folders by a name
	Returns a tuple of full paths to various occurances
	search_in may contain a tuple of full paths to search in specifically separated by commas, or one path
	Optional search in filters can be specified, file_search_list and file_search_list_type True is equals, False contains
	Optional ignore in filters can be specified, file_ignore_list and file_ignore_list_type True is equals, False contains
	'''
	found_paths_list = []
	found_one = False
	for folder in search_in:
		for root, dirs, files in os.walk(folder):
			for file in files:
				if file == file_name:
					file_full = os.path.join(root, file)
					if file_search_list:
						if not isInList(file_full, file_search_list, equals_or_contains=file_search_list_type, string_in_list_or_items_in_list_in_string=False):
							continue
					if file_ignore_list:
						if isInList(file_full, file_ignore_list, equals_or_contains=file_ignore_list_type, string_in_list_or_items_in_list_in_string=False):
							continue
					found_paths_list.append( file_full )
					found_one = True
					log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "):  Found: " + file_name] )
					continue
				else:
					continue
	if found_one:
		return(True, tuple(found_paths_list))
	else:
		return(False, tuple())

def replaceTextInFile(file_name:str, replace_dict:dict, create_backup=False, backup_to='', case_senseitive=False, test_run=False, verbose_prints=True) -> dict:
	'''
	file_name should be full path to the file
	This will read the file, make the changes and write the full file out
	replace_dict should be in the format of 'replace_me':'replace_with'
	test_run = True will return the changes and print the new file but won't actually write it
	Returns a dict with just the changes, orig_line:new_line
	'''
	# this function will read a file line by line
	# check each line for occurences of the replacements and do the replacements if needed
	# add the original or new line to a new list
	# backup the original file if requested
	# then write out the file again with the replaced lines0
	changes_log = {}
	final_changes_log = {}
	new_file_list = []
	file_name = normalizePathOS(file_name)[:-1] #normalize path for OS redundancy check and remove trailing slash
	try:
		if test_run:
			print("- WRC(" + str(sys._getframe().f_lineno) + "): ########################################## -")
			print("- WRC(" + str(sys._getframe().f_lineno) + "): THIS IS A TEST RUN - NO RENAMES WILL OCCUR -")
			print("- WRC(" + str(sys._getframe().f_lineno) + "): ########################################## -")
		print("\n- WRC(" + str(sys._getframe().f_lineno) + "): Attempting to open: " + file_name + " -")
		log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Attempting to open: " + file_name] )
		with open(file_name) as f: # open file and read each line
			for line in f:
				new_line = line # set new_line to original line, if no replacement needed, we keep original line
				for k,v in replace_dict.items():
					if not case_senseitive:
						k = str(k).lower()
						v = str(v).lower()
						line = line.lower()
#					print("TEST LINE: ", line)
#					print("TEST K: ", k)
#					print("TEST V: ", v)
					if str(k) in line:
						if verbose_prints:
							print("- WRC(" + str(sys._getframe().f_lineno) + "):	Found: " + k + " in " + line.strip() + " -")
							print("- WRC(" + str(sys._getframe().f_lineno) + "):	Replacing: " + k + " with " + v + " -\n")
						log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Found: " + str(k) + " in " + line.strip()])
						log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Replacing: " + k + " with " + v])
						new_line = line.replace(str(k),str(v)) # add replacement line to new_line var
						changes_log[line]=new_line
						break
				new_file_list.append(new_line) # add line to ongoing list of file lines to write back later
		f.close()

		if changes_log:
			# create backup
			if not test_run: # only write the new file if and create a backup if NOT a test!
				if create_backup:
					if backup_to:
						try:
							backup_to = normalizePathOS(backup_to)[:-1] # remove trailing slash
							os.makedirs(str(backup_to), exist_ok=True)
						except Exception as ex:
							print("- WRC(" + str(sys._getframe().f_lineno) + "): Specified backup dir couldn't be used: " + backup_to + ", backing up in same dir instead. -")
							print(ex)						
							log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Specified backup dir couldn't be used: " + backup_to + ", backing up in same dir instead."] )
							log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): " + str(ex)] )
							backup_to = False
					if file_name.endswith('/') or file_name.endswith('\\'):
						file_name = file_name[:-1]
					os.makedirs(backup_to + os.path.dirname(file_name), exist_ok=True)
					if not backup_to:
						suffix_num = 1
						suffix = '_rename_backup_' + str(suffix_num)
						while os.path.exists(file_name + suffix):
							suffix_num += 1
							suffix = '_rename_backup_' + str(suffix_num)
						shutil.copy(file_name, file_name + suffix)
					else:
						suffix_num = 0
						final_backup_path = backup_to + file_name
						while os.path.exists(final_backup_path):
							suffix_num += 1
							final_backup_path = final_backup_path + '_rename_backup_' + str(suffix_num)
						shutil.copy(file_name, final_backup_path) # keeps dir structure

				# write out new file
				print("- WRC(" + str(sys._getframe().f_lineno) + "): Writing out new file: " + file_name + " -")
				log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Writing out new file: " + file_name] )
				with open(file_name, 'w') as f:
					for i in new_file_list:
						for k, v in changes_log.items():
							if str(i) == str(v):
								final_changes_log[k]=(v)
								break
						f.write("%s" % i)
				f.close()
				print("- WRC(" + str(sys._getframe().f_lineno) + "): Write successful of file: " + file_name + " -")
				log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Write successful of file: " + file_name] )

			# print output of what was done
			if test_run:
				action = "would be"
			else:
				action = "are"
			if verbose_prints:
				if changes_log:
					print("\n- WRC(" + str(sys._getframe().f_lineno) + "): The following lines "+ action + " changed: -")
			if changes_log:
				log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): The following lines "+ action + " changed:"], 3)
			for k, v in changes_log.items():
				if verbose_prints:
					print("- WRC(" + str(sys._getframe().f_lineno) + "):	Original line: " + str(k).strip() + " -")
					print("- WRC(" + str(sys._getframe().f_lineno) + "):	Replaced line: " + v + " -\n")
				log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): 	Original line: " + k], 3)
				log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): 	Replaced line: \n" + v], 3)

			if verbose_prints:
				print("- WRC(" + str(sys._getframe().f_lineno) + "): The new line(s) in the file in its entirety "+ action +": -")
			log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): The new line(s) in the file in its entirety "+ action +": "], 3)
			for i in new_file_list:
				if verbose_prints:
					print("- WRC(" + str(sys._getframe().f_lineno) + "):	" + str(i).strip() )
				log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "):  " + str(i).strip()], 3)
		else:
			if verbose_prints:
				print("- WRC(" + str(sys._getframe().f_lineno) + "): No changes found in file. Nothing done\n")
			log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): No changes found in file. Nothing done\n"])
			return(final_changes_log)
		
	except Exception as ex:
		print("- WRC(" + str(sys._getframe().f_lineno) + "): Replacing items in file failed for: " + file_name + ", check permissions? -")
		print(ex)
		log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Replacing items in file failed for: " + file_name + ", check permissions?"] )
		log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): " + str(ex)] )
	return(final_changes_log)

def renameFolder(orig:str, new:str, create_backup=False, backup_to='', test_run=False) -> bool:
	'''
	orig and new should be FULL paths to the folder to be renamed
	backup_to is optional, leave blank to create a backup in the same folder if create_backup is True, speficy to create it somewhere else
	Returns True of False if successful
	'''
	orig = normalizePathOS(orig)
	new = normalizePathOS(new)
	try:
		if test_run:
			print("- WRC(" + str(sys._getframe().f_lineno) + "): ########################################## -")
			print("- WRC(" + str(sys._getframe().f_lineno) + "): THIS IS A TEST RUN - NO RENAMES WILL OCCUR -")
			print("- WRC(" + str(sys._getframe().f_lineno) + "): ########################################## -")
			log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): THIS IS A TEST RUN - NO RENAMES WILL OCCUR"] )
		print("- WRC(" + str(sys._getframe().f_lineno) + "): Attempting to rename: " + orig + " TO " + new + " -")
		log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Attempting to rename: " + orig + " TO " + new] )

		if not test_run: # do the rename if NOT a test run
			if create_backup:
				if backup_to:
					try:
						backup_to = normalizePathOS(backup_to)[:-1]
						os.makedirs(str(backup_to), exist_ok=True)
					except Exception as ex:
						print("- WRC(" + str(sys._getframe().f_lineno) + "): Specified backup dir couldn't be used: " + backup_to + ", backing up in same dir instead. -")
						print(ex)						
						log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Specified backup dir couldn't be used: " + backup_to + ", backing up in same dir instead."] )
						log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): " + str(ex)] )
						backup_to = False
				if not backup_to:
					suffix_num = 1
					if orig.endswith('/') or orig.endswith('\\'):
						orig = orig[:-1]
					suffix = '_rename_backup_' + str(suffix_num)
					while os.path.exists(orig + suffix):
						suffix_num += 1
						suffix = '_rename_backup_' + str(suffix_num)
					shutil.copytree(orig, orig + suffix)
				else:
					suffix_num = 0
					tmp_backup_to = backup_to + orig[:-1]
					while os.path.exists(tmp_backup_to):
						suffix_num += 1
						tmp_backup_to = tmp_backup_to + "_" + str(suffix_num)
					shutil.copytree(orig, tmp_backup_to) # keeps dir structure
			os.rename(orig, new)
		return(True)

	except Exception as ex:
		print("- WRC(" + str(sys._getframe().f_lineno) + "): Renaming " + orig + " FAILED. Are you using full paths? -")
		print(ex)
		log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): Renaming " + orig + " FAILED. Are you using full paths?"] )
		log_file.writeLinesToFile(["(" + str(sys._getframe().f_lineno) + "): " + str(ex)] )
		return(False)