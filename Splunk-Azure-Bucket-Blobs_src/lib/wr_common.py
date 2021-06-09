##############################################################################################################
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
##############################################################################################################

### Imports ###########################################
import time, sys, os

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
	def __init__(self, name: str, max_time_sec:int, start_time_sec = 0, start_immediately=False, print_outs=True):
		self.name = name # timer name
		self.start_time_sec = start_time_sec
		self.max_time_sec = max_time_sec
		self.started = False
		self.current_time_sec = 0
		self.max_time_reached = False
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
				if not self.max_time_sec == 0:
					print("\n- WRC Timer: "+ self.name + " - Elapsed (min): " + str(self.current_time_sec / 60) + " / " + str(self.max_time_sec / 60) + "-" )
					if self.current_time_sec >= self.max_time_sec:
						print("\n- WRC Timer: "+ self.name + " - Maxt time reached, stopping timer. -")
						self.max_time_reached = True
						self.stop()
						break
				else:
					print("\n- WRC Timer: "+ self.name + " - Elapsed (min): " + str(self.current_time_sec / 60) + "-")
				self.current_time_sec += 1
					

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
		self.start_time_sec = start_time_sec
		self.max_time_sec = max_time_sec
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
			ret_time = self.current_time_sec / 60
		elif unit == 'h':
			ret_time = self.current_time_sec / 3600
		else:
			ret_time =self.current_time_sec
		return(ret_time)

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