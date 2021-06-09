#!/usr/bin/env python3
#
##############################################################################################################

### IMPORTS ###########################################
import os, time, datetime, csv, pandas, sys

from pathlib import Path

### FUNCTIONS ###########################################

# after 50mb create a new log file and append number at the end
def checkFileSize(log_file: str, roll_size_bytes=50000000, max_files_to_keep=0, debug=False) -> bool:
	'''
	Checks if current log is greater than bytes and creates a new by appending number to end
	Will delete any logs greater than max_files_to_keep unless 0 which is keep all
	'''
	if os.path.exists(log_file):
		if os.path.getsize(log_file) >= roll_size_bytes: #25000000:
			counter = 0
			if debug:
				print("- WRLog(" + str(sys._getframe().f_lineno) + "): File to be rotated / removed: {}".format(log_file))
			while os.path.exists((log_file) + "_" + str(counter)):
				counter = (counter) + 1
			else:
				os.rename( (log_file), (log_file) + "_" + str(counter) )
			if not max_files_to_keep == 0: # check if file need deletion after each new made
				log_dir = Path(log_file).parent
				path, dirs, files = next(os.walk(log_dir))
				num_logs = len(files)
				while num_logs > max_files_to_keep: # remove oldest log files until at specified keep amount
					path, dirs, files = next(os.walk(log_dir))
					num_logs = len(files)
					for file in files:
						oldest_file = min(log_dir + '/' + file, key=os.path.getctime)
						os.remove(oldest_file)
	else:
		if debug:
			print("- WRLog(" + str(sys._getframe().f_lineno) +"): File {} does not appear to exist: Skipping rotation".format(str(log_file)))

def isLogFileOld(file, log_retention_days):
	""" Determines if a log file is dictated to be 'old' - (I.e. if the log file is older then the retention period)
	Returns True is logfile is old, otherwise returns False
	"""
	#Age Format: YYYY-MM-DD HR:HH
	current_fileage = (time.strftime('%Y-%m-%d %H', time.gmtime(os.path.getmtime(file))))
	purge_age = (datetime.datetime.now() - datetime.timedelta(days=(log_retention_days))).strftime("%Y-%m-%d %H")
	if (current_fileage) < (purge_age):
		return True
	else:
		return False

def removeOldLogFiles(class_name:str, log_folder:str, log_file:str, log_retention_days:int):
	"""
	Removes all Log Files in that are older then a certain date
	Must be called manually
	"""
	# fetch list of every file in (log_folder)
	all_files_list = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser( (log_folder) )) for f in fn]
	if all_files_list:
		print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + class_name + ") : Log files Found: -")
		for f in all_files_list:
			if str(log_file) in f:
				print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + class_name + ") : " + f + " -") 
	if not os.path.exists(log_folder):
		print("-" + (log_folder)+" does not exist or couldn't be accessed, will attempt to create -")
	if not all_files_list:
		print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + class_name + ") : No old logs found for removal in " + class_name + " -")
		return
	print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + class_name + ") : Removing old log files -")
	for f in all_files_list:
		if str(log_file) in f:
			any_old_found = False
			if isLogFileOld(f, log_retention_days) or log_retention_days == 0:
				any_old_found = True
				try:
					os.remove(f)
					print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + class_name + ") : " + (f) + " deleted -")
				except:
					print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + class_name + ") : " +  (f) + " could not be deleted, permissions? -")
			if not any_old_found:
				print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + class_name + ") : No more logs older than " + str(log_retention_days) + " days found. -")

### CLASSES ###########################################

class LogFile():
	def __init__(self, name: str, log_folder='./logs/', remove_old_logs=False, log_level=1, log_retention_days=7, roll_size_bytes=50000000, max_files_to_keep=0,  prefix_date=True, debug=False):
		self.name = name # log file name - day will automatically be prefixed
		self.log_folder = log_folder # folder to write the log to
		self.log_level = log_level
		self.log_retention_days = log_retention_days
		self.roll_size_bytes = roll_size_bytes
		self.max_files_to_keep = max_files_to_keep
		self.debug = debug
		# if user specified own extension, dont add .log
		root, ext = os.path.splitext(self.name)
		if ext:
			if prefix_date:
				self.log_file = datetime.datetime.now().strftime("%Y_%m_%d") + "_" + (self.name)
			else:
				self.log_file = "_" + (self.name)
		else:
			if prefix_date:
				self.log_file = datetime.datetime.now().strftime("%Y_%m_%d") + "_" + (self.name) + ".log"
			else:
				self.log_file = "_" + (self.name) + ".log"
		if not os.path.exists(self.log_folder):
			try:
				os.makedirs( (self.log_folder), exist_ok=True)
			except:
				print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + self.name + ") : " + (self.log_folder) + ' - could not be accessed or created. Check permissions?')
		if remove_old_logs:
			removeOldLogFiles(self.name, self.log_folder, self.log_file, self.log_retention_days)
		self.log_path = (self.log_folder) + '/' + (self.log_file).replace('//','/').replace('\\\\','\\')

	def writeLinesToFile(self, lines: list, level=1, include_break=True):
		retry = 4
		while retry > 0:
			if not self.log_level == 1 or not self.log_level == 2 or not self.log_level == 3:
				self.log_level = 1
			if level <= self.log_level or level == 9:
				try:
					with open( (self.log_path),'a+' ) as file:
						for line in lines:
							time_stamp = datetime.datetime.now().strftime("%Y_%m_%d_T%H_%M_%S.%f")
							prefix = time_stamp + ' - LOG-LVL_' + str(level) + ' - '
							if include_break:
								file.write("%s" % (prefix) + line + "\n")
							else:
								file.write("%s" % (prefix) + line)
					file.close
					retry = 0
					checkFileSize(self.log_file, self.roll_size_bytes, self.max_files_to_keep, self.debug)
				except Exception as ex:
					print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + self.name + ") : Exception: -")
					print(ex)
					if retry > 0:
						print("Retrying write: " + (retry))
						time.sleep(0.1)
						retry -= 1
					else:
						print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + self.name + ") : Could not write to log file, check permissions of " + (self.log_folder) + " -")
			else:
				retry = 0

class CSVFile():
	def __init__(self, name: str, log_folder='./logs/', remove_old_logs=False, log_retention_days=7, prefix_date=True, debug=False):
		self.debug = debug
		self.name = name # log file name - day will automatically be prefixed
		self.log_folder = log_folder # folder to write the log to
		self.log_retention_days = log_retention_days
		# if user specified own extension, dont add .log
		root, ext = os.path.splitext(self.name)
		if ext:
			if prefix_date:
				self.log_file = datetime.datetime.now().strftime("%Y_%m_%d") + "_" + (self.name)
			else:
				self.log_file = "_" + (self.name)
		else:
			if prefix_date:
				self.log_file = datetime.datetime.now().strftime("%Y_%m_%d") + "_" + (self.name) + ".csv"
			else:
				self.log_file = "_" + (self.name) + ".log"
		if remove_old_logs:
			removeOldLogFiles(self.name, self.log_folder, self.log_file, self.log_retention_days)
		if not os.path.exists(self.log_folder):
			try:
				os.makedirs( (self.log_folder), exist_ok=True)
			except:
				print( (self.log_folder) + ' - could not be accessed or created. Check permissions?')
		self.log_path = (self.log_folder) + '/' + (self.log_file).replace('//','/').replace('\\\\','\\')

	def writeLinesToCSV(self, csv_rows: list, header_row=[]):
		if not os.path.exists(self.log_path):
			write_header = True
			header_written = False
		else:
			write_header = False
			header_written = True
		retry = 4
		while retry > 0:
			try:
				with open( (self.log_path), 'a+') as csv_file:
					writer = csv.writer(csv_file)
					for row in csv_rows:
						if header_row and header_written == False:
							# write header row
							if write_header:
								writer.writerow(header_row)
								header_written = True
						writer.writerow(row)
				csv_file.close
				retry = 0
			except Exception as ex:
				print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + self.name + ") : Exception: -")
				print(ex)
				if retry > 0:
					print("Retrying write: " + (retry))
					time.sleep(0.1)
					retry -= 1
				else:
					print("Could not write to log file, check permissions of " + (self.log_folder) )

	def updateCellByHeader(self, header_to_search_under: str, value_to_search, header_to_update: str, value_to_write):
		'''
		Search by header for a string to find the row.
		Then update / add value under a header with the value in value_to_write
		'''
		if os.path.exists(self.log_path):
			try:
				df = pandas.read_csv(self.log_path)
				df.loc[df [ (header_to_search_under) ] == (value_to_search), (header_to_update)] = (value_to_write)
				return(True)
			except:
				print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + self.name + "): - Could not read csv specified. -")
				return(False)
		else:
			return(False)

	def getValueByHeaders(self, first_header_to_search_under: str, value_under_first_header_to_search, second_header_to_search_under: str) -> list:
		'''
		Search by header for a string to find the row.
		Return the (True, str(<value>)) if found.
		'''
		if os.path.exists(self.log_path):
			try:
				df = pandas.read_csv(self.log_path)
				value = df.loc[df[first_header_to_search_under] == value_under_first_header_to_search, second_header_to_search_under].tolist()
				return(True, value)
				#value = df.loc[df['Blob_Path_Name'] == 'frozendata/barracuda/frozendb/db_1621091116_1625030436_62_98B6F435-6FB4-4FE5-8E89-6F7C865A4F9E/rawdata/journal.gz', 'Download_Complete'].tolist()
			except:
				print("- WRLog(" + str(sys._getframe().f_lineno) +") (" + self.name + "): - Could not read csv specified. -")
				return(False, "")
		else:
			return(False, "")