##############################################################################################################
# Contact: Will Rivendell 
# 	E1: wrivendell@splunk.com
# 	E2: contact@willrivendell.com
#
#   A "custom" queue making 'pool-like' process class
##############################################################################################################

### Imports
import datetime, time, uuid, threading, sys

from . import wr_logging as log

### Classes ###########################################

class Queue():
	'''
	Import this into any script where you want to run parallel jobs. This isn't POOLING from the multiprocessing
	module, this is Threading. This is excellent for jobs where you want to run many parallel tasks that are IO intensive
	and can take awhile. Particularly (but not limited to) copy, move, delete, download, upload, cross server operations etc.
	You can of course kill your resources still. 
	
	You can raise or lower threads_at_once on the fly by running: wrq_print.increaseThreadsTo(8)
	If you're monitoring your HDD activity, Memory, Network and CPU, you could make this "smart" but auto adjusting based on
		your specs.

	Queue will stay open for 60 sec when empty if no jobs are added or not killed directly.

	Typical use-case:

		create a wrq for downloading files from place A
		create a second wrq for uploading downloaded files to place B
		create a third wrq for writing results to files for resuming and logging


	For each "queue" you want to make for processing, simply:
		1. Create an instance of this Queue class with a name
		2. Add jobs to it
		3. Start the queue as a thread in the main script

		You CAN add jobs to the queue while the queue is running.
		 i.e: an example of a queue that will traverse the list of strings printing two at once since its specified 
		 	2 threads at a time
		
		Quick start:

			EXAMPLE FUNCTION:

			 def testPrint(string_to_print):
				 time.sleep(3)
				 print(string_to_print)

			import threading
			from lib import wr_queue as wrq
			wrq_print = wrq.Queue('print_q', 2)
			wrq_print.add(testPrint, [
										['string_1'], ['string_2'], ['string_3'], ['string_4'],
        	                         	['string_5'], ['string_6'], ['string_7'], ['string_8'],
        	                            ['string_9'], ['string_10'], ['string_11']
        	                        ], start_after_add=False)

									# NOTE args MUST be in the form of a list of lists, even if just one arg!
									# eg. two args for the one process [ ['string_1', True] ]
									# each inside list is ONE thread on its own

		# create a parent thread (in your main script) to run this queue while not tying up your script
		thread1 = threading.Thread( target=wrq_print.start, name='test', args=() )
		thread1.start()

			OUTPUT:

				Only 1 processing, adding new!
				Still 2 processing, waiting!
				string_1
				string_2
				Only 1 processing, adding new!
				Still 2 processing, waiting!
				string_3
				Only 1 processing, adding new!
				Still 2 processing, waiting!
				string_4
				Only 1 processing, adding new!
				Still 2 processing, waiting!
				string_5
				Only 1 processing, adding new!
				Still 2 processing, waiting!
				string_6
				Only 1 processing, adding new!
				Still 2 processing, waiting!
				string_7
				Only 1 processing, adding new!
				Still 2 processing, waiting!
				string_8
				Only 1 processing, adding new!
				Still 2 processing, waiting!
				string_9
				Only 1 processing, adding new!
				Still 2 processing, waiting!
				string_10
				Only 1 processing, adding new!
				Still 2 processing, waiting!
				string_11

		wrq_print.status()

			OUTPUT:

				active

		check jobs completed list:

			for jwrq_print.jobs_complete:
				print(j)

		# ... see function __init__ comments for more details on job lists
	'''
	# this is the startup script, init?
	def __init__(self, name: str, threads_at_once: int, debug=False):
		self.debug = debug # enable debug printouts
		self.name = name # unique thread name
		self.threads_at_once = threads_at_once # how many max threads can run at one time
		self.queue_started = False # whether or not x.start() has been run on this queue
		self.stopped = False # job processing is stopped or not
		self.paused = False # job processing is paused or not
		self.pause_timeout_sec = 3600 # how many seconds to wait before terminating whole queue if left paused
		self.inactive_queue_timeout_sec = 60 # how many seconds to wait before terminating whole queue if no jobs are processed or added
		self.inactive_timeout_counter = 0 # the actual count down, not just the max
		self.total_time_taken = 0 # sum of time taken for all jobs in minutes - accumulates
		self.average_job_time = 0 # average time per job in minutes
		self.estimated_finish_time = 0 # estimated time to copletion based on average
		self.log_file = log.LogFile('wrq_' + self.name + '.log', log_folder='./logs/', remove_old_logs=True, log_level=3, log_retention_days=10)

		'''
		The following tuples have two entries and are in the format of
		(threading.Thread, str(args used in the command))
		The job/thread details are at index 0 while the command it runs is index [1]
		i.e.

			for j in wrq_print.jobs_completed:
        		job_name = j[0].name
        		job_id = j[0].ident
        		job_command = j[1]
				job_start_time = j[2]    - datetime.datetime
				job_finish_time = j[3]   - datetime.datetime
				job_time_taken = j[4]    - float in minutes
		'''
		self.jobs_waiting = [] # jobs waiting their turn to be processed when active frees up
		self.jobs_active = [] # jobs currently being processed
		self.jobs_completed = [] # jobs completed are moved here from active for record keeping
		
	# increases threads at once
	def increaseThreadsTo(self, new_threads_at_once: int):
		'''
		Increase on the fly simultaneous process limit.
		You could dynamically update this from your main script
			if monitoring system performance or bandwidth as an idea.
		'''
		if new_threads_at_once <= 0:
			new_threads_at_once = 1
		self.threads_at_once = new_threads_at_once

	# calculate thread and queue timing info
	def updateTimings(self, additional_time:float):
		additional_time = additional_time / ( float(self.threads_at_once) / 3)
		self.total_time_taken = self.total_time_taken + additional_time
		self.average_job_time = self.total_time_taken / float(len(self.jobs_completed)) / 60
		self.estimated_finish_time = len(self.jobs_waiting) * self.average_job_time

	# check, finish, calculate time on active jobs
	def activeJobsHandler(self):
		if len(self.jobs_active) > 0:
			for j in self.jobs_active:
				time.sleep(0.5)
				if j[0].is_alive():
					pass
				else:
					j.append(datetime.datetime.now())
					diff = j[3] - j[2]
					diff = diff.seconds
					j.append(diff)
					self.jobs_completed.append(j)
					self.updateTimings(diff)
					self.jobs_active.remove(j)
		if self.paused:
			time.sleep(10)
			if self.debug:
				print("- WRQ(" + str(sys._getframe().f_lineno) +"): Queue is paused. -")
				self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": Queue is paused."] )
		else:
			if self.debug:
				print("- WRQ(" + str(sys._getframe().f_lineno) +"): Still "+str(len(self.jobs_active))+" running. -")
				self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": Still "+str(len(self.jobs_active))+" running."] )

	# checks to see how much room active jobs can hold and relays that to activateNextJobs()
	def activeJobsRoom(self):
		active_room = self.threads_at_once - len(self.jobs_active)
		if active_room > 0:
			return(int(active_room))
		else:
			return(1)

	# moves new jobs from waiting to active and starts if there's room
	def activateNextJobs(self):
		job_max = 1
		if self.debug:
			print("- WRQ(" + str(sys._getframe().f_lineno) +"): " + str( len(self.jobs_active) )+" running, adding next. -")
			self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ":" +  str( len(self.jobs_active) )+" running, adding next."] )
		# add as many jobs as we have room and start them in the active queue
		if len(self.jobs_waiting) > 0:
			if not self.threads_at_once == 1:
				if len(self.jobs_waiting) < self.activeJobsRoom():
					job_max = len(self.jobs_waiting)
				else:
					job_max = self.activeJobsRoom()
			# to active list from waiting
			tmp_list = self.jobs_waiting[0:(job_max)]
			for i in tmp_list:
				self.jobs_active.append(i)
				self.jobs_waiting.remove(i)
			# start any newly added jobs
			for j in self.jobs_active:
				if j[0].is_alive():
					continue
				else:
					try:
						# add start time
						j.append(datetime.datetime.now())
						j[0].start()
					except:
						continue
			if self.debug:
				print("- WRQ(" + str(sys._getframe().f_lineno) +"): Started "+str( job_max )+" new processes. -")
				self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": Started " + str( job_max ) + " new processes."] )
		else:
			if self.debug:
				print("- WRQ(" + str(sys._getframe().f_lineno) +"): No more jobs waiting in the queue, only active left. -")
				self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": No more jobs waiting in the queue, only active left."] )
			while len(self.jobs_active) > 0:
				time.sleep(10)
				self.activeJobsHandler()

	# removes all jobs from the queues and joins them
	def clearJobs(self, wait_for_complete=False):
		'''
		Clears ALL lists of jobs, waits for completion of jobs unless forced
		Runs at the end of queue timeout
		'''
		while len(self.jobs_active) > 0 or len(self.jobs_waiting) > 0 or len(self.jobs_completed) > 0:
			list_of_job_lists = [(self.jobs_active), (self.jobs_waiting), (self.jobs_completed)]
			for job_list in list_of_job_lists:
				for j in job_list:
					if j[0].is_alive():
						if wait_for_complete:
							continue
						j[0].join()
						job_list.remove(j)
					else:
						job_list.remove(j)
		else:
			return(True)

	# can be called from anywhere to return the current status processing threads
	def status(self):
		'''
		returns QUEUE status
			paused if paused
			active if running
			empty if no jobs
			stalled if jobs but not paused or active (broken)
		'''
		if self.jobs_active:
			if self.paused:
				return("paused")
			active = False
			for ja in self.jobs_active:
				if ja[0].is_alive():
					active = True
					return('active')
		else:
			return("empty")
		if active:
			return("active")
		else:
			return("stalled")

	# pauses processing of the active queue - toggles - check status function for paused status
	def pause(self):
		'''
		Toggles. If paused and called it unpauses.
		'''
		if self.paused:
			if self.debug:
				print("- WRQ(" + str(sys._getframe().f_lineno) +"): Queue already paused, unpausing. -")
				self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": Queue already paused, unpausing."] )
			self.paused = False
			timeout = self.pause_timeout_sec
		else:
			print("- WRQ(" + str(sys._getframe().f_lineno) +"): Queue will pause after current jobs finish. -")
			self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": Queue will pause after current jobs finish."] )
			self.paused = True
			timeout = self.pause_timeout_sec
			while self.paused:
				# If paused, start timeout timer
				timeout -= 1
				time.sleep(1)
				if timeout <= 0:
					print("- WRQ(" + str(sys._getframe().f_lineno) +"): " + str(self.name) + " has been paused for too long. Killing jobs. -")
					self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + str(self.name) + ": has been paused for too long. Killing jobs."] )
					while not self.clearJobs():
						time.sleep(10)
						print("- WRQ(" + str(sys._getframe().f_lineno) +"): " + self.name + " is waiting for active jobs to finish before timing out. -")
						self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": is waiting for active jobs to finish before timing out."] )
					else:
						print("- WRQ(" + str(sys._getframe().f_lineno) +"): " + self.name + " - All jobs cleared after pause timeout. -")
						self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": - All jobs cleared after pause timeout."] )
						sys.exit(1)

	def stop(self):
		'''
		Clears all jobs and stops the queue when called
		Does not wait for time out counter, nor does it collect $200
		'''
		if self.stopped:
			while not self.clearJobs():
				time.sleep(10)
				print("- WRQ(" + str(sys._getframe().f_lineno) +"): " + self.name + " is waiting for active jobs to finish before fully stopping. -")
				self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": is waiting for active jobs to finish before fully stopping."] )
			else:
				print("- WRQ(" + str(sys._getframe().f_lineno) +"): " + self.name + " - All jobs cleared after stop. -")
				self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": - All jobs cleared after stop."] )
				sys.exit(1)
		else:
			self.stopped = True

	# adds a list of jobs to the self.jobs_waiting queue
	def add(self, function_to_run: 'function', arg_list_to_process: list, start_after_add=False):
		'''
		Function_to_run should be the function you want to spawn up workers to tackle in parallel.
		TLDR: 
			# put the name of the funtion in function_to_run e.g. printHelloWorldxTimes
			# the args that the function requires should be put in a list eg. ['helloWorld', 15]
			# make as many lists as you want total (doesnt affect concurrent) jobs to run
			# each list of args is a single job i.e if you want to run 10000 jobs, you'd need 10000 lists of args
			# add the above lists to one master list of args eg. queued_jobs_arg_lists.append(['helloWorld', 15])
			# put queued_jobs_arg_lists in this input arg: arg_list_to_process

		arg_list_to_process should contain the arguments youd pass to function_to_run. The arguments per
			run/process/job should be in a list in the proper order the function expects. Each set of args should then
			be in a larger singular list which is what arg_list_to_process is. If your function_to_run takes one single string
			for example, you'd still pass this in as your arg_list_to process:   [['string']]

		Once the queue is STARTED, it will process all jobs and then close the queue after a timeout.

		start_after_add True will auto start the queue (and hold up the script that runs this until queue is completed).
		Therefore if you want your main script to keep doing things after starting jobs in this queue 
			use <wrq_name>.start() instead inside a simple thread (see start() for details)
		'''
		for i in arg_list_to_process:
			if self.debug:
				print("- WRQ(" + str(sys._getframe().f_lineno) +"): job added: " + str(i) + " -")
				self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": job added: " + str(i)] )
			job_name = str(self.name) + '_j_' + str(uuid.uuid4().hex)
			t = threading.Thread( target=function_to_run, name=job_name, args=(i) )
			t.daemon = True
			tmp_job = [t, str(i)]
			self.jobs_waiting.append(tmp_job)
		if start_after_add:
			self.start()

	# starts the loop of jobs_waiting -> jobs_active -> jobs_completed and continues til completion
	def start(self):
		'''
		Start processing the jobs in jobs_waiting.
		This loop can be paused once started but won't stop. 

		Pause will timeout after 60 min (by default) if not unpaused. 
		Pause timer resets every pause.

		While loop runs while there are jobs present in jobs_waiting or jobs_active. 
		'''
		if self.queue_started:
			print("- WRQ(" + str(sys._getframe().f_lineno) +"): Queue already started. -")
			return
		self.inactive_timeout_counter = self.inactive_queue_timeout_sec
		self.queue_started = True
		self.inactive_timeout_counter = self.inactive_queue_timeout_sec
		# Run this processing loop as long as there are items in the either job list
		while self.inactive_timeout_counter > 0:
			while len(self.jobs_waiting) > 0 or len(self.jobs_active) > 0 and not self.stopped:
				self.inactive_timeout_counter = self.inactive_queue_timeout_sec
				if self.debug:
					print("- WRQ(" + str(sys._getframe().f_lineno) +"): Still actively looking for and processing jobs... -")
					self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": Still actively looking for and processing jobs..."] )
				# Wait until a job finishes before starting another or stay in loop while paused
				while len(self.jobs_active) >= (self.threads_at_once) or self.paused:
					if self.stopped:
						self.stop()
					self.activeJobsHandler() # will remove any finished and break the while loop if so
				else:
					# Start the newly added jobs in active list
					self.activateNextJobs()
			else:
				# Should only enter the below code when all jobs are processed
				time.sleep(1)
				if self.stopped:
					self.stop()
				self.inactive_timeout_counter -= 1
				if self.debug:
					print("- WRQ(" + str(sys._getframe().f_lineno) +") " + self.name + ": Inactive timeout in: " + str(self.inactive_timeout_counter) + " seconds -")
					self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": Inactive timeout in: " + str(self.inactive_timeout_counter) + " seconds"] )
		else:
			print("- WRQ(" + str(sys._getframe().f_lineno) +"): " + self.name + " is exiting due to no new jobs added. -")
			self.log_file.writeLinesToFile([str(sys._getframe().f_lineno) + self.name + ": is exiting due to no new jobs added."] )
			while not self.clearJobs():
				time.sleep(3)
			else:
				sys.exit(0)

		