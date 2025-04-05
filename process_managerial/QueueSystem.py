"""
Module: QueueSystem
Description:
    This module implements a QueueSystem class that manages a queue of functions to be executed
    asynchronously in a background worker thread. It provides methods to queue functions, start and
    stop the worker, and wait for all queued tasks to complete. Additionally, the status and results of
    the executed functions can be stored and retrieved via pickle files when a processing directory is provided.
"""

import threading
import queue
import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Callable, Optional, List
from . import toolbox
import os
import pickle as pkl
from enum import Enum
import datetime
import time

class QueueStatus(Enum):
    """
    Enumeration for representing the status of a queued function.
    """
    STOPPED = -2
    RETURNED_ERROR = -1
    RETURNED_CLEAN = 0
    RUNNING = 1
    QUEUED = 2
    CREATED = 3


class FunctionPropertiesStruct:
    """
    Structure holding the properties of a queued function, including metadata and execution results.
    
    Attributes:
        unique_hex (str): A unique identifier for the task.
        func (Callable): The function to be executed.
        args (tuple): A tuple of positional arguments for the function.
        kwargs (dict): A dictionary of keyword arguments for the function.
        start_time (datetime.datetime): The timestamp when the task was added.
        end_time (Optional[datetime.datetime]): The timestamp when the task completed execution.
        status (QueueStatus): The current status of the task.
        output (str): The output message or error message if an exception occurs.
        result (Any): The result returned by the function.
        keep_indefinitely (bool): If True, the task will not be automatically cleared.
    """
    def __init__(self, 
                 unique_hex: str,
                 func: Callable,
                 args: tuple,
                 kwargs: dict = None,
                 start_time: datetime.datetime = None,
                 end_time: Optional[datetime.datetime] = None,
                 status: QueueStatus = QueueStatus.CREATED,
                 output: str = "",
                 keep_indefinitely: bool = False,
                 result: Any = None):
        self.unique_hex = unique_hex
        self.func = func
        self.args = args
        self.kwargs = kwargs if kwargs is not None else {}
        self.start_time = start_time or datetime.datetime.now(tz=datetime.timezone.utc)
        self.end_time = end_time
        self.status = status
        self.output = output
        self.result = result
        self.keep_indefinitely = keep_indefinitely



class QueueSystem:
    """
    Manages a queue of functions to be executed asynchronously in a background thread.
    
    This class provides a simple way to offload function calls to a worker thread,
    allowing the main thread to continue execution without waiting for each function
    to complete. It supports starting and stopping the worker thread, adding tasks
    (functions and their arguments) to the queue, and waiting for all queued tasks
    to be processed.
    
    Attributes:
        q (queue.Queue): A thread-safe queue holding FunctionPropertiesStruct instances.
        is_running (bool): Flag indicating whether the worker thread should continue running.
        process_dir (str): Directory path for storing task status and results via pickle files.
        logger (logging.Logger): Logger instance for recording system events.
    """
    def __init__(self, process_dir: str = "processes", log_path: Optional[str] = "queue_log.txt", clear_hexes_after_days: int = -1):
        """
        Initializes the QueueSystem.
        
        Sets up the internal queue and initializes the running state to False. If a
        processing directory is provided, ensures that it exists. Also sets up the
        logging configuration using the provided log file path.
        
        Args:
            process_dir (str): Path to the directory for storing task pickle files.
            log_path (Optional[str]): Path to the log file for recording events.
        """
        self.q = queue.Queue()
        self.is_running = False
        self._mutex = threading.Lock()
        self.process_dir = process_dir

        self.time_to_wait = 30 # Time to wait for an erraneous issue
        self.time_increment = 0.01 # Incremental time
        
        if process_dir:
            os.makedirs(process_dir, exist_ok=True)
        
        # Set up the logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if log_path:
            rotating_handler = RotatingFileHandler(log_path, maxBytes=1024*1024, backupCount=5)
            rotating_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            rotating_handler.setFormatter(formatter)
            self.logger.addHandler(rotating_handler)

        if clear_hexes_after_days == 0:
            self.clear_hexes()
        elif clear_hexes_after_days > 0:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            days_ago = now - datetime.timedelta(days=clear_hexes_after_days)
            self.clear_hexes(days_ago)
            
        self._signify_restarted()

    def _signify_restarted(self):
        """
        Marks tasks that were queued or running before a restart as stopped.
        
        Retrieves all stored task identifiers and updates the status of tasks that
        were either QUEUED or RUNNING to STOPPED.
        """
        hexes = self.get_hexes()
        for hex_val in hexes:
            function_properties = self.get_properties(hex_val)
            queued_enums = [QueueStatus.QUEUED, QueueStatus.RUNNING]
            if function_properties and function_properties.status in queued_enums:
                function_properties.status = QueueStatus.STOPPED
                function_properties.end_time = datetime.datetime.now(tz=datetime.timezone.utc)
                self._update_status(function_properties)

    def requeue_hex(self, unique_hex: str):
        """
        Requeues a task identified by its unique hexadecimal identifier.
        
        Sets the task status to QUEUED and updates its status file, then puts the
        task back onto the queue.
        
        Args:
            unique_hex (str): The unique identifier of the task.
        """
        function_properties = self.get_properties(unique_hex)
        if function_properties:
            function_properties.status = QueueStatus.QUEUED
            function_properties.end_time = None
            function_properties.start_time = datetime.datetime.now(tz=datetime.timezone.utc)
            function_properties.result = None
            self._update_status(function_properties)
            self.q.put(function_properties)

    def clear_hexes(self, before_date: datetime = None):
        """
        Removes stored task pickle files based on a given date.
        If before_date is provided, only tasks with a start_time earlier than before_date are removed.
        If before_date is None, all task pickle files in the process directory are removed.

        Args:
            before_date (datetime.datetime, optional): The datetime threshold. Tasks with start_time
                                                         earlier than this will be removed.
        """
        # Retrieve the list of hexes using the thread-safe get_hexes method.
        hexes = self.get_hexes()
        # Process each task outside of a global lock to avoid long blocking.
        for hex_val in hexes:
            task = self.get_properties(hex_val)
            with self._mutex:
                if task is None or before_date is None or task.start_time < before_date:
                    if task and task.keep_indefinitely: # Ignore if told to keep indefinitely
                        continue
                    
                    pkl_path = os.path.join(self.process_dir, hex_val + ".pkl")
                    try:
                        os.remove(pkl_path)
                        self.logger.info(f"Removed task {hex_val}")
                    except Exception as e:
                        self.logger.error(f"Error removing task {hex_val}: {e}")
            



    def get_hexes_after(self, after_time: datetime.datetime) -> List[str]:
        """
        Retrieves a list of task identifiers whose stored start_time is after the specified datetime.

        Args:
            after_time (datetime.datetime): The datetime threshold.

        Returns:
            List[str]: A list of unique hexadecimal identifiers for tasks with a start_time after after_time.
        """
        hexes_after = []
        for hex_val in self.get_hexes():
            task = self.get_properties(hex_val)
            if task and task.start_time > after_time:
                hexes_after.append(hex_val)
        return hexes_after


    def get_hexes(self) -> List[str]:
        """
        Retrieves a list of task identifiers based on the pickle files in the process directory,
        sorted by the task's start_time (process date).

        Returns:
            List[str]: A list of unique hexadecimal identifiers for stored tasks, sorted in ascending
                    order by their start_time.
        """
        with self._mutex:
            hex_files = [file for file in os.listdir(self.process_dir) if file.endswith('.pkl')] if self.process_dir else []
            tasks = []
            for file in hex_files:
                hex_val = file[:-4]  # Remove the '.pkl' extension.
                pkl_path = os.path.join(self.process_dir, file)
                try:
                    with open(pkl_path, "rb") as f:
                        task = pkl.load(f)
                    tasks.append((hex_val, task.start_time))
                except Exception as e:
                    self.logger.error(f"Error loading properties for {hex_val}: {e}")
            # Sort the tasks based on their start_time.
            tasks.sort(key=lambda item: item[1])
            return [hex_val for hex_val, _ in tasks]

        
    def cancel_queue(self, unique_hex: str) -> bool:
        """
        Cancels a queued task identified by its unique hexadecimal identifier.

        This function checks whether the task corresponding to the given unique_hex exists
        and is in the QUEUED state. If the task is found and is queued, the function deletes
        its associated pickle file from the process directory and removes the task from the
        internal queue to ensure it will not be executed. It returns True if the cancellation
        is successful; otherwise, it returns False.

        Args:
            unique_hex (str): The unique hexadecimal identifier of the task to be cancelled.

        Returns:
            bool: True if the task was successfully cancelled, False otherwise.
        """
        # Acquire mutex to safely check and update the task's pickle file.
        with self._mutex:
            task = self.get_properties(unique_hex)
            # Only allow cancellation if the task exists and is in a QUEUED state.
            if not task or task.status != QueueStatus.QUEUED:
                return False
            pkl_path = os.path.join(self.process_dir, unique_hex + ".pkl")
            try:
                os.remove(pkl_path)
            except Exception as e:
                self.logger.error(f"Error removing task {unique_hex}: {e}")
                return False

        # Remove the task from the internal queue.
        with self.q.mutex:
            # Filter out the task with the specified unique_hex.
            filtered_queue = [item for item in list(self.q.queue) if item.unique_hex != unique_hex]
            self.q.queue.clear()
            self.q.queue.extend(filtered_queue)
        
        self.logger.info(f"Cancelled task {unique_hex}")
        return True


    def get_all_hex_properties(self) -> List[FunctionPropertiesStruct]:
        """
        Retrieves a list of all hexes and their properties.
        
        Returns:
            List[FunctionPropertiesStruct]: A list of unique hexes and results.
        """
        hexes = self.get_hexes()
        results = []
        for hex in hexes:
            results.append(self.get_properties(hex))

    def _update_status(self, function_properties: FunctionPropertiesStruct) -> bool:
        """
        Updates the status of a task by saving its properties to a pickle file.
        
        Args:
            function_properties (FunctionPropertiesStruct): The task properties to update.
            
        Returns:
            bool: True if the update was successful, False otherwise.
        """
        with self._mutex:
            if not self.process_dir:
                return False
            pkl_path = os.path.join(self.process_dir, function_properties.unique_hex + ".pkl")
            try:
                with open(pkl_path, "wb") as f:
                    pkl.dump(function_properties, f)
                return True
            except Exception as e:
                self.logger.error(f"Error updating status for {function_properties.unique_hex}: {e}")
                return False

    def get_properties(self, unique_hex: str) -> Optional[FunctionPropertiesStruct]:
        """
        Retrieves the properties of a task using its unique identifier.
        
        Args:
            unique_hex (str): The unique identifier of the task.
            
        Returns:
            Optional[FunctionPropertiesStruct]: The task properties if found, otherwise None.
        """
        with self._mutex:
            if not self.process_dir:
                return None
            pkl_path = os.path.join(self.process_dir, unique_hex + ".pkl")
            if os.path.exists(pkl_path):
                try:
                    with open(pkl_path, "rb") as f:
                        return pkl.load(f)
                except Exception as e:
                    self.logger.error(f"Error loading properties for {unique_hex}: {e}")
        return None

    def _worker(self):
        """
        Internal worker method executed by the background thread.
        
        Continuously fetches tasks from the queue and executes them as long as the
        `is_running` flag is True. The status and results of the task are updated
        and saved after execution. This method is intended for internal use.
        """
        while self.is_running:
            try:
                function_properties: FunctionPropertiesStruct = self.q.get(timeout=1)
                func = function_properties.func
                pos_args = function_properties.args
                kw_args = function_properties.kwargs
                self.logger.info(f"Working on {func.__name__}")
                function_properties.status = QueueStatus.RUNNING
                self._update_status(function_properties)
                
                try:
                    result = func(*pos_args, **kw_args)
                    function_properties.status = QueueStatus.RETURNED_CLEAN
                    function_properties.end_time = datetime.datetime.now(tz=datetime.timezone.utc)
                    function_properties.result = result
                    self._update_status(function_properties)
                except Exception as e:
                    function_properties.status = QueueStatus.RETURNED_ERROR
                    function_properties.output += f"Error executing {func.__name__}: {e}" + "\n"
                    function_properties.end_time = datetime.datetime.now(tz=datetime.timezone.utc)
                    self._update_status(function_properties)
                    self.logger.error(f"Error executing {func.__name__}: {e}")
                finally:
                    self.logger.info(f"Finished {func.__name__}")
                    self.q.task_done()
            except queue.Empty:
                continue


    def start_queuesystem(self):
        """
        Starts the background worker thread if it is not already running.
        
        If the system is not currently running, sets the `is_running` flag to True and
        launches the `_worker` method in a new daemon thread. If already running, a
        message is logged and no action is taken.
        """
        if not self.is_running:
            self.is_running = True
            thread = threading.Thread(target=self._worker, daemon=True)
            thread.start()
            self.logger.info("Queue system started.")
        else:
            self.logger.warning("Queue system already running.")

    def stop_queuesystem(self):
        """
        Signals the worker thread to stop processing new tasks.
        
        Sets the `is_running` flag to False. The worker thread will finish its current task
        (if any) and then exit its processing loop. This method does not wait for the thread
        to terminate or for the queue to be emptied.
        """
        self.logger.info("Stopping queue system...")
        self.is_running = False
        # Note: Additional logic would be required to join the thread if immediate termination is needed.

        def queue_function(self, func: Callable, *args, **kwargs) -> str:
            """
            Adds a function and its arguments to the queue for asynchronous execution.
            
            The function and its arguments are encapsulated in a FunctionPropertiesStruct and
            placed on the queue for processing by the worker thread.
            
            Args:
                func (Callable): The function to be executed.
                *args: Positional arguments for the function.
                **kwargs: Keyword arguments for the function.
                
            Returns:
                str: A unique hexadecimal identifier associated with the queued task.
            """
            now, unique_hex = toolbox.generate_time_based_hash()

            # Re-generate the unique_hex if it already exists in the process directory.
            while unique_hex in self.get_hexes():
                now, unique_hex = toolbox.generate_time_based_hash()

            function_properties = FunctionPropertiesStruct(
                unique_hex=unique_hex,
                func=func,
                args=args,
                kwargs=kwargs,
                start_time=now,
                status=QueueStatus.QUEUED
            )

            if not self.is_running:
                self.logger.warning("Warning: Queue system is not running. Task added but won't be processed until started.")
            self.q.put(function_properties)
            return unique_hex

    
    def wait_until_hex_finished(self, unique_hex: str):
        """
        Blocks the calling thread until the task with the given unique_hex has finished processing.
        
        It continuously checks the task's status stored in its pickle file and waits until the status
        is one of the terminal states: RETURNED_CLEAN, RETURNED_ERROR, or STOPPED.
        
        Args:
            unique_hex (str): The unique identifier of the task to wait for.
        """
        emergency_yield = 0
        while True:
            function_properties = self.get_properties(unique_hex)
            if function_properties is None:
                emergency_yield += self.time_increment
                if emergency_yield > self.time_to_wait:
                    self.logger.info(f"Task {unique_hex} not found. Assuming it is finished.")
                    break
            else:
                emergency_yield = 0 # Resets emergency yield
                if function_properties.status in (QueueStatus.RETURNED_CLEAN, QueueStatus.RETURNED_ERROR, QueueStatus.STOPPED):
                    self.logger.info(f"Task {unique_hex} has finished with status {function_properties.status.name}.")
                    break
            time.sleep(self.time_increment)
        

    def wait_until_finished(self):
        """
        Blocks the calling thread until all tasks in the queue have been processed.
        
        Waits until the internal count of unfinished tasks in the queue reaches zero. Each
        task is marked as complete by the worker thread via `task_done()`.
        """
        self.logger.info("Waiting for all tasks to complete...")
        self.q.join()
        self.logger.info("All tasks completed.")
