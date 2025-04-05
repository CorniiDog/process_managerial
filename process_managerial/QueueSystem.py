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
from typing import Any, Callable, Optional, List, Dict
from . import toolbox
import os
import pickle as pkl
from enum import Enum
import datetime
import time

class QueueStatus(Enum):
    """
    Enumeration for representing the status of a queued function.

    Attributes:
        STOPPED (int): Indicates the task was stopped before completion.
        RETURNED_ERROR (int): Indicates the task finished with an error.
        RETURNED_CLEAN (int): Indicates the task finished successfully.
        RUNNING (int): Indicates the task is currently running.
        QUEUED (int): Indicates the task is waiting in the queue.
        CREATED (int): Indicates the task has been created but not yet queued.
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
        """
        Initializes a new instance of FunctionPropertiesStruct.

        Args:
            unique_hex (str): Unique identifier for the task.
            func (Callable): The function to execute.
            args (tuple): Positional arguments for the function.
            kwargs (dict, optional): Keyword arguments for the function. Defaults to an empty dict.
            start_time (datetime.datetime, optional): Time when the task was created; defaults to current UTC time.
            end_time (Optional[datetime.datetime], optional): Time when the task finished execution; defaults to None.
            status (QueueStatus, optional): Initial status of the task; defaults to CREATED.
            output (str, optional): Output or error messages; defaults to an empty string.
            keep_indefinitely (bool, optional): If True, the task will not be auto-cleared; defaults to False.
            result (Any, optional): The result returned by the function; defaults to None.
        """
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
    to be processed. Task statuses and results are persisted to pickle files in a specified
    processing directory.
    
    Attributes:
        q (queue.Queue): A thread-safe queue holding FunctionPropertiesStruct instances.
        is_running (bool): Flag indicating whether the worker thread should continue running.
        process_dir (str): Directory path for storing task pickle files.
        logger (logging.Logger): Logger instance for recording system events.
        time_to_wait (int): Maximum time (in seconds) to wait for a task before assuming it is finished.
        time_increment (float): Sleep interval (in seconds) for polling task status.
    """
    def __init__(self, process_dir: str = "processes", log_path: Optional[str] = "queue_log.txt", clear_hexes_after_days: int = -1):
        """
        Initializes the QueueSystem.
        
        Sets up the internal queue, logging configuration, and processing directory for storing task pickle files.
        Optionally clears old task files based on the clear_hexes_after_days parameter, and marks tasks from previous
        sessions as stopped.
        
        Args:
            process_dir (str): Path to the directory for storing task pickle files.
            log_path (Optional[str]): Path to the log file for recording events.
            clear_hexes_after_days (int): 
                If 0, clears all stored tasks.
                If greater than 0, clears tasks with a start_time older than the specified number of days.
                If less than 0, no clearing is performed.
        """
        self.q = queue.Queue()
        self.is_running = False
        self._mutex = threading.Lock()
        self.process_dir = process_dir

        self.time_to_wait = 30  # Time to wait for an erraneous issue
        self.time_increment = 0.01  # Incremental time
        
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
        
        Iterates through all stored task identifiers and updates the status of any tasks that were either
        QUEUED or RUNNING to STOPPED. This prevents tasks from a previous session from being left in an indeterminate state.
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
        
        Resets the task's status and timing attributes, updates its stored status, and places it back onto the queue for reprocessing.
        
        Args:
            unique_hex (str): The unique identifier of the task to be requeued.
        """
        function_properties = self.get_properties(unique_hex)
        if function_properties:
            function_properties.status = QueueStatus.QUEUED
            function_properties.end_time = None
            function_properties.start_time = datetime.datetime.now(tz=datetime.timezone.utc)
            function_properties.result = None
            self._update_status(function_properties)
            self.q.put(function_properties)

    def clear_hexes(self, before_date: datetime.datetime = None):
        """
        Removes stored task pickle files based on a given date.
        
        If before_date is provided, only tasks with a start_time earlier than before_date are removed.
        If before_date is None, all task pickle files in the process directory are removed.
        Tasks marked with keep_indefinitely=True are not removed.
        
        Args:
            before_date (datetime.datetime, optional): The datetime threshold. Tasks with a start_time
                                                         earlier than this will be removed.
        """
        # Retrieve the list of task identifiers using the thread-safe get_hexes method.
        hexes = self.get_hexes()
        for hex_val in hexes:
            task = self.get_properties(hex_val)
            with self._mutex:
                if task is None or before_date is None or task.start_time < before_date:
                    if task and task.keep_indefinitely:
                        continue
                    pkl_path = os.path.join(self.process_dir, hex_val + ".pkl")
                    try:
                        os.remove(pkl_path)
                        self.logger.info(f"Removed task {hex_val}")
                    except Exception as e:
                        self.logger.error(f"Error removing task {hex_val}: {e}")

    def get_hexes_after(self, after_time: datetime.datetime) -> List[str]:
        """
        Retrieves a list of task identifiers for tasks with a start_time after a specified datetime.
        
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
        sorted by the task's start_time.
        
        Returns:
            List[str]: A list of unique hexadecimal identifiers for stored tasks, sorted in ascending order by start_time.
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
            tasks.sort(key=lambda item: item[1])
            return [hex_val for hex_val, _ in tasks]

    def cancel_queue(self, unique_hex: str) -> bool:
        """
        Cancels a queued task identified by its unique hexadecimal identifier.
        
        Checks if the task exists and is in the QUEUED state, then removes its pickle file and
        filters it out from the internal queue, ensuring it will not be executed.
        
        Args:
            unique_hex (str): The unique identifier of the task to be cancelled.
        
        Returns:
            bool: True if the task was successfully cancelled; False otherwise.
        """
        with self._mutex:
            task = self.get_properties(unique_hex)
            if not task or task.status != QueueStatus.QUEUED:
                return False
            pkl_path = os.path.join(self.process_dir, unique_hex + ".pkl")
            try:
                os.remove(pkl_path)
            except Exception as e:
                self.logger.error(f"Error removing task {unique_hex}: {e}")
                return False

        with self.q.mutex:
            filtered_queue = [item for item in list(self.q.queue) if item.unique_hex != unique_hex]
            self.q.queue.clear()
            self.q.queue.extend(filtered_queue)
        
        self.logger.info(f"Cancelled task {unique_hex}")
        return True

    def get_all_hex_properties(self) -> List[FunctionPropertiesStruct]:
        """
        Retrieves a list of all task properties stored in the processing directory.
        
        Returns:
            List[FunctionPropertiesStruct]: A list of FunctionPropertiesStruct instances for all stored tasks.
        """
        hexes = self.get_hexes()
        results = []
        for hex_val in hexes:
            results.append(self.get_properties(hex_val))
        return results

    def _update_status(self, function_properties: FunctionPropertiesStruct) -> bool:
        """
        Updates the status of a task by saving its properties to a pickle file.
        
        Args:
            function_properties (FunctionPropertiesStruct): The task properties to update.
        
        Returns:
            bool: True if the status update was successful; False otherwise.
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
            Optional[FunctionPropertiesStruct]: The task properties if found; otherwise, None.
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
        
        Continuously retrieves tasks from the queue and executes them while the system is running.
        It updates each task's status and result, handles exceptions during function execution,
        and marks tasks as completed.
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
                    function_properties.output += f"Error executing {func.__name__}: {e}\n"
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
        
        Sets the running flag to True and launches the worker thread as a daemon. If the system is already running,
        logs a warning message.
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
        
        Sets the running flag to False. The worker thread will complete its current task before terminating.
        Note: Additional logic would be required if immediate termination or thread joining is desired.
        """
        self.logger.info("Stopping queue system...")
        self.is_running = False

    def queue_function(self, func: Callable, *args, **kwargs) -> str:
        """
        Adds a function and its arguments to the queue for asynchronous execution.
        
        Generates a unique hexadecimal identifier for the task, encapsulates the function and its parameters in a 
        FunctionPropertiesStruct, and enqueues the task for processing.
        
        Args:
            func (Callable): The function to be executed.
            *args: Positional arguments for the function.
            **kwargs: Keyword arguments for the function.
        
        Returns:
            str: The unique hexadecimal identifier associated with the queued task.
        """
        now, unique_hex = toolbox.generate_time_based_hash()

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
        Blocks the calling thread until the task with the specified unique hexadecimal identifier has finished processing.
        
        Continuously polls the task's stored status until it reaches one of the terminal states: RETURNED_CLEAN,
        RETURNED_ERROR, or STOPPED. If the task is not found within a specified time, the method logs that the task
        is assumed to be finished.
        
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
                emergency_yield = 0  # Reset the emergency yield timer
                if function_properties.status in (QueueStatus.RETURNED_CLEAN, QueueStatus.RETURNED_ERROR, QueueStatus.STOPPED):
                    self.logger.info(f"Task {unique_hex} has finished with status {function_properties.status.name}.")
                    break
            time.sleep(self.time_increment)

    def wait_until_finished(self):
        """
        Blocks the calling thread until all tasks in the queue have been processed.
        
        Waits until the internal count of unfinished tasks (tracked via task_done) reaches zero,
        ensuring that every enqueued task has been executed.
        """
        self.logger.info("Waiting for all tasks to complete...")
        self.q.join()
        self.logger.info("All tasks completed.")


class QueueSystemLite:
    """
    A lightweight version of QueueSystem that maintains task data in memory.
    
    Instead of persisting task data to disk using pickle files, QueueSystemLite stores all task properties
    in an in-memory list and dictionary. This class provides similar functionality to QueueSystem including
    queuing functions, processing tasks asynchronously in a background thread, and managing task statuses.
    
    Attributes:
        task_list (List[FunctionPropertiesStruct]): In-memory list representing the task queue.
        tasks (Dict[str, FunctionPropertiesStruct]): Dictionary mapping unique task identifiers to task properties.
        is_running (bool): Flag indicating whether the worker thread is running.
        _mutex (threading.Lock): Mutex for thread-safe access to shared in-memory data structures.
        time_to_wait (int): Maximum time to wait for a task during polling.
        time_increment (float): Sleep interval for polling.
        logger (logging.Logger): Logger instance for system events.
    """
    def __init__(self, log_path: Optional[str] = None):
        """
        Initializes the QueueSystemLite.
        
        Sets up the in-memory data structures for managing tasks and configures the logging mechanism.
        
        Args:
            log_path (Optional[str]): Path to the log file for recording events. If not provided, basic logging is configured.
        """
        self.task_list: List[FunctionPropertiesStruct] = []  # In-memory queue of tasks
        self.tasks: Dict[str, FunctionPropertiesStruct] = {}  # Map unique_hex -> task properties
        self.is_running = False
        self._mutex = threading.Lock()
        self.time_to_wait = 30  # Maximum wait time for a task
        self.time_increment = 0.01  # Polling interval

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if log_path:
            handler = logging.FileHandler(log_path)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        else:
            logging.basicConfig(level=logging.INFO)

    def queue_function(self, func: Callable, *args, **kwargs) -> str:
        """
        Queues a function for asynchronous execution in memory.
        
        Generates a unique identifier for the task, creates a FunctionPropertiesStruct instance,
        and adds it to both the task list and the tasks dictionary.
        
        Args:
            func (Callable): The function to execute.
            *args: Positional arguments for the function.
            **kwargs: Keyword arguments for the function.
        
        Returns:
            str: The unique hexadecimal identifier of the queued task.
        """
        now, unique_hex = toolbox.generate_time_based_hash()
        with self._mutex:
            while unique_hex in self.tasks:
                now, unique_hex = toolbox.generate_time_based_hash()
            task = FunctionPropertiesStruct(
                unique_hex=unique_hex,
                func=func,
                args=args,
                kwargs=kwargs,
                start_time=now,
                status=QueueStatus.QUEUED
            )
            self.task_list.append(task)
            self.tasks[unique_hex] = task
            if not self.is_running:
                self.logger.warning("Queue system is not running. Task added but won't be processed until started.")
        return unique_hex

    def _worker(self):
        """
        Internal worker method executed by a background thread.
        
        Continuously retrieves tasks from the in-memory task list and processes them.
        Updates each task's status and result accordingly. If no tasks are available, the worker sleeps briefly.
        """
        while self.is_running:
            task = None
            with self._mutex:
                if self.task_list:
                    task = self.task_list.pop(0)
            if task:
                self.logger.info(f"Working on {task.func.__name__}")
                task.status = QueueStatus.RUNNING
                try:
                    result = task.func(*task.args, **task.kwargs)
                    task.status = QueueStatus.RETURNED_CLEAN
                    task.result = result
                    task.end_time = datetime.datetime.now(tz=datetime.timezone.utc)
                except Exception as e:
                    task.status = QueueStatus.RETURNED_ERROR
                    task.output += f"Error executing {task.func.__name__}: {e}\n"
                    task.end_time = datetime.datetime.now(tz=datetime.timezone.utc)
                    self.logger.error(f"Error executing {task.func.__name__}: {e}")
                self.logger.info(f"Finished {task.func.__name__}")
            else:
                time.sleep(0.1)

    def start_queuesystem(self):
        """
        Starts the background worker thread for processing in-memory tasks.
        
        Sets the running flag to True and initiates the worker thread as a daemon. Logs the event.
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
        Signals the background worker thread to stop processing tasks.
        
        Sets the running flag to False, which causes the worker thread to exit after completing its current task.
        """
        self.logger.info("Stopping queue system...")
        self.is_running = False

    def wait_until_hex_finished(self, unique_hex: str):
        """
        Blocks until the task with the specified unique hexadecimal identifier has finished processing.
        
        Periodically polls the status of the task until it reaches a terminal state (RETURNED_CLEAN, RETURNED_ERROR, or STOPPED).
        
        Args:
            unique_hex (str): The unique identifier of the task.
        """
        emergency_yield = 0
        while True:
            with self._mutex:
                task = self.tasks.get(unique_hex)
            if task is None:
                emergency_yield += self.time_increment
                if emergency_yield > self.time_to_wait:
                    self.logger.info(f"Task {unique_hex} not found. Assuming it is finished.")
                    break
            else:
                emergency_yield = 0
                if task.status in (QueueStatus.RETURNED_CLEAN, QueueStatus.RETURNED_ERROR, QueueStatus.STOPPED):
                    self.logger.info(f"Task {unique_hex} has finished with status {task.status.name}.")
                    break
            time.sleep(self.time_increment)

    def wait_until_finished(self):
        """
        Blocks until all in-memory tasks have been processed.
        
        Continuously checks for any tasks that are still QUEUED or RUNNING and waits until all tasks have completed.
        """
        self.logger.info("Waiting for all tasks to complete...")
        while True:
            with self._mutex:
                pending = any(task.status in (QueueStatus.QUEUED, QueueStatus.RUNNING)
                              for task in self.tasks.values())
            if not pending:
                break
            time.sleep(self.time_increment)
        self.logger.info("All tasks completed.")

    def cancel_queue(self, unique_hex: str) -> bool:
        """
        Cancels a queued task if it is still pending.
        
        If the task with the specified unique_hex is in the QUEUED state, it is removed from the in-memory queue
        and its status is set to STOPPED.
        
        Args:
            unique_hex (str): The unique identifier of the task to cancel.
        
        Returns:
            bool: True if the task was successfully cancelled; False otherwise.
        """
        with self._mutex:
            task = self.tasks.get(unique_hex)
            if not task or task.status != QueueStatus.QUEUED:
                return False
            self.task_list = [t for t in self.task_list if t.unique_hex != unique_hex]
            task.status = QueueStatus.STOPPED
            task.end_time = datetime.datetime.now(tz=datetime.timezone.utc)
        self.logger.info(f"Cancelled task {unique_hex}")
        return True

    def get_properties(self, unique_hex: str) -> Optional[FunctionPropertiesStruct]:
        """
        Retrieves the properties of an in-memory task using its unique identifier.
        
        Args:
            unique_hex (str): The unique identifier of the task.
        
        Returns:
            Optional[FunctionPropertiesStruct]: The task properties if found; otherwise, None.
        """
        with self._mutex:
            return self.tasks.get(unique_hex)

    def get_all_hex_properties(self) -> List[FunctionPropertiesStruct]:
        """
        Returns a list of all in-memory task properties.
        
        Returns:
            List[FunctionPropertiesStruct]: A list containing the properties of all queued tasks.
        """
        with self._mutex:
            return list(self.tasks.values())

    def requeue_hex(self, unique_hex: str):
        """
        Requeues a task identified by its unique hexadecimal identifier.
        
        Resets the task's status and timing attributes, and appends it back to the in-memory queue for reprocessing.
        
        Args:
            unique_hex (str): The unique identifier of the task to requeue.
        """
        with self._mutex:
            task = self.tasks.get(unique_hex)
            if task:
                task.status = QueueStatus.QUEUED
                task.start_time = datetime.datetime.now(tz=datetime.timezone.utc)
                task.end_time = None
                task.result = None
                self.task_list.append(task)
                self.logger.info(f"Requeued task {unique_hex}")
