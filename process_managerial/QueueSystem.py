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
import copy


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
                 result: Any = None,
                 to_be_shelved: bool = False):
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
        self.to_be_shelved = to_be_shelved


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

        self.shelve_dir = 'shelved_hexes'
        os.makedirs(self.shelve_dir, exist_ok=True)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if log_path:
            handler = logging.FileHandler(log_path)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        else:
            logging.basicConfig(level=logging.INFO)

    def queue_function_shelved(self, func: Callable, *args, **kwargs) -> str:
        """
        Queues a function for asynchronous execution in memory. After execution, it 
        shelves the result in a permanent storage.
        
        Generates a unique identifier for the task, creates a FunctionPropertiesStruct instance,
        and adds it to both the task list and the tasks dictionary.
        
        Args:
            func (Callable): The function to execute.
            *args: Positional arguments for the function.
            **kwargs: Keyword arguments for the function.
        
        Returns:
            str: The unique hexadecimal identifier of the queued task.
        """
        return self._queue_function(func, True, *args, **kwargs)
    
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
        return self._queue_function(func, False, *args, **kwargs)

    def _queue_function(self, func: Callable, is_shelved:bool,  *args, **kwargs) -> str:
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
                status=QueueStatus.QUEUED,
                to_be_shelved=is_shelved

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

                try:
                    if task.to_be_shelved: # Shelve hex
                        self.shelve_hex(task.unique_hex)
                except:
                    pass

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
            self.thread = threading.Thread(target=self._worker, daemon=True)
            self.thread.start()
            self.logger.info("Queue system started.")
        else:
            self.logger.warning("Queue system already running.")

    def shelve_hex(self, unique_hex: str):
        """
        Shelves the hex into a .pkl file 

        Args:
            unique_hex (str): The unique hexcode identifier for the queue process
        """
        with self._mutex:
            os.makedirs(self.shelve_dir, exist_ok=True)
            shelve_path = os.path.join(self.shelve_dir, unique_hex + ".pkl")

            if os.path.exists(shelve_path):
                raise FileExistsError("Unique Hex Already Shelved")

            hex_shelved = self._get_properties(unique_hex)
            if hex_shelved: # If it exists
                with open(shelve_path, 'wb') as f:
                    pkl.dump(hex_shelved, f)
            else:
                raise ValueError("Cannot find or locate the hex in memory.")
        
    def list_shelved_hexes(self) -> List[str]:
        """
        Returns a list of shelved hexes

        Returns:
            List[str]: A list of shelved hexes
        """
        with self._mutex:
            os.makedirs(self.shelve_dir, exist_ok=True)
            unique_hexes = os.listdir(self.shelve_dir)
            unique_hexes = [unique_hex.replace(".pkl", "") for unique_hex in unique_hexes] # Replace pixkles with empty
            return unique_hexes
        
    def clear_shelved_hexes(self):
        """
        Clears all shelved hexes
        """
        with self._mutex:
            os.makedirs(self.shelve_dir, exist_ok=True)
            unique_hexes_files = os.listdir(self.shelve_dir)
            for unique_hex_file in unique_hexes_files:
                unique_hex_file_full = os.path.join(self.shelve_dir, unique_hex_file)
                os.remove(unique_hex_file_full)
    
    def get_shelved_hex(self, unique_hex:str) -> FunctionPropertiesStruct:
        """
        Returns a function properties from a shelved hex
        
        Args:
            unique_hex (str): The unique hexcode identifier from the queue process
        Returns:
            FunctionPropertiesStruct: The function properties that have data
        """
        with self._mutex:
            os.makedirs(self.shelve_dir, exist_ok=True)
            shelve_path = os.path.join(self.shelve_dir, unique_hex + ".pkl")

            if not os.path.exists(shelve_path):
                raise FileNotFoundError(f"Unique hex is not shelved: {unique_hex}.")

            with open(shelve_path, 'rb') as f:
                return pkl.load(f)
        
    def delete_shelved_hex(self, unique_hex:str):
        """
        Deletes a shelved hex 
        
        Args:
            unique_hex (str): The unique hexcode identifier from the queue process
        """
        with self._mutex:
            os.makedirs(self.shelve_dir, exist_ok=True)
            shelve_path = os.path.join(self.shelve_dir, unique_hex + ".pkl")

            if not os.path.exists(shelve_path):
                raise FileNotFoundError(f"Unique hex is not shelved: {unique_hex}.")
            
            os.remove(shelve_path) # Remove the file


    def clear_hex(self, unique_hex:str):
        """
        Clears a specific hex, as long as it is not running
        
        Args:
            unique_hex (str): The hexcode to clear
        """
        with self._mutex:
            unique_hex_properties: FunctionPropertiesStruct = self._get_properties(unique_hex)
            if not unique_hex_properties or unique_hex_properties.status not in (QueueStatus.STOPPED, QueueStatus.RETURNED_CLEAN, QueueStatus.RETURNED_ERROR, QueueStatus.QUEUED):
                raise Exception("Cannot clear the hex. Either it does not exist or has not reached a stopping point")
            
            del self.tasks[unique_hex] # Delete key from hex
            self.task_list = [task for task in self.task_list if task.unique_hex != unique_hex]


    def clear_hexes(self, before_date: datetime.datetime = None):
        """
        Clears tasks from the in-memory storage based on a given date.
        
        If before_date is provided, only tasks with a start_time earlier than before_date are removed.
        If before_date is None, all tasks in memory (that are not marked with keep_indefinitely) are removed.
        This method removes tasks from both the tasks dictionary and the task_list.
        
        Args:
            before_date (datetime.datetime, optional): The datetime threshold. Tasks with a start_time
                                                         earlier than this will be cleared.
        """
        with self._mutex:
            keys_to_remove = []
            for unique_hex, task in self.tasks.items():
                if not task.keep_indefinitely and (before_date is None or task.start_time < before_date) and not (task.status in (QueueStatus.CREATED, QueueStatus.QUEUED, QueueStatus.RUNNING)):
                    keys_to_remove.append(unique_hex)
            for key in keys_to_remove:
                del self.tasks[key]
            self.task_list = [task for task in self.task_list if task.unique_hex not in keys_to_remove]
            if keys_to_remove:
                self.logger.info(f"Cleared tasks: {', '.join(keys_to_remove)}")
            else:
                self.logger.info("No tasks were cleared.")

    def stop_queuesystem(self):
        """
        Signals the background worker thread to stop processing tasks.
        
        Sets the running flag to False, which causes the worker thread to exit after completing its current task.
        """
        self.logger.info("Stopping queue system...")
        self.is_running = False
        self.thread.join() # Join into main

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
    
    def _get_properties(self, unique_hex: str, data_safe:bool = True, exclude_result = False) -> Optional[FunctionPropertiesStruct]:
        data = self.tasks.get(unique_hex)
        if data is not None:
            data = copy.deepcopy(data)  # Standard deepcopy call.
            if data_safe:
                data.func = data.func.__name__
            if exclude_result:
                data.result = None
        return data

    def get_properties(self, unique_hex: str, data_safe:bool = True, exclude_result = False) -> Optional[FunctionPropertiesStruct]:
        """
        Retrieves the properties of a task using its unique identifier.
        
        Args:
            unique_hex (str): The unique identifier of the task.
            data_safe (bool): Return a data-safe properties dict that is pickle-able
            exclude_result (bool) : Set to true to exclude the result, for optimization purposes
        
        Returns:
            Optional[FunctionPropertiesStruct]: The task properties if found; otherwise, None.
        """
        with self._mutex:
            return self._get_properties(unique_hex=unique_hex, data_safe=data_safe, exclude_result=exclude_result)


    def get_all_hex_properties(self, data_safe:bool=True, exclude_result:bool = False) -> List[FunctionPropertiesStruct]:
        """
        Retrieves a list of all task properties stored in the processing directory.
        
        Returns:
            List[FunctionPropertiesStruct]: A list of FunctionPropertiesStruct instances for all stored tasks.
        """
        with self._mutex:
            hexes = self.get_hexes()
            results = []
            for hex_val in hexes:
                results.append(self._get_properties(hex_val, data_safe=data_safe, exclude_result=exclude_result))
            return results
        
    def get_hexes(self) -> List[str]:
        """
        Returns a list of all in-memory task property hexes.
        
        Returns:
            List[str]: A list containing the hex codes of all properties
        """
        with self._mutex:
            return list(self.tasks.keys())

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
