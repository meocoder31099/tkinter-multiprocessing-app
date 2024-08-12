from asyncio import create_task as asyncio_create_task
from asyncio import run as asyncio_run
from asyncio import wait as asyncio_wait
from csv import writer as csv_writer
from datetime import datetime
from json import loads as json_loads
from multiprocessing.connection import Connection
from re import sub as re_sub
from typing import Dict, List

from msgpack import unpackb as msgpack_unpackb
from numpy import arange as np_arange
from numpy import array as np_array
from numpy import cumsum as np_cumsum
from numpy import datetime64 as np_datetime64
from numpy import dtype as np_dtype
from numpy import frombuffer as np_frombuffer
from numpy import timedelta64 as np_timedelta64
from numpy import void as np_void

from ..exceptions import FileWriteError


# Remove special characters from the name and also replace spaces with underscores.
def safe_name(string: str) -> str:
    string = re_sub(r"[^a-zA-Z0-9 ]", "", string)
    string = string.replace(" ", "_") or "no_name_" + str(
        datetime.now().timestamp()
    ).replace(".", "")
    return string


# Decode encoded data with list type
def list_decode_data(raw_data: str) -> Dict | List:
    if raw_data.startswith(b"{"):
        return [json_loads(raw_data)]
    elif raw_data.startswith(b"["):
        return json_loads(raw_data)
    else:
        return msgpack_unpackb(raw_data, raw=False)


# Calculate the total number of bytes of a record
def calculate_bytes_of_record(format_string):
    format_size_dict = {
        "x": 0,
        "c": 1,
        "b": 1,
        "B": 1,
        "?": 1,
        "h": 2,
        "H": 2,
        "i": 4,
        "I": 4,
        "l": 4,
        "L": 4,
        "q": 8,
        "Q": 8,
        "e": 2,
        "f": 4,
        "d": 8,
    }
    return sum(format_size_dict[char] for char in format_string)


# Divide work among workers in the most equal way
def distribute_jobs(num_jobs, num_workers):
    # Calculate the average number of jobs per worker
    average_jobs_per_worker = num_jobs // num_workers

    # Calculate the remainder of jobs after evenly distributing
    remainder = num_jobs % num_workers

    # Create an array to store the range of jobs for each worker
    job_ranges = []

    # Initialize the start and end index of the job range
    start = 0
    end = 0

    # Distribute the jobs to each worker
    for i in range(num_workers):
        # Calculate the end index of the job range
        end = start + average_jobs_per_worker

        # Add one extra job to the range if there's a remainder
        if remainder > 0:
            end += 1
            remainder -= 1

        # Append the job range to the list
        job_ranges.append((start, end))

        # Update the start index for the next worker
        start = end

    return job_ranges


def parse_date(date_string):
    # New format for time strings containing timezone information
    time_format_new = "%Y-%m-%dT%H:%M:%S%z"
    # Old format for time strings without timezone information
    time_format_old = "%Y-%m-%dT%H:%M:%SZ"

    try:
        # Try parsing the time string with the new format
        time_obj = datetime.strptime(date_string, time_format_new)
        return time_obj
    except ValueError:
        try:
            # If unsuccessful, try parsing with the old format
            time_obj = datetime.strptime(date_string, time_format_old)
            return time_obj
        except ValueError:
            # If both attempts fail, return an error message
            return "Unable to parse time."


def datatime_to_timestamp(date_string) -> float:
    # print(date_string)
    # Date needs to be converted
    datetime_value = parse_date(date_string)
    datetime_value = np_datetime64(datetime_value)

    # Epoch is the start of the UNIX time series (1970-01-01T00:00:00)
    epoch = np_datetime64("1970-01-01T00:00:00")

    # Convert datetime64 to timestamp
    start_nano_time = (datetime_value - epoch) / np_timedelta64(1, "ms")
    return start_nano_time


def compute_sample_periods(
    sample_rate: int,
    total_values: int,
    measured_timestamp: str,
) -> list[list, list]:
    # The interval between sampling times, measured in ms
    sample_period = 1000 / sample_rate

    # Create array of milliseconds
    miliseconds = np_arange(
        start=0, stop=total_values * sample_period, step=sample_period
    )

    # Create an array of timestamps based on milliseconds
    timestamp_start = datatime_to_timestamp(measured_timestamp)
    timestamps = timestamp_start + miliseconds
    timestamps = np_array(timestamps, dtype="datetime64[ms]")

    return [timestamps, miliseconds]


def compute_sample_periods_unit_ms(
    data_decoded: list,
    measured_timestamp: str,
) -> list[list, list]:

    # Create array of milliseconds
    miliseconds = np_cumsum(data_decoded)

    # Create an array of timestamps based on milliseconds
    timestamp_start = datatime_to_timestamp(measured_timestamp)
    timestamps = timestamp_start + miliseconds
    timestamps = np_array(timestamps, dtype="datetime64[ms]")

    return [timestamps, miliseconds]


# Decode sample data
def sample_data_decode(data_enc, raw_data: bytes) -> list[tuple[float, ...]]:
    if data_enc == "list":
        unpacked_data = list_decode_data(raw_data=raw_data)
    else:
        format_string = "".join(format_char for _, format_char in data_enc)
        dtype = format_string_to_numpy_dtype(fromat_string=format_string)
        unpacked_data = np_frombuffer(raw_data, dtype=dtype)
        unpacked_data = unpacked_data.reshape(-1)
    return unpacked_data


def get_dtype_from_fromat(fromat_char: str) -> np_dtype:
    return np_dtype("<i") if fromat_char == "l" else np_dtype("<" + fromat_char)


def format_string_to_numpy_dtype(fromat_string: str) -> np_dtype:
    fromats = []
    if len(fromat_string) == 1:
        return get_dtype_from_fromat(fromat_char=fromat_string)
    for fromat_char in fromat_string:
        fromats.append(("", get_dtype_from_fromat(fromat_char=fromat_char)))
    return np_dtype(fromats)


# Convert decoded data to string
def data_fromat(data: float | tuple) -> str:
    if type(data) is not np_void:
        return "%f" % (data)
    elif len(data) == 1:
        return "%f" % (data[0])
    elif len(data) == 2:
        return "%f %f" % (data[0], data[1])
    elif len(data) == 3:
        return "%f %f %f" % (data[0], data[1], data[2])
    else:
        return " ".join([f"{v:.6f}" for v in data])


def float_to_string(float_number: float) -> str:
    return "%f" % (float_number)


# Workers decompress KDF files
def worker_KDF_extract(
    data_enc,
    unit: str,
    raw_data: bytes,
    measured_timestamp: str,
    total_values: int,
    sample_rate: str,
    channel_label: str,
    channel_type: str,
    file_name: str,
    path_save_data: str,
    pipe: Connection,
    task_id: int,
):
    try:
        unpacked_data = sample_data_decode(data_enc=data_enc, raw_data=raw_data)
        miliseconds = None
        timestamps = None

        # Set sennor type name
        channel_type = (
            "H10"
            if channel_label == "ECG"
            else "VS" if channel_label == "PPG" else channel_label
        )

        # The unpacked_data is a JSON and needs to be converted to a list of strings
        if data_enc == "list":
            if isinstance(unpacked_data, list):
                unpacked_data = [
                    ", ".join([f"{key}: {value}" for key, value in value.items()])
                    for value in unpacked_data
                ]
            else:
                unpacked_data = [
                    ", ".join(
                        [f"{key}: {value}" for key, value in unpacked_data.items()]
                    )
                ]

        DURATION = None
        DATAPOINTS = len(unpacked_data)

        # Sensors given 'ms' will calculate milliseconds by summing the values ​​from the decoded data
        if unit == "ms":
            timestamps, miliseconds = compute_sample_periods_unit_ms(
                data_decoded=unpacked_data,
                measured_timestamp=measured_timestamp,
            )
        # Sensors with an encoding data type of "list" will not have a timestamp
        elif data_enc != "list":
            timestamps, miliseconds = compute_sample_periods(
                sample_rate=sample_rate,
                measured_timestamp=measured_timestamp,
                total_values=total_values,
            )

        # Format data from float to string, used for writing data to file
        if data_enc != "list":
            miliseconds = [float_to_string(ms) for ms in miliseconds]
            unpacked_data = [data_fromat(data) for data in unpacked_data]
            DURATION = miliseconds[-1]

        if data_enc == "list":
            # List data will not have timestamps and miliseconds values ​​so it needs to be initialized to 'N/A'
            # It is initialized to 2 times the number of elements contained in "unpacked_data" because this is a Generator,
            # it needs (len(unpacked_data) * 2) because it is used in two different functions to write to .txt and .csv files
            timestamps = ("N/A" for _ in range(len(unpacked_data) * 2))
            miliseconds = ("N/A" for _ in range(len(unpacked_data) * 2))
            DURATION = "N/A"

        OSC_file_path = f"{path_save_data}/{channel_label}.txt"
        CSV_file_path = f"{path_save_data}/{channel_label}.csv"
        asyncio_run(
            write_file(
                OSC_file_path=OSC_file_path,
                CSV_file_path=CSV_file_path,
                channel_label=channel_label,
                channel_type=channel_type,
                unpacked_data=unpacked_data,
                miliseconds=miliseconds,
                timestamps=timestamps,
                file_name=file_name,
                DATAPOINTS=DATAPOINTS,
                DURATION=DURATION,
                pipe=pipe,
                task_id=task_id,
            )
        )
    except Exception as e:
        print(f"loi {e}")


async def write_file(
    OSC_file_path: str,
    CSV_file_path: str,
    channel_label: str,
    channel_type: str,
    unpacked_data: list,
    timestamps: list,
    miliseconds: list,
    file_name: str,
    DATAPOINTS: int,
    DURATION: str,
    pipe: Connection,
    task_id: int,
):

    channel_data = f"{file_name}/{channel_type}/{channel_label}"

    tasks = []
    OSC_kwargs = {
        "OSC_file_path": OSC_file_path,
        "channel_data": channel_data,
        "sennor_data": zip(timestamps, miliseconds, unpacked_data),
        "DURATION": DURATION,
        "DATAPOINTS": DATAPOINTS,
    }
    CSV_kwargs = {
        "CSV_file_path": CSV_file_path,
        "channel_type": channel_type,
        "channel_label": channel_label,
        "file_name": file_name,
        "sennor_data": zip(timestamps, miliseconds, unpacked_data),
    }
    # Add write_CSV_file to tasks
    task = asyncio_create_task(write_CSV_file(**CSV_kwargs))
    task.set_name(CSV_file_path)
    task.add_done_callback(
        lambda t: pipe.send(
            {
                "task_id": task_id,
                "message": f"{t.get_name()} - {t.exception().__str__() if t.exception() else "saved"}",
            }
        )
    )
    tasks.append(task)
    # Add write_OSC_file to tasks
    task = asyncio_create_task(write_OSC_file(**OSC_kwargs))
    task.set_name(OSC_file_path)
    task.add_done_callback(
        lambda t: pipe.send(
            {
                "task_id": task_id,
                "message": f"{t.get_name()} - {t.exception().__str__() if t.exception() else "saved"}",
            }
        )
    )
    tasks.append(task)
    # Run tasks and waiting for it done
    await asyncio_wait(tasks)

    # Send a message notifying the task has been completed
    pipe.send({"task_id": task_id, "message": "end"})


async def write_OSC_file(
    OSC_file_path: str,
    channel_data: str,
    sennor_data: zip,
    DURATION: str,
    DATAPOINTS: int,
):
    try:
        with open(file=OSC_file_path, mode="w") as OSC_file:
            # Write header
            OSC_file.write("#DURATION %s\n" % DURATION)
            OSC_file.write("#DATAPOINTS %d\n" % DATAPOINTS)
            OSC_file.write("\n")
            # Write header
            for timestamps, miliseconds, unpacked_data in sennor_data:
                OSC_file.write(
                    "%s %s %s %s\n"
                    % (
                        timestamps,
                        miliseconds,
                        channel_data,
                        unpacked_data,
                    )
                )
    except:
        raise FileWriteError


async def write_CSV_file(
    CSV_file_path: str,
    channel_type: str,
    channel_label: str,
    file_name: str,
    sennor_data: zip,
):
    try:
        with open(file=CSV_file_path, mode="w", newline="") as CSV_file:
            csvwriter = csv_writer(CSV_file)
            for timestamps, miliseconds, unpacked_data in sennor_data:
                csvwriter.writerow(
                    [
                        timestamps,
                        miliseconds,
                        file_name,
                        channel_type,
                        channel_label,
                        unpacked_data,
                    ]
                )
    except:
        raise FileWriteError
