# Import typing
from concurrent.futures import ProcessPoolExecutor

# Import libs
from dataclasses import dataclass, field
from json import loads as json_loads
from multiprocessing import Pipe
from os import makedirs as os_makedirs
from os.path import basename as os_basename
from os.path import exists as os_exists
from os.path import splitext as os_splitext
from typing import BinaryIO, Dict, Optional

from msgpack import unpackb as msgpack_unpackb

from .exceptions import FileWriteError, HeaderNotFoundError, ParserDataError
from .utils import csv_writer, np_dtype, np_frombuffer, safe_name, worker_KDF_extract


@dataclass
class KDFExtractor:
    KDF_file_path: str
    path_save_data: str
    num_worker: int = field(default=1)
    KDF_file: Optional[BinaryIO] = field(default=None, init=False, repr=False)
    header_size: None | int = field(default=None, init=False)
    header: None | Dict[str, any] = field(default=None, init=False)
    file_name: str = field(init=False)

    def __del__(self):
        # Close KDF file if it is exist
        if self.KDF_file is not None:
            self.KDF_file.close()

    def __post_init__(self):
        try:
            filename_with_extension = os_basename(self.KDF_file_path)
            filename_without_extension = os_splitext(filename_with_extension)[0]
            self.file_name = safe_name(filename_without_extension)
            # Update the path to save the file and add a subfolder that is the name of the KDF file to be extracted
            self.path_save_data = f"{self.path_save_data}/{self.file_name}"

            # Create a directory containing the exported data files if it does not already exist
            if not os_exists(self.path_save_data):
                os_makedirs(self.path_save_data)

            # Read headers from KDF file
            # Open KDF file and save it to self.KDF_file
            self.KDF_file = open(self.KDF_file_path, "rb")
            # Read format_identifier
            file_format_identifier = self.KDF_file.read(7).decode("ascii")
            # Skip format_version because it is not use
            self.KDF_file.seek(10)
            # file_format_version = self.KDF_file.read(3).decode("ascii")
            self.header_size = np_frombuffer(
                self.KDF_file.read(4), dtype=np_dtype("<I"), count=1
            )[0]
            header_data = self.KDF_file.read(self.header_size)

            if file_format_identifier == "KDFJSON":
                self.header = json_loads(header_data)
            elif file_format_identifier == "KDFMSGP":
                self.header = msgpack_unpackb(header_data, raw=False)
        except:
            raise HeaderNotFoundError

    # Read channel data contained in KDF files
    def get_channel_data(self, on_event: callable, on_succes: callable):
        if self.header is None or self.header_size is None:
            raise HeaderNotFoundError

        if self.KDF_file is None:
            raise HeaderNotFoundError

        channels = self.header.get("channels", None)
        if channels is None:
            raise HeaderNotFoundError

        measured_timestamp = self.header["measured_timestamp"]

        # Create a pipe to communicate between main process and child process
        parent_pipe, child_pipe = Pipe()
        # ID of the tasks assigned to the worker
        task_ids = []
        # Contains the names of the channel_labels, which will be used to merge the CSV files of the channels into one data.csv file
        part_names = []
        with ProcessPoolExecutor(max_workers=self.num_worker) as executor:
            for task_id, channel in enumerate(channels):
                data_enc = channel["data_enc"]
                data_size = channel["data_size"]
                # data_url is data offset
                data_url = channel["data_url"]
                # Sampling rate per second
                sample_rate = channel["sample_rate"]
                # The total number of samples was recorded
                total_values = channel["total_values"]
                unit = channel["unit"]
                channel_label = channel["label"]

                # Add channel_labels to part_names, used to merge the individual CSV files of each sensor into one data.csv file
                part_names.append(channel_label)

                channel_type = channel["type"]

                # Parser data_size and data_offset to int
                try:
                    data_size = int(data_size)
                    data_url = int(data_url)
                except:
                    raise ParserDataError

                self.KDF_file.seek(self.header_size + 14 + data_url)
                raw_data = self.KDF_file.read(data_size)

                kwargs = {
                    "data_enc": data_enc,
                    "unit": unit,
                    "raw_data": raw_data,
                    "measured_timestamp": measured_timestamp,
                    "total_values": total_values,
                    "sample_rate": sample_rate,
                    "channel_label": channel_label,
                    "channel_type": channel_type,
                    "file_name": self.file_name,
                    "path_save_data": self.path_save_data,
                    "pipe": child_pipe,
                    "task_id": task_id,
                }
                executor.submit(worker_KDF_extract, **kwargs)
                task_ids.append(task_id)

            # Listen for events emitted from the child process and emit them out through the callback function
            while True:
                event = parent_pipe.recv()
                if event["message"] == "end":
                    task_ids.pop()
                    if len(task_ids) == 0:
                        break
                else:
                    on_event(event)

            # Close the stream, no more events will be emitted
            child_pipe.close()
            parent_pipe.close()

            try:
                # Write CSV file with complete data (Contains data of all sensors)
                data_path = f"{self.path_save_data}/data.csv"
                with open(file=data_path, mode="w", newline="") as CSV_file:
                    header = [
                        "Timestamp",
                        "Milliseconds",
                        "FileName",
                        "SensorType",
                        "Channel",
                        "Data",
                    ]
                    csvwriter = csv_writer(CSV_file)
                    # Write CSV headers
                    csvwriter.writerow(header)
                    for part_name in part_names:
                        with open(
                            f"{self.path_save_data}/{part_name}.csv", "r"
                        ) as part_file:
                            CSV_file.write(part_file.read())
                on_event(
                    {"task_id": "write_data.csv", "message": f"{data_path} - saved"}
                )

            except:
                on_event(
                    {
                        "task_id": "write_data.csv",
                        "message": f"data.csv - {FileWriteError}",
                    }
                )

            on_succes()
            # for future in futures:
            #     future.result()

            # format_string = "".join(format_char for _, format_char in data_enc)
            # record_size = calculate_bytes_of_record(format_string)
            # num_records = len(raw_data) // record_size

            # worker_jobs = distribute_jobs(
            #     num_jobs=num_records, num_workers=self.num_worker
            # )

            # print(data_enc)

            # unpacked_data = sample_data_decode(data_enc=data_enc, raw_data=raw_data)
            # print(unpacked_data[:2])

            # if data_enc == "list":
            #     continue

            # if unit == "ms":
            #     timestamps, miliseconds = compute_sample_periods_unit_ms(
            #         data_decoded=unpacked_data,
            #         measured_timestamp=measured_timestamp,
            #     )
            # else:
            #     timestamps, miliseconds = compute_sample_periods(
            #         sample_rate=sample_rate,
            #         measured_timestamp=measured_timestamp,
            #         total_values=total_values,
            #     )

            # all_data = zip(timestamps, miliseconds, unpacked_data)

            # format_string = "".join(format_char for _, format_char in data_enc)
            # record_size = calculate_bytes_of_record(format_string)
            # if record_size != 12:
            #     continue
            # with open("../../output.txt", "w") as file:
            #     for timestamps, miliseconds, unpacked_data in all_data:
            #         file.write(
            #             "%s %s %s\n"
            #             % (
            #                 timestamps,
            #                 float_to_string(miliseconds),
            #                 data_fromat(unpacked_data),
            #             )
            #         )

            # print(data_enc)
            # print(format_string)
            # print(record_size)
            # print(worker_jobs)

        on_event({"task_id": None, "message": "Extracted files successfully"})


if __name__ == "__main__":
    KDF_file_path = "../../test1.kdf"
    path_save_data = "../../out"
    on_event = lambda event: print(event)
    on_succes = lambda event: print(event)
    kdf_extractor = KDFExtractor(
        KDF_file_path=KDF_file_path,
        path_save_data=path_save_data,
        num_worker=4,
    )

    print(kdf_extractor)

    import time

    start_time = time.time()

    print(kdf_extractor.get_channel_data(on_event=on_event, on_succes=on_succes))

    end_time = time.time()
    single_time = end_time - start_time
    print("Thời gian thực thi:", single_time)
