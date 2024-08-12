def read_and_filter_time_codes(file_path, start_time, end_time):
    filtered_time_codes = []
    try:
        with open(file_path, "r") as file:
            for line in file:
                # Removes whitespace and newline characters from the beginning and end of the line
                line = line.strip()
                # If the line is not empty, perform time code extraction
                if line and not line.startswith("#D"):
                    time_code = line.split(" ")[0]
                    # compare time code with start_time and end_time
                    if start_time and start_time > time_code:
                        continue
                    if end_time and end_time < time_code:
                        break
                    filtered_time_codes.append(line)
    except FileNotFoundError:
        print("The file does not exist or cannot be opened.")

    data_len = len(filtered_time_codes)
    if data_len != 0:
        last_time = filtered_time_codes[-1].split()[1]
        filtered_time_codes.insert(0, f"#DURATION {last_time}")
        filtered_time_codes.insert(1, f"#DATAPOINTS {data_len}")
        filtered_time_codes.insert(2, "")
    return filtered_time_codes


if __name__ == "__main__":
    file_path = "PPG.txt"

    start_time = "19:13:27.000"
    end_time = "19:13:60.000"
    filtered_time_codes = read_and_filter_time_codes(file_path, start_time, end_time)

    print("Time codes within the specified range:")
    for time_code in filtered_time_codes:
        print(time_code)
