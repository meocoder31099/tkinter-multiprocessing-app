from threading import Thread
from tkinter import filedialog

import customtkinter

from core.kdf_extractor import KDFExtractor
from core.txt_select_regions import read_and_filter_time_codes

# Top level window for select an outputted txt file,
# define regions on the txt file based on the time code,
# and save these regions
shared_data = {}


class SelectRegionsTLV(customtkinter.CTkToplevel):
    def __init__(self, master, *args, **kwargs):
        super().__init__(master, *args, **kwargs)
        self.title("TXT select")
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.window_width = 700
        self.window_height = 400

        # Get the size of the parent window
        app_width = master.master.winfo_width()
        app_height = master.master.winfo_height()

        # Get the position of the parent window
        x = master.master.winfo_x()
        y = master.master.winfo_y()

        # Get offsets
        dx = (app_width - self.window_width) // 2
        dy = (app_height - self.window_height) // 2

        # Set position of the toplevel window follow the position of the parent window
        self.geometry(
            "%dx%d+%d+%d" % (self.window_width, self.window_height, x + dx, y + dy)
        )

        self.checkbox_frame = SelectRegionsSideBarFrame(self)
        self.checkbox_frame.grid(row=0, column=0, padx=10, pady=10, sticky="ns")

        self.main_content_frame = SelectRegionsMainContentFrame(self)
        self.main_content_frame.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")


class SelectRegionsSideBarFrame(customtkinter.CTkFrame):
    def __init__(self, master):
        super().__init__(master)

        # Button select an outputted txt file,
        # define regions on the txt file based on the time code,
        # and save these regions.
        self.open_txt_file_btn = OpenFileTxtButton(master=self)
        shared_data.update({"open_txt_file_btn": self.open_txt_file_btn})
        self.open_txt_file_btn.grid(row=0, column=0, padx=10, pady=10, sticky="w")

        # Input time code
        self.timecode_start = customtkinter.CTkEntry(
            self,
            placeholder_text="F e.g: 19:41:41.000",
        )
        shared_data.update({"timecode_start_entry": self.timecode_start})
        self.timecode_start.grid(row=1, column=0, padx=10, pady=2, sticky="w")
        self.timecode_end = customtkinter.CTkEntry(
            self,
            placeholder_text="T e.g: 20:11:11.000",
        )
        shared_data.update({"timecode_end_entry": self.timecode_end})
        self.timecode_end.grid(row=2, column=0, padx=10, pady=2, sticky="w")

        # Button select output directory
        self.preview_btn = PreViewRegionsButton(master=self)
        shared_data.update({"preview_btn": self.preview_btn})
        self.preview_btn.grid(row=3, column=0, padx=10, pady=10, sticky="w")

        # Button run convert function
        self.export_btn = ExportButton(master=self)
        shared_data.update({"export_btn": self.export_btn})
        self.export_btn.grid(row=4, column=0, padx=10, pady=10, sticky="w")


class SelectRegionsMainContentFrame(customtkinter.CTkFrame):
    def __init__(self, master):
        super().__init__(master=master)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # File label
        self.txt_file_label = customtkinter.CTkLabel(
            self,
            text="Select the path to the Txt file",
            fg_color="transparent",
        )
        shared_data.update({"txt_file_label": self.txt_file_label})
        self.txt_file_label.grid(row=0, column=0, padx=20, pady=10, sticky="w")

        # Log textbox
        self.preview_textbox = customtkinter.CTkTextbox(self)
        shared_data.update({"preview_textbox": self.preview_textbox})
        self.preview_textbox.grid(
            row=1, column=0, padx=20, pady=(10, 20), sticky="nsew"
        )


class OpenFileTxtButton(customtkinter.CTkButton):
    def __init__(self, master):
        super().__init__(
            master, text="Choose file", command=self.button_callback, corner_radius=12
        )

    def button_callback(self):
        file_path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt")])
        # Focus to main window after choose file
        self.master.master.focus_force()

        shared_data.get("txt_file_label").configure(text=file_path)
        shared_data.update({"txt_file_path": file_path if file_path else None})
        if file_path == "":
            shared_data.get("txt_file_label").configure(
                text="Select the path to the Txt file"
            )
        # Check if selected file and output directory
        if file_path != "":
            shared_data.get("preview_btn").configure(state="normal")
        else:
            shared_data.get("preview_btn").configure(state="disabled")

        # Disable export button
        shared_data.get("export_btn").configure(state="disabled")

        # Reset time entry
        start_time = (
            shared_data.get("timecode_start_entry").get()
            if shared_data.get("timecode_start_entry", None)
            else None
        )
        end_time = (
            shared_data.get("timecode_end_entry").get()
            if shared_data.get("timecode_end_entry", None)
            else None
        )
        if start_time:
            shared_data.get("timecode_start_entry").delete(0, "end")
        if end_time:
            shared_data.get("timecode_end_entry").delete(0, "end")


class PreViewRegionsButton(customtkinter.CTkButton):
    def __init__(self, master):
        super().__init__(
            master,
            text="Preview",
            command=self.button_callback,
            corner_radius=12,
            state="disabled",
        )

    def button_callback(self):
        preview_textbox = shared_data.get("preview_textbox", None)
        try:
            # Reset textbox
            preview_textbox.delete("1.0", "end")
            # Reset txt data
            shared_data.update({"txt_data": None})

            file_path = shared_data.get("txt_file_path", None)
            start_time = (
                shared_data.get("timecode_start_entry").get()
                if shared_data.get("timecode_start_entry", None)
                else None
            )
            end_time = (
                shared_data.get("timecode_end_entry").get()
                if shared_data.get("timecode_end_entry", None)
                else None
            )
            if (
                file_path is not None
                and start_time is not None
                and end_time is not None
            ):
                filtered_time_codes = read_and_filter_time_codes(
                    file_path, start_time, end_time
                )
                # Check if had data
                if len(filtered_time_codes) != 0:
                    for line in filtered_time_codes:
                        preview_textbox.insert("end", line + "\n")
                    shared_data.update({"txt_data": filtered_time_codes})
                    # enable export data button
                    shared_data.get("export_btn").configure(state="normal")
                else:
                    preview_textbox.insert(
                        "end",
                        "No data",
                    )
                    shared_data.get("export_btn").configure(state="disabled")
            else:
                preview_textbox.insert(
                    "end",
                    "Please select a valid txt file and provide all necessary information and try again",
                )
        except Exception as e:
            preview_textbox.insert(
                "end",
                f"Error: {str(e)}\n",
            )


class ExportButton(customtkinter.CTkButton):
    def __init__(self, master):
        super().__init__(
            master,
            text="Export",
            command=self.button_callback,
            corner_radius=12,
            state="disabled",
        )

    def button_callback(self):
        txt_data = shared_data.get("txt_data", None)
        preview_textbox = shared_data.get("preview_textbox", None)
        file_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt")],
            initialfile="my_file.txt",
        )
        # Check if the user has selected a file path and txt_data is not None
        if file_path and txt_data is not None:
            # Reset textbox
            preview_textbox.delete("1.0", "end")
            try:
                with open(file_path, "w") as file:
                    file.write("\n".join(txt_data))

                preview_textbox.insert(
                    "end",
                    f"Saved to {file_path}",
                )
            except Exception as e:
                preview_textbox.insert(
                    "end",
                    f"Error: {str(e)}\n",
                )
        # Disable export button
        shared_data.get("export_btn").configure(state="disabled")


# Main app begin
class SideBarFrame(customtkinter.CTkFrame):
    def __init__(self, master, shared_data: dict):
        super().__init__(master)

        # These data are shared throughout the application
        self.shared_data = shared_data

        # Button select an outputted txt file,
        # define regions on the txt file based on the time code,
        # and save these regions.
        self.select_regions_btn = SelectRegionAndExportButton(master=self)
        self.select_regions_btn.grid(row=0, column=0, padx=10, pady=10, sticky="w")

        # Button select file
        self.open_file_btn = OpenFileButton(master=self)
        shared_data.update({"open_file_btn": self.open_file_btn})
        self.open_file_btn.grid(row=1, column=0, padx=10, pady=10, sticky="w")

        # Button select output directory
        self.select_output_dir_btn = SelectOutputDirButton(master=self)
        shared_data.update({"select_output_dir_btn": self.select_output_dir_btn})
        self.select_output_dir_btn.grid(row=2, column=0, padx=10, pady=10, sticky="w")

        # Button run convert function
        self.convert_btn = ConvertButton(master=self)
        shared_data.update({"convert_btn": self.convert_btn})
        self.convert_btn.grid(row=3, column=0, padx=10, pady=10, sticky="w")


class MainContentFrame(customtkinter.CTkFrame):
    def __init__(self, master, shared_data: dict):
        super().__init__(master=master)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1)
        # These data are shared throughout the application
        self.shared_data = shared_data

        # File label
        self.select_regions_label = customtkinter.CTkLabel(
            self,
            text="Extract a section of the TXT file",
            fg_color="transparent",
        )
        self.select_regions_label.grid(row=0, column=0, padx=20, pady=10, sticky="w")

        # File label
        self.file_label = customtkinter.CTkLabel(
            self,
            text="Select the path to the KDF file to convert",
            fg_color="transparent",
        )
        shared_data.update({"file_label": self.file_label})
        self.file_label.grid(row=1, column=0, padx=20, pady=10, sticky="w")

        # Output directory label
        self.output_dir_label = customtkinter.CTkLabel(
            self,
            text="Select the folder path to export your file",
            fg_color="transparent",
        )
        shared_data.update({"output_dir_label": self.output_dir_label})
        self.output_dir_label.grid(row=2, column=0, padx=20, pady=10, sticky="w")

        # Log textbox
        self.log_textbox = customtkinter.CTkTextbox(self)
        shared_data.update({"log_textbox": self.log_textbox})
        self.log_textbox.grid(row=3, column=0, padx=20, pady=(10, 20), sticky="nsew")


class OpenFileButton(customtkinter.CTkButton):
    def __init__(self, master):
        super().__init__(
            master, text="Choose file", command=self.button_callback, corner_radius=12
        )

    def button_callback(self):
        file_path = filedialog.askopenfilename(filetypes=[("KDF files", "*.kdf")])
        # Focus to main window after choose file
        self.master.master.focus_force()

        self.master.shared_data.get("file_label").configure(text=file_path)
        self.master.shared_data.update({"file_path": file_path if file_path else None})
        if file_path == "":
            self.master.shared_data.get("file_label").configure(
                text="Select the path to the KDF file to convert"
            )
        # Check if selected file and output directory
        if (
            self.master.shared_data.get("output_dir_path", None) is not None
            and file_path != ""
        ):
            self.master.shared_data.get("convert_btn").configure(state="normal")
        else:
            self.master.shared_data.get("convert_btn").configure(state="disabled")


class SelectOutputDirButton(customtkinter.CTkButton):
    def __init__(self, master):
        super().__init__(
            master, text="Select output", command=self.button_callback, corner_radius=12
        )

    def button_callback(self):
        output_dir_path = filedialog.askdirectory()
        # Focus to main window after choose directory
        self.master.master.focus_force()

        self.master.shared_data.get("output_dir_label").configure(text=output_dir_path)
        self.master.shared_data.update(
            {"output_dir_path": output_dir_path if output_dir_path else None}
        )
        if output_dir_path == "":
            self.master.shared_data.get("output_dir_label").configure(
                text="Select the folder path to export your file"
            )

        # Check if selected file and output directory
        if (
            self.master.shared_data.get("file_path", None) is not None
            and output_dir_path != ""
        ):
            self.master.shared_data.get("convert_btn").configure(state="normal")
        else:
            self.master.shared_data.get("convert_btn").configure(state="disabled")


class ConvertButton(customtkinter.CTkButton):
    def __init__(self, master):
        super().__init__(
            master,
            text="Convert",
            command=self.button_callback,
            corner_radius=12,
            state="disabled",
        )

    def button_callback(self):

        # Rest log textbox
        self.master.shared_data.get("log_textbox").delete("1.0", "end")

        file_path = self.master.shared_data.get("file_path", None)
        output_dir_path = self.master.shared_data.get("output_dir_path", None)
        if output_dir_path and file_path:
            try:
                # Disable select button when process_kdf_file is running
                self.master.shared_data.get("open_file_btn").configure(state="disabled")
                self.master.shared_data.get("select_output_dir_btn").configure(
                    state="disabled"
                )
                on_event = lambda event: self.master.shared_data.get(
                    "log_textbox"
                ).insert("end", event["message"] + "\n")

                def on_succes():
                    # Reset file patch after process_kdf_file is finished
                    self.master.shared_data.update({"file_path": None})
                    self.master.shared_data.get("file_label").configure(
                        text="Select the path to the KDF file to convert"
                    )

                    # Enable select button when process_kdf_file is finished
                    self.master.shared_data.get("open_file_btn").configure(
                        state="normal"
                    )
                    self.master.shared_data.get("select_output_dir_btn").configure(
                        state="normal"
                    )
                    # Disable convert btn when process_kdf_file is finished
                    self.master.shared_data.get("convert_btn").configure(
                        state="disabled"
                    )

                extractor = Thread(
                    target=KDFExtractor(
                        KDF_file_path=file_path,
                        path_save_data=output_dir_path,
                        num_worker=4,
                    ).get_channel_data,
                    args=(on_event, on_succes),
                )
                # extractor.setDaemon(True)
                extractor.start()
                # kdf_extractor.get_channel_data(
                #     on_event=lambda event: self.master.shared_data.get(
                #         "log_textbox"
                #     ).insert("end", event["message"] + "\n"),
                # )

            except Exception as e:
                print(e)
                self.master.shared_data.get("log_textbox").insert(
                    "end", "\nError: " + str(e) + "\n"
                )
        else:
            self.master.shared_data.get("log_textbox").insert(
                "end", "Please provide the full KDF file path and file export path\n"
            )


class SelectRegionAndExportButton(customtkinter.CTkButton):
    def __init__(self, master):
        super().__init__(
            master,
            text="Extract Sections",
            command=self.button_callback,
            corner_radius=12,
            # state="disabled",
        )

    def button_callback(self):
        if (
            self.master.shared_data.get("select_regions_tlv", None) is None
            or not self.master.shared_data.get(
                "select_regions_tlv", None
            ).winfo_exists()
        ):
            self.master.shared_data.update(
                {"select_regions_tlv": SelectRegionsTLV(self.master)}
            )  # create window if its None or destroyed
        else:
            self.master.shared_data.get("select_regions_tlv").focus()


class App(customtkinter.CTk):
    def __init__(self):
        super().__init__()
        self.geometry("800x500")
        self.title("KDF Extractor")
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        # self.grid_rowconfigure(1, weight=1)

        # These data are shared throughout the application
        self.shared_data = {}

        self.checkbox_frame = SideBarFrame(self, shared_data=self.shared_data)
        self.checkbox_frame.grid(row=0, column=0, padx=10, pady=10, sticky="ns")

        self.main_content_frame = MainContentFrame(self, shared_data=self.shared_data)
        self.main_content_frame.grid(row=0, column=1, padx=10, pady=10, sticky="nsew")


# app = App()
# app.mainloop()
