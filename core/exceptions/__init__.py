from dataclasses import dataclass, field


@dataclass
class HeaderNotFoundError(Exception):
    message: str = field(default="Invalid KDF file or header not found", init=False)

    def __post_init__(self):
        super().__init__(self.message)


@dataclass
class ParserDataError(Exception):
    message: str = field(
        default="Cannot convert data_size or data_offset to numeric type", init=False
    )

    def __post_init__(self):
        super().__init__(self.message)


@dataclass
class FileWriteError(Exception):
    message: str = field(default="An error occurred while writing the file", init=False)

    def __post_init__(self):
        super().__init__(self.message)
