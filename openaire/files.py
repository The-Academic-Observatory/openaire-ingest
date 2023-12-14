# Copyright 2023 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: James Diprose, Aniek Roelofs, Alex Massen-Hane

import io
import os
import gzip
import codecs
import tarfile
import pathlib
import jsonlines
from typing import List, Dict, Optional, Any
from google_crc32c import Checksum as Crc32cChecksum


def decompress_tar_gz(file_path: str, extract_path: Optional[str] = "."):
    """
    Decompress a .tar file.

    :param file_path: Path to the .tar.gz file to decompress.
    :param extract_path: Directory where the contents will be extracted (default is current directory).
    """

    with tarfile.open(file_path, "r") as tar:
        tar.extractall(extract_path)


def schema_folder() -> str:
    """Return the path to the database schema template folder.

    :return: the path.
    """

    return module_file_path(os.path.join("database", "schemas"))


def module_file_path(module_path: str, nav_back_steps: int = -1) -> str:
    """Get the file path of a module, given the Python import path to the module.

    :param module_path: the Python import path to the module
    :param nav_back_steps: the number of steps on the path to step back.
    :return: the file path to the module.
    """

    file_path = pathlib.Path(module_path).resolve()
    print("file_path: ", file_path)
    return os.path.normpath(str(pathlib.Path(*file_path.parts[:nav_back_steps]).resolve()))


def load_jsonl_gz(file_path: str) -> List[Dict]:
    """Reads and loads data from a gzipped JSONL file.
    :param file_path: Path to the .jsonl.gz file
    :return: A list of dictionaries loaded from the file.
    """
    data = []

    with open(file_path, "rb") as jsonl_gzip_file:
        with gzip.GzipFile(fileobj=jsonl_gzip_file, mode="rb") as gzip_file:
            with jsonlines.Reader(gzip_file) as reader:
                for line in reader.iter():
                    data.append(line)

    return data


def save_jsonl_gz(file_path: str, data: List[Dict]) -> None:
    """Takes a list of dictionaries and writes this to a gzipped jsonl file.
    :param file_path: Path to the .jsonl.gz file
    :param data: a list of dictionaries that can be written out with jsonlines
    :return: None.
    """

    with io.BytesIO() as bytes_io:
        with gzip.GzipFile(fileobj=bytes_io, mode="w") as gzip_file:
            with jsonlines.Writer(gzip_file) as writer:
                writer.write_all(data)

        with open(file_path, "wb") as jsonl_gzip_file:
            jsonl_gzip_file.write(bytes_io.getvalue())


def crc32c_base64_hash(file_path: str, chunk_size: int = 8 * 1024) -> str:
    """Create a base64 crc32c checksum of a file.

    :param file_path: the path to the file.
    :param chunk_size: the size of each chunk to check.
    :return: the checksum.
    """

    hash_alg = Crc32cChecksum()

    with open(file_path, "rb") as f:
        chunk = f.read(chunk_size)
        while chunk:
            hash_alg.update(chunk)
            chunk = f.read(chunk_size)
    return hex_to_base64_str(hash_alg.hexdigest())


def hex_to_base64_str(hex_str: bytes) -> str:
    """Covert a hexadecimal string into a base64 encoded string. Removes trailing newline character.

    :param hex_str: the hexadecimal encoded string.
    :return: the base64 encoded string.
    """

    string = codecs.decode(hex_str, "hex")
    base64 = codecs.encode(string, "base64")
    return base64.decode("utf8").rstrip("\n")


def get_chunks(*, input_list: List[Any], chunk_size: int = 8) -> List[Any]:
    """Generator that splits a list into chunks of a fixed size.

    :param input_list: Input list.
    :param chunk_size: Size of chunks.
    :return: The next chunk from the input list.
    """

    n = len(input_list)
    for i in range(0, n, chunk_size):
        yield input_list[i : i + chunk_size]
