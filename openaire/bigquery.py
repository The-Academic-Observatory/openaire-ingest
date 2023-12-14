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

import logging
from typing import Union, List
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest, Conflict, NotFound
from google.cloud.bigquery import LoadJob, LoadJobConfig, SourceFormat


def assert_table_id(table_id: str):
    """Assert that a BigQuery table_id contains three parts.

    :param table_id: the BigQuery fully qualified table identifier.
    :return: None.
    """

    n_parts = len(table_id.split("."))
    assert n_parts == 3, f"bq_table_id_parts: table_id={table_id} requires 3 parts but only has {n_parts}"


def bq_table_exists(table_id: str) -> bool:
    """Checks whether a BigQuery table exists or not.

    :param table_id: the fully qualified BigQuery table identifier
    :return: whether the table exists or not.
    """

    assert_table_id(table_id)
    client = bigquery.Client()
    table_exists = True

    try:
        client.get_table(table_id)
    except NotFound:
        table_exists = False

    return table_exists


def bq_create_dataset(project_id: str, dataset_id: str, location: str, description: str = "") -> bigquery.Dataset:
    """Create a BigQuery dataset.

    :param project_id: the Google Cloud project id.
    :param dataset_id: the BigQuery dataset id.
    :param location: the location where the dataset will be stored:
    https://cloud.google.com/compute/docs/regions-zones/#locations
    :param description: a description for the dataset
    :return: None
    """

    func_name = bq_create_dataset.__name__

    # Make the dataset reference
    dataset_ref = f"{project_id}.{dataset_id}"

    # Make dataset handle
    client = bigquery.Client()
    ds = bigquery.Dataset(dataset_ref)

    # Set properties
    ds.location = location
    ds.description = description

    # Create dataset, if already exists then catch exception
    try:
        print(f"{func_name}: creating dataset dataset_ref={dataset_ref}")
        ds = client.create_dataset(ds)
    except Conflict as e:
        logging.warning(f"{func_name}: dataset already exists dataset_ref={dataset_ref}, exception={e}")
    return ds


def bq_load_table(
    *,
    uri: Union[str, List[str]],
    table_id: str,
    schema_file_path: str,
    source_format: str,
    csv_field_delimiter: str = ",",
    csv_quote_character: str = '"',
    csv_allow_quoted_newlines: bool = False,
    csv_skip_leading_rows: int = 0,
    partition: bool = False,
    partition_field: Union[None, str] = None,
    partition_type: bigquery.TimePartitioningType = bigquery.TimePartitioningType.DAY,
    require_partition_filter=False,
    write_disposition: str = bigquery.WriteDisposition.WRITE_EMPTY,
    table_description: str = "",
    cluster: bool = False,
    clustering_fields=None,
    ignore_unknown_values: bool = False,
) -> bool:
    """Load a BigQuery table from an object on Google Cloud Storage.

    :param uri: the uri(s) of the object to load from Google Cloud Storage into BigQuery.
    :param table_id: the fully qualified BigQuery table identifier.
    :param schema_file_path: path on local file system to BigQuery table schema.
    :param source_format: the format of the data to load into BigQuery.
    :param csv_field_delimiter: the field delimiter character for data in CSV format.
    :param csv_quote_character: the quote character for data in CSV format.
    :param csv_allow_quoted_newlines: whether to allow quoted newlines for data in CSV format.
    :param csv_skip_leading_rows: the number of leading rows to skip for data in CSV format.
    :param partition: whether to partition the table.
    :param partition_field: the name of the partition field.
    :param partition_type: the type of partitioning.
    :param require_partition_filter: whether the partition filter is required or not when querying the table.
    :param write_disposition: whether to append, overwrite or throw an error when data already exists in the table.
    :param table_description: the description of the table.
    :param cluster: whether to cluster the table or not.
    :param clustering_fields: what fields to cluster on.
    Default is to overwrite.
    :param ignore_unknown_values: whether to ignore unknown values or not.
    :return: True if the load job was successful, False otherwise.
    """

    func_name = bq_load_table.__name__

    if isinstance(uri, str):
        uri = [uri]

    for u in uri:
        msg = f"uri={u}, table_id={table_id}, schema_file_path={schema_file_path}, source_format={source_format}"
        print(f"{func_name}: load bigquery table {msg}")
        assert u.startswith("gs://"), "load_big_query_table: 'uri' must begin with 'gs://'"

    assert_table_id(table_id)

    # Handle mutable default arguments
    if clustering_fields is None:
        clustering_fields = []

    # Create load job
    client = bigquery.Client()
    job_config = LoadJobConfig()

    # Set global options
    job_config.source_format = source_format
    job_config.schema = client.schema_from_json(schema_file_path)
    job_config.write_disposition = write_disposition
    job_config.destination_table_description = table_description
    job_config.ignore_unknown_values = ignore_unknown_values

    # Set CSV options
    if source_format == SourceFormat.CSV:
        job_config.field_delimiter = csv_field_delimiter
        job_config.quote_character = csv_quote_character
        job_config.allow_quoted_newlines = csv_allow_quoted_newlines
        job_config.skip_leading_rows = csv_skip_leading_rows

    # Set partitioning settings
    if partition:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=partition_type, field=partition_field, require_partition_filter=require_partition_filter
        )
    # Set clustering settings
    if cluster:
        job_config.clustering_fields = clustering_fields

    load_job = None
    try:
        load_job: [LoadJob, None] = client.load_table_from_uri(uri, table_id, job_config=job_config)

        result = load_job.result()
        state = result.state == "DONE"

        print(f"{func_name}: load bigquery table result.state={result.state}, {msg}")
    except BadRequest as e:
        logging.error(f"{func_name}: load bigquery table failed: {e}.")
        if load_job:
            logging.error(f"Error collection:\n{load_job.errors}")
        state = False

    return state
