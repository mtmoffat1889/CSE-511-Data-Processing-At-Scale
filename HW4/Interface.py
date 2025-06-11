#!/usr/bin/python3


import psycopg2
import os
import sys


DATABASE_NAME='dds_assignment'
RATINGS_TABLE_NAME='ratings'
RANGE_TABLE_PREFIX='range_part'
RROBIN_TABLE_PREFIX='rrobin_part'
RANGE_QUERY_OUTPUT_FILE='RangeQueryOut.txt'
PONT_QUERY_OUTPUT_FILE='PointQueryOut.txt'
RANGE_RATINGS_METADATA_TABLE ='rangeratingsmetadata'
RROBIN_RATINGS_METADATA_TABLE='roundrobinratingsmetadata'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    # List to store all output rows for writing to the file
    results = []
    print(f"Lower Bound: {ratingMinValue} \nUpper Bound {ratingMaxValue}")
    # Fetch range partitions from the metadata table and query each partition

    range_partitions_query = f"SELECT partitionnum FROM {RANGE_RATINGS_METADATA_TABLE}"
    cursor = openconnection.cursor()
    cursor.execute(range_partitions_query)
    range_partitions = cursor.fetchall()

    for partition in range_partitions:
        partition_name = f"{RANGE_TABLE_PREFIX}{partition[0]}"
        print(partition_name)
        query = f"""
            SELECT '{partition_name}', userid, movieid, rating
            FROM {partition_name}
            WHERE rating >= {ratingMinValue} AND rating <= {ratingMaxValue}
        """
        cursor.execute(query)
        results.extend(cursor.fetchall())
    print(results)

    # Fetch round-robin partitions from the metadata table and query each partition
    rrobin_partitions_query = f"SELECT partitionnum FROM {RROBIN_RATINGS_METADATA_TABLE}"
    cursor.execute(rrobin_partitions_query)
    rrobin_partitions = cursor.fetchall()
    numberofpartitions = rrobin_partitions[0][0]
    #range_interval = 5.0 / numberofpartitions
    for i in range(numberofpartitions):
        partition_name = f"{RROBIN_TABLE_PREFIX}{i}"
        query = f"""
            SELECT '{partition_name}', userid, movieid, rating
            FROM {partition_name}
            WHERE rating >= {ratingMinValue} AND rating <= {ratingMaxValue}
        """
        cursor.execute(query)
        results.extend(cursor.fetchall())

    # Write results to the output file
    writeToFile(RANGE_QUERY_OUTPUT_FILE, results)



def PointQuery(ratingsTableName, ratingValue, openconnection):
    # List to store all output rows for writing to the file
    results = []

    # Fetch range partitions from the metadata table and query each partition
    range_partitions_query = f"SELECT partitionnum FROM {RANGE_RATINGS_METADATA_TABLE}"
    cursor = openconnection.cursor()
    cursor.execute(range_partitions_query)
    range_partitions = cursor.fetchall()

    for partition in range_partitions:
        partition_name = f"{RANGE_TABLE_PREFIX}{partition[0]}"
        print(partition_name)
        query = f"""
            SELECT '{partition_name}', userid, movieid, rating
            FROM {partition_name}
            WHERE rating = {ratingValue}
        """
        cursor.execute(query)
        results.extend(cursor.fetchall())

    # Fetch round-robin partitions from the metadata table and query each partition
    rrobin_partitions_query = f"SELECT partitionnum FROM {RROBIN_RATINGS_METADATA_TABLE}"
    cursor.execute(rrobin_partitions_query)
    rrobin_partitions = cursor.fetchall()
    numberofpartitions = rrobin_partitions[0][0]
    #range_interval = 5.0 / numberofpartitions
    for i in range(numberofpartitions):
        partition_name = f"{RROBIN_TABLE_PREFIX}{i}"
        query = f"""
            SELECT '{partition_name}', userid, movieid, rating
            FROM {partition_name}
            WHERE rating = {ratingValue}
        """
        cursor.execute(query)
        results.extend(cursor.fetchall())

    # Write results to the output file
    writeToFile(PONT_QUERY_OUTPUT_FILE, results)
                


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()