#!/usr/bin/python3
#
# Interface for the assignement
#

import psycopg2

RANGE_RATINGS_METADATA_TABLE ='rangeratingsmetadata'
RROBIN_RATINGS_METADATA_TABLE='roundrobinratingsmetadata'

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()

    # Create the ratings table if it doesn't exist, ignoring the timestamp column in the schema
    
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {ratingstablename} (
        userid INT,
        movieid INT,
        rating FLOAT
    )
    """)
    openconnection.commit()

    # Load data from the file into the ratings table, skipping the timestamp column
    with open(ratingsfilepath, 'r') as f:
        for line in f:
            # Split each line by '::' and extract the first three fields
            userid, movieid, rating, _ = line.strip().split('::')
            cursor.execute(f"""
                INSERT INTO {ratingstablename} (userid, movieid, rating)
                VALUES (%s, %s, %s)
            """, (int(userid), int(movieid), float(rating)))

    openconnection.commit()
    cursor.close()



def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {RANGE_RATINGS_METADATA_TABLE} (
        partitionnum int
    )
    """)
    openconnection.commit()

    cursor.execute(f"""
                INSERT INTO {RANGE_RATINGS_METADATA_TABLE} (partitionnum)
                VALUES (%s)
            """, [int(numberofpartitions)])

    # Determine the range interval for each partition
    range_interval = 5.0 / numberofpartitions  # Ratings range from 0 to 5

    # Create partitions based on the calculated range intervals
    for i in range(numberofpartitions):
        # Define the range for the current partition
        lower_bound = i * range_interval
        upper_bound = (i + 1) * range_interval

        # Partition table name for each range partition
        partition_table_name = f"range_part{i}"
        print(f"Creating partion {partition_table_name} Lower Bound: {lower_bound} Upper Bound: {upper_bound}")
        # Drop partition table if it exists and then create it
        cursor.execute(f"DROP TABLE IF EXISTS {partition_table_name}")
        cursor.execute(f"""
            CREATE TABLE {partition_table_name} AS
            SELECT *
            FROM {ratingstablename}
            WHERE rating > {lower_bound} AND rating <= {upper_bound}
        """)

    # Special case: include ratings of 5 in the last partition
    cursor.execute(f"""
        INSERT INTO range_part0
        SELECT *
        FROM {ratingstablename}
        WHERE rating = 0.0
    """)

    openconnection.commit()
    cursor.close()



def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()

    # Create partition tables
    for i in range(numberofpartitions):
        partition_table_name = f"rrobin_part{i}"
        cursor.execute(f"DROP TABLE IF EXISTS {partition_table_name}")
        cursor.execute(f"""
            CREATE TABLE {partition_table_name} (
                userid INT,
                movieid INT,
                rating FLOAT
            )
        """)
    
    # Retrieve all rows from the ratings table
    cursor.execute(f"SELECT * FROM {ratingstablename}")
    rows = cursor.fetchall()

    # Distribute rows across partitions in a round-robin manner
    for index, row in enumerate(rows):
        # Calculate the partition index for each row
        partition_index = index % numberofpartitions
        partition_table_name = f"rrobin_part{partition_index}"
        
        # Insert the row into the appropriate partition table
        cursor.execute(f"""
            INSERT INTO {partition_table_name} (userid, movieid, rating)
            VALUES (%s, %s, %s)
        """, row)

    openconnection.commit()
    cursor.close()



def roundrobininsert(ratingstablename, userid, movieid, rating, openconnection):
    cursor = openconnection.cursor()

    # Insert the new row into the main ratings table
    cursor.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, movieid, rating))

    # Get the number of existing partitions
    cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE %s", 
                   (f"rrobin_part%",))
    numberofpartitions = cursor.fetchone()[0]

    # Get the current count of records in the ratings table to determine the next partition index
    cursor.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cursor.fetchone()[0]

    # Determine the partition index for the new row
    partition_index = (total_rows - 1) % numberofpartitions  # Subtract 1 because total_rows includes the new row

    # Insert the new row into the appropriate round-robin partition
    partition_table_name = f"rrobin_part{partition_index}"
    cursor.execute(f"""
        INSERT INTO {partition_table_name} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, movieid, rating))

    openconnection.commit()
    cursor.close()



def rangeinsert(ratingstablename, userid, movieid, rating, openconnection):
    cursor = openconnection.cursor()

    # Insert the new row into the main ratings table
    cursor.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, movieid, rating))

    # Get the number of partitions
    cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE %s",
                   (f"range_part%",))
    numberofpartitions = cursor.fetchone()[0]

    # Calculate the range interval for each partition
    range_interval = 5.0 / numberofpartitions  # Ratings range from 0 to 5

    # Determine the partition index for the new rating
    partition_index = int(rating // range_interval)
    # Ensure ratings of exactly 5 go to the last partition
    if rating % partition_index == 0 and partition_index != 0:
        partition_index = partition_index - 1

    # Insert the new row into the appropriate range partition
    partition_table_name = f"range_part{partition_index}"
    cursor.execute(f"""
        INSERT INTO {partition_table_name} (userid, movieid, rating)
        VALUES (%s, %s, %s)
    """, (userid, movieid, rating))
    print(f"Adding ({userid}, {movieid}, {rating}) to partion {partition_table_name}")

    openconnection.commit()
    cursor.close()


def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()