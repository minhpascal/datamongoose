"""
    IQFeed plugin and some misc things

    Slight modification to
    https://www.quantstart.com/articles/Downloading-Historical-Intraday-US-Equities-From-DTN-IQFeed-with-Python
    by Michael Halls-Moore

    Still uses Michael's COM socket calls but differences are:
        - Instead of f.write to csv then re-read with Pandas, I'm using
          data from COM socket -> python list -> numpy array then reshape -> pandas dataframe
        - Saves the pandas dataframe into a pickled file.

    Considered using HDF5 but issues of file corruption (although journaling support is coming?) worries me.
"""
import datetime as dt
import os
import socket

import numpy as np
import pandas as pd


def read_historical_data_socket(
        sock,
        recv_buffer=4096
):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    buffer = ""
    data = ""
    while True:
        data = sock.recv(recv_buffer)
        buffer += data

        # Check if the end message string arrives
        if "!ENDMSG!" in buffer:
            break

    # Remove the end message string
    buffer = buffer[:-12]
    return buffer


def clean_db(
        symbol,
        tf="60",
        start_time="20000101 000000",
        cache="C:/mongoose"
):
    # Define port
    # Level 1 port # 5009, Level 2 port # 9200, historical port # 9100
    host = "127.0.0.1"
    port = 9100  # Historical data socket port

    # Download each symbol to disk
    print "Downloading symbol: %s..." % symbol  # Construct the message needed by IQFeed to retrieve data

    # Create message based on time frame.
    if tf == "tick":
        message = "HTT,%s,%s,,,,,1\n" % (symbol, start_time)
    elif tf == "86400":
        message = "HDX,%s,%s,\n" % (symbol, 0)
    else:
        message = "HIT,%s,%s,%s,,,,,1\n" % (symbol, tf, start_time)

    # Open a streaming socket to the IQFeed server locally
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    # Send the historical data request
    # message and buffer the data
    sock.sendall(message)
    data = read_historical_data_socket(sock)
    sock.close

    # Clean up the data.
    data = "".join(data.split("\r"))
    data = data.replace(",\n", ",")[:-1]
    data = data.split(",")

    # Column names for the incoming data.
    if tf == "tick":
        col_names = ['Last', 'Size', 'Volume', 'Bid', 'Ask', 'TickID', 'Bid Size', 'Ask Size', 'Drop']
    else:
        col_names = ['High', 'Low', 'Open', 'Close', 'Volume', 'OI']

    # Build pandas dataframe by converting the list first to numpy.
    data = np.array(data).reshape(-1, len(col_names) + 1)
    r = pd.DataFrame(data=data[:, 1:], index=data[:, 0], columns=col_names)
    r.index = pd.to_datetime(r.index)

    # Remove timestampe information for daily data.
    if tf == "86400":
        r.index = r.index.normalize()

    # Sort the dataframe based on ascending dates.
    r = r.sort_index(ascending=True)

    # Convert dataframe columns to float and ints.
    if tf == "tick":
        r[['Last', 'Bid', 'Ask']] = r[['Last', 'Bid', 'Ask']].astype(float)
        r[['Size', 'Volume', 'TickID', 'Bid Size', 'Ask Size']].astype(int)
    else:
        r[['High', 'Low', 'Open', 'Close']] = r[['High', 'Low', 'Open', 'Close']].astype(float)
        r[['Volume', 'OI']] = r[['Volume', 'OI']].astype(int)

    # Pickle the file.
    r.to_pickle("%s/%s_%s.pkl" % (cache, symbol, tf))

    print "Done creating a clean db for %s at %s seconds" % (symbol, tf)

    return r


def update_db(
        symbol,
        tf="60",
        cache="C:/mongoose"
):
    if not (os.path.isfile("%s/%s_%s.pkl" % (cache, symbol, tf))):
        print "Database file not found... creating a new price db."
        clean_db(symbol=symbol, tf=tf, cache=cache)

    print "Querying iqFeed servers for updates in %s" % symbol

    # Define port
    # Level 1 port # 5009, Level 2 port # 9200, historical port # 9100
    host = "127.0.0.1"
    port = 9100  # Historical data socket port

    # Define the datetime to retrieve.
    start_time = (dt.datetime.now() - dt.timedelta(days=4)).strftime('%Y%m%d %H%M%S')

    if tf == "tick":
        start_time = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y%m%d %H%M%S')

    # Download each symbol to disk
    print "Downloading updates for symbol: %s..." % symbol  # Construct the message needed by IQFeed to retrieve data

    # Create message based on time frame.
    if tf == "tick":
        message = "HTT,%s,%s,,,,,1\n" % (symbol, start_time)
    elif tf == "86400":
        message = "HDX,%s,%s,\n" % (symbol, 0)
    else:
        message = "HIT,%s,%s,%s,,,,,1\n" % (symbol, tf, start_time)

    # Open a streaming socket to the IQFeed server locally
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    # Send the historical data request
    # message and buffer the data
    sock.sendall(message)
    data = read_historical_data_socket(sock)
    sock.close

    # Clean up the data.
    data = "".join(data.split("\r"))
    data = data.replace(",\n", ",")[:-1]
    data = data.split(",")

    # Column names for the incoming data.
    if tf == "tick":
        col_names = ['Last', 'Size', 'Volume', 'Bid', 'Ask', 'TickID', 'Bid Size', 'Ask Size', 'Drop']
    else:
        col_names = ['High', 'Low', 'Open', 'Close', 'Volume', 'OI']

    # Build pandas dataframe by converting the list first to numpy.
    data = np.array(data).reshape(-1, len(col_names) + 1)
    r = pd.DataFrame(data=data[:, 1:], index=data[:, 0], columns=col_names)
    r.index = pd.to_datetime(r.index)

    # Remove timestampe information for daily data.
    if tf == "86400":
        r.index = r.index.normalize()

    # Sort the dataframe based on ascending dates.
    r = r.sort_index(ascending=True)

    # Convert dataframe columns to float and ints.
    if tf == "tick":
        r[['Last', 'Bid', 'Ask']] = r[['Last', 'Bid', 'Ask']].astype(float)
        r[['Size', 'Volume', 'TickID', 'Bid Size', 'Ask Size']].astype(int)
    else:
        r[['High', 'Low', 'Open', 'Close']] = r[['High', 'Low', 'Open', 'Close']].astype(float)
        r[['Volume', 'OI']] = r[['Volume', 'OI']].astype(int)

    hist_data = pd.io.pickle.read_pickle("%s/%s_%s.pkl" % (cache, symbol, tf))
    new_data = pd.concat([hist_data, r]).drop_duplicates(keep='last')
    new_data = new_data.groupby(new_data.index).last()

    # Print what we have done so far.
    print "Updating the following information for %s into old data base" % symbol
    print "================= NEW DATA ================="
    print r.tail()

    print "================= OLD DATA ================="
    print hist_data.tail()

    print "================= UPD DATA ================="
    print new_data.tail()

    new_data.to_pickle("%s/%s_%s.pkl" % (cache, symbol, tf))

    return new_data


def load_db(
        symbol,
        tf="60",
        update="y",
        remove_last="y",
        cache="C:/mongoose"
):
    if update == 'y':
        # Update database from IQFeed and get values.
        load_px = update_db(symbol=symbol,
                            tf=tf,
                            cache=cache)  # Call IQFeed package and update accordingly.
    else:
        load_px = pd.io.pickle.read_pickle("%s/%s_%s.pkl" % (cache, symbol, tf))

    if remove_last == "y":
        end_index = load_px.index.searchsorted(dt.datetime.now())
        load_px = load_px.iloc[:end_index, :]

    return load_px


def bond_str_conversion(x):
    back = x % 1
    front = int(x // 1)

    back = back.as_integer_ratio()[0] * (128 / back.as_integer_ratio()[1])

    back = float(back) / (128 / 32)

    return '%s - %s' % (front, str(back))
