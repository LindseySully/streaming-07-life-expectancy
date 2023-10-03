"""
This producer program prompts the user if they'd like to open the RabbitMQ
Admin website to monitor the queues. It will also take the input CSV file; output an 
intermediate file that stores all rows read from the input CSV file. 
It delays the messages by 3 seconds each.
"""

# import libraries
import pika
import sys
import webbrowser
import csv
import os
import time

from util_logger import setup_logger
logger, logname = setup_logger(__file__)


# declare constants
FILE_PATH = "/Users/lindseysullivan/Documents/School/Streaming-Data/Modules/streaming-07-life-expectancy"
INPUT_CSV_FILE = "Life-Expectancy-Data-Updated.csv"
HOST = "localhost"
INTERMEDIATE_CSV_FILE = "intermediate_file.csv"

# setup path to directory for CSV file
os.chdir(FILE_PATH)

# ----------------------------------------------
# Define Program Functions
# ----------------------------------------------
def offer_rabbitmq_admin_site(show_offer):
    """
    Offer to open the RabbitMQ Admin website
    """
    if show_offer == True:
         ans = input("Would you like to monitor RabbitMQ queues? y or n ")
         print()
         if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def send_to_queue(host, queue_name, message):
    """
    Establishes a connection
    Messages are delivered persistently to ensure that if RabbitMQ fails or restarts it will not be lost.
    """
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    channel = conn.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))
    conn.close()


def stream_csv_messages(input_file_name: str, intermediate_file_name: str, host: str):
    """
    Read input CSV file, save to an intermediate file, and send each row as a message to a queue based on the region.

    Parameters:
        input_file_name (str): The name of the original CSV file
        intermediate_file_name (str): The name of the intermediate CSV file
        host (str): host name or IP address of the RabbitMQ server
    """
    try:
        with open(input_file_name, "r", encoding="utf-8") as infile, \
             open(intermediate_file_name, "w", newline='', encoding="utf-8") as outfile:

            reader = csv.reader(infile, delimiter=",")
            writer = csv.writer(outfile)

            headers = next(reader, None)  # skip header row
            # write specific header row columns to intermediate file
            writer.writerow(["Country", "Region", "Year", "Life Expectancy", "GDP Per Capita"])  

            for row in reader:
                # write specific data row columns to intermediate file
                country = row[headers.index('Country')]
                region = row[headers.index('Region')]
                year = row[headers.index('Year')]
                life_expectancy = row[headers.index('Life_expectancy')]
                gdp_per_capita = row[headers.index('GDP_per_capita')]

                writer.writerow([country, region, year, life_expectancy, gdp_per_capita])

                queue_name = f"queue_{region.replace(' ', '_')}"
                message = ",".join([country, region, year, life_expectancy, gdp_per_capita])
                send_to_queue(host, queue_name, message)
                logger.info(f"Sent message for {country} to {queue_name}")

                time.sleep(3) # delay messages by 3 seconds

            logger.info(f"Data written to {intermediate_file_name}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
            
          

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    # true shows the offer/false turns off the offer for the user
    offer_rabbitmq_admin_site(show_offer=True)

    # Stream messages from the CSV file and send them to RabbitMQ
    stream_csv_messages(INPUT_CSV_FILE, INTERMEDIATE_CSV_FILE, HOST)