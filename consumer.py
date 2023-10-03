"""
The consumer program is responsible for consuming the messages sent via dynamic queues based off a countries region. 
This is leveraging multi-threading. 
The program will then output the message into a output csv file if it meets the criteria (*Life expectancy & GDP per captia greater than the average*). 
After the program is interrupted by the user the program will send an email to to the specified to email with the CSV files as an attachment. 
"""

# import libraries
import csv
import pika
import os
import sys
import threading
import smtplib # Import smtplib to send an email with the output CSV files
import glob  # Import the glob module to easily list all CSV files in the output directory
import signal # Import signal to have worker to stop their execution and then send the email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv

from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# Load .env file
load_dotenv()

# Retrieve environment variables
FROM_EMAIL = os.getenv('EMAIL')
TO_EMAIL = os.getenv('TOEMAIL')
PASSWORD = os.getenv('PASSWORD')

# Check if email and password are set
if not FROM_EMAIL or not PASSWORD:
    print("Error: EMAIL and PASSWORD must be set in the .env file.")
    sys.exit(1)

# -------------------------------------
# Define Constants
# -------------------------------------

# define email and SMTP details

SMTP_SERVER = "smtp.gmail.com"
PORT = 587

# define constants
HOST = "localhost"
INTERMEDIATE_FILE_PATH = "/Users/lindseysullivan/Documents/School/Streaming-Data/Modules/streaming-07-life-expectancy/intermediate_file.csv"

# define criteria
LIFE_EXPECTANCY_CRITERIA = 72.72
GDP_PER_CAPITA_CRITERIA = 10881

# Creating a directory to store output files
os.makedirs("output", exist_ok=True)

# Flag to signal all threads to stop
shutdown_flag = False

def signal_handler(signal, frame):
    """
    Used for catching the interruption signal and setting the shutdown_flag to True. 
    When the user presses CTRL+C, the signal_handler function is invoked, and it updates the shutdown_flag variable. 
    """
    global shutdown_flag
    print("\nCTRL+C detected. Preparing to shut down.")
    shutdown_flag = True

# Register signal handler to detect CTRL+C
signal.signal(signal.SIGINT, signal_handler)

def send_email(subject, body, attachment_path, to_email, from_email, smtp_server, port, login, password, queue_name):
    """
    
    """
    full_subject = f"{subject} - {queue_name}"
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = full_subject

    msg.attach(MIMEText(body, 'plain'))

    if os.path.isfile(attachment_path):
        with open(attachment_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(attachment_path)}")
            msg.attach(part)

    try:
        with smtplib.SMTP(smtp_server, port) as server:
            server.starttls()
            server.login(login, password)
            text = msg.as_string()
            server.sendmail(from_email, to_email, text)
            logger.info(f"Email sent to {to_email}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


def callback(ch, method, properties, body):
    """
    
    """
    try:
        message = body.decode()
        row = message.split(',')

        country = row[0].strip()
        region = row[1].strip()
        year = row[2].strip()
        life_expectancy = float(row[3].strip())  
        gdp_per_capita = float(row[4].strip())   

        if life_expectancy > LIFE_EXPECTANCY_CRITERIA and gdp_per_capita > GDP_PER_CAPITA_CRITERIA:
            filename = f"output/{region}.csv"

            mode = 'a' if os.path.exists(filename) else 'w'
            with open(filename, mode, newline='', encoding="utf-8") as file:
                writer = csv.writer(file)
                
                if mode == 'w':
                    writer.writerow(["Country", "Region", "Year", "GDP_per_capita", "Life_expectancy"])

                writer.writerow([country, region, year, gdp_per_capita, life_expectancy])
                logger.info(f"Data written for {country} in {region}")
    
    except IndexError:
        logger.error(f"Index error with row: {row}. Ensure the expected CSV structure is correct.")

    except ValueError as e:
        logger.error(f"Value error: {e}. Ensure the data types and values in the row are correct: {row}")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e} with row {row}")


def worker(host, queue_name):
    """
    """
    global shutdown_flag

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        channel = connection.channel()
    except Exception as e:
        print("\nERROR: Connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={host}.")
        print(f"The error says: {e}")
        sys.exit(1)

    try:
        channel.queue_declare(queue=queue_name, durable=True)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=True
        )

        print(f"\nWaiting for messages in {queue_name}. To exit press CTRL+C")

        while not shutdown_flag:
            channel.connection.process_data_events(time_limit=1)
            
    except KeyboardInterrupt:
        shutdown_flag = True
        print("\nUser interrupted continuous listening process.")

    except Exception as e:
        print(f"ERROR: {e}")

    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

        

   
    
# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below

if __name__ == "__main__":
    
    # call the main function with the information needed
    regions = set()

    if os.path.exists(INTERMEDIATE_FILE_PATH):
        with open(INTERMEDIATE_FILE_PATH, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            regions = {row[1] for row in reader}

        threads = []
        for region in regions:
            queue_name = f"queue_{region.replace(' ', '_')}"
            thread = threading.Thread(target=worker, args=(HOST, queue_name))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        if shutdown_flag:
            try:
                files = glob.glob("output/*.csv")

                if files:
                    subject = "Life Expectancy/GDP by Countries - Greater than 2020 Average - Data Export"
                    body = "The consumer has been interrupted. Attached are the CSV files with the data gathered so far."

                    for file in files:
                        send_email(subject, body, file, TO_EMAIL, FROM_EMAIL, SMTP_SERVER, PORT, FROM_EMAIL, PASSWORD, queue_name)
                        print(f"\nEmail sent with attachment: {file}")
                else:
                    print("No CSV files found in the output directory to send.")
            except Exception as e:
                print(f"Error sending email: {e}")
    else:
        logger.error(f"The file {INTERMEDIATE_FILE_PATH} does not exist or is not accessible.")
    
