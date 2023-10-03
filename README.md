# Project Overview
The objective of this project was to leverage the Life Expectacy Data set provided by Kaggle. This dataset contains the life expectancy collected by the World Health Organization. It outlines multiple data points, but my primary focus was on the **life expectancy and GDP per capita.**

From the data points provided by the CSV file; I gathered the criteria on the average life expectancy and GDP per capita in 2020. 
**2020 Data:**
- Life Expectancy: 72.72
- GDP per Capita: 10,881

The goal is that my producer/consumer programs read the CSV file, and create an output directory that contains the countries by region that exceed the average. The program will then send me an email with each region's CSV file to an email for review. 

## Project Prerequisties
1. Git
1. Python 3.7+ (3.11+ Preferred)
1. VS Code Editor
1. VS Code Extension: Python (by Microsoft)
1. RabbitMQ

## Getting Started
1. Fork this starter repo into your GitHub.
1. Clone your repo down to your machine.
1. View / Command Palette - then Python: Select Interpreter
1. Select your conda environment. 

## Project Libraries

### Producer Libraries
- pika
    - Enables communication with RabbitMQ's message broker, allowing the publishing and consumption using the AMQP protocol.
- sys
    - offers access to Python interpreter variables and functions, allowing manipulation of the Python runtime environment, including exiting the program and handling paths.
- webbrowser
    - webbrowser is a convenient module for launching and managing web browsers, enabling Python scripts to open web pages or HTML files in a browser window, aiding in automated web interactions and testing.
- CSV
    - Facilitates the reading and writing of the data between Python programs and CSV files.
- os
    - The os module provides a portable way of using operating system-dependent functionality, such as reading/writing files, launching and killing processes, and managing directories.
- time
    - The time module provides various time-related functions, including retrieving the current time, delaying program execution, and formatting time, essential for handling durations and timestamps.
### Consumer Libraries
- CSV
- pika
- os
- sys
- threading
    - The threading module allows the creation, synchronization, and management of multiple threads in a Python program, enabling concurrent execution for improved efficiency.
    - Used to allow the consumer to dynamically manage multiple queues
- smtplib
    - is used for sending emails using the Simple Mail Transfer Protocol (SMTP), supporting server authentication and secure connections.
        - MIMEMultipart
        - MIMEText
        - MiMEBase
        - encoders
- glob
    - The glob module is used to retrieve files/pathnames matching a specified pattern, simplifying file handling and manipulation.
- signal
    - signal provides mechanisms to handle signals and timers, allowing Python programs to have more control over interactions with the operating system.
- dotenv
    - The dotenv module enables loading environment variables from a .env file into the environment, securing sensitive information.
        - load_dotenv
            - load_dotenv is a function within the dotenv module that loads environment variables from a .env file into the environment for access and management within the application.


# Producer

### Program Functions
1. offer_rabbitmq_admin_site()
    -  offers the user to open the RabbitMQ admin page
1. send_to_queue()
    - establishes a connection
    - messages are delivered persistently to ensure that if RabbitMQ fails or restarts it will not be lost.
1. stream_csv_messages()
    - **Parameters:**
        - input_file_name (str): original CSV file
        - intermediate_file_name (str): intermediate CSV file that stores all sent messages
        - host (str): host name or IP address of the RabbitMQ server
    - This will write a row with the headers of Country, Region, Year, Life Expectancy, and GDP per Capita. The program will also let the user know that the message was sent for the country to the specific queue name, and that the data was written to an intermediate file. 
### Screenshots

**Producer Terminal**
![Alt text](Screenshots/Producer.png)

# Consumer
### Program Functions
