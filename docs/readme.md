The main.py (heart of the application) is created from scratch, a lot of modifications are done in order to tighten the loose ends within the project.
I have used Ubuntu os and pyCharm ide to write the code for this spark app. 

The base code is taken from Manish's repo.
This endeavor aims to provide you with insights into the functioning of projects within a real-time environment.

The code has been meticulously crafted with careful consideration for various aspects. It not only nurtures your coding skills but also imparts a comprehensive comprehension of project structures.

```plaintext
Project structure:-
my_project
├── docs
│   ├── architecture.png
│   ├── database_schema.drawio.png
│   ├── project_structure.txt
│   └── readme.md
├── .env
├── .gitignore
├── __init__.py
├── logs
│   └── application.log
├── resources
│   ├── dev
│   │   ├── config.py
│   │   ├── __init__.py
│   │   └── requirements.txt
│   ├── __init__.py
│   ├── prod
│   │   ├── config.py
│   │   └── requirements.txt
│   ├── qa
│   │   ├── config.py
│   │   └── requirements.txt
│   └── sql_scripts
│       └── table_scripts.sql
└── src
    ├── __init__.py
    ├── main
    │   ├── delete
    │   │   ├── aws_delete.py
    │   │   ├── database_delete.py
    │   │   └── local_file_delete.py
    │   ├── download
    │   │   └── aws_file_download.py
    │   ├── __init__.py
    │   ├── move
    │   │   └── move_files.py
    │   ├── read
    │   │   ├── aws_read.py
    │   │   └── database_read.py
    │   ├── transformations
    │   │   ├── __init__.py
    │   │   └── jobs
    │   │       ├── customer_mart_sql_tranform_write.py
    │   │       ├── dimension_tables_join.py
    │   │       ├── __init__.py
    │   │       ├── main.py
    │   │       └── sales_mart_sql_transform_write.py
    │   ├── upload
    │   │   └── upload_to_s3.py
    │   ├── utility
    │   │   ├── encrypt_decrypt.py
    │   │   ├── logging_config.py
    │   │   ├── my_sql_session.py
    │   │   ├── s3_client_object.py
    │   │   └── spark_session.py
    │   └── write
    │       ├── database_write.py
    │       └── parquet_writer.py
    └── test
        ├── C:\Users\nikita\Documents\data_engineering\spark_data
        │   └── sales_data_2023-11-01.csv
        ├── extra_column_csv_generated_data.py
        ├── generate_csv_data.py
        ├── generate_customer_table_data.py
        ├── generate_datewise_sales_data.py
        ├── __init__.py
        ├── less_column_csv_generated_data.py
        ├── sales_data_upload_s3.py
        └── scratch_pad.py

20 directories, 49 files

19 directories, 45 files
```

Project Architecture:
![Project Architecture](architecture.png)

MYSQL Database Schema (ER Diagram):
![MYSQL Database Schema](database_schema.drawio.png)

How to run the program in Pycharm:-
1. Open the pycharm editor.
2. Upload or pull the project from GitHub.
3. Open terminal from bottom pane.
4. Goto virtual environment and activate it. Let's say you have venv as virtual environment.i) cd venv ii) cd Scripts iii) activate (if activate doesn't work then use ./activate)
5. Create .env file and add key, iv and salt. Ensure iv is exactly 16 byte long. Open pyCHarm terminal and run pip install -r resources/dev/requirements.txt command to load the required packages.
6. You will have to create a user on AWS also and assign s3 full access and provide secret key and access key to the config file.
7. Run main.py from green play button on top right hand side or in terminal run spark-submit master local[*] main.py
8. If everything works as expected enjoy, else re-try and watch https://youtu.be/FWiwI5DheO0?si=kYDKzlEhACkXOvmo
9. If you want to run the main.py using spark submit command, you need let spark know to run it from venv python interpreter and also provide all the necessary jar files. Below is the code, I am using to submit the spark command.
spark-submit --master local[4] --conf spark.pyspark.python=/home/hdoop/PycharmProjects/de_project1/venv/bin/python --jars /usr/share/java/mysql-connector-java-8.3.0.jar src/main/transformations/jobs/main.py
