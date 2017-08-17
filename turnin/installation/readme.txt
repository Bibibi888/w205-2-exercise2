In this folder, I've included three files: create_table.sql, create_table.sh, create_database.sql, create_database.sh, and requirements.txt.

The create_tables scripts are for creating the PostgreSQL tables. The script version is designed with trust authentication in mind- just pass in a username when you run the script as the first argument, and the script should handle the rest for you.

We also have files for creating databases if that is required.

I've also included a requirements.txt file. This is the requirements file for actually running my Apache Storm code. You can run requirements.txt via the following command:
pip install -r requirements.txt
