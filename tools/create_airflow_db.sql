-- Create the new user
CREATE USER airflow WITH PASSWORD 'airflow_pw';

-- Create the new database
CREATE DATABASE airflow_data;

-- Grant all privileges on the new database to the new user
GRANT ALL PRIVILEGES ON DATABASE airflow_data TO airflow;

-- Optionally, you can set the new user as the owner of the new database
ALTER DATABASE airflow_data OWNER TO airflow;
