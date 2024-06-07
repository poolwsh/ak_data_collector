@echo off
setlocal

rem Set the USERPROFILE environment variable
set USERPROFILE=%USERPROFILE%

rem Define the required directories
set directories=(
    "%USERPROFILE%\airflow_data\log"
    "%USERPROFILE%\airflow_data\cache"
    "%USERPROFILE%\airflow_data\db\redis\config"
    "%USERPROFILE%\airflow_data\db\redis\data"
    "%USERPROFILE%\airflow_data\db\timescaledb\data"
)

rem Create the directories if they don't exist
for %%d in %directories% do (
    if not exist "%%d" (
        echo Creating directory: %%d
        mkdir "%%d"
    ) else (
        echo Directory already exists: %%d
    )
)

rem Change to the directory containing the Dockerfile and docker-compose.yml
cd /d %~dp0

rem Build the Docker image
docker build -t my_airflow_image -f Dockerfile .

rem Start Docker Compose services
docker-compose up -d

endlocal
