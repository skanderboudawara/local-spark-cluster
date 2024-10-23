@echo off
SETLOCAL ENABLEDELAYEDEXPANSION

set "BASE_PATH=/src/scripts"

REM Check if an argument is provided
if "%~1"=="" (
    echo No command provided.
    call :help
)

REM Parse the command
set command=%~1
shift

if /i "%command%"=="deploy" (
    echo Deploying the Spark cluster...
    docker-compose up -d --build
    if errorlevel 1 (
        echo Failed to deploy the Spark cluster.
        exit /b 1
    )
    echo Spark cluster successfuly deployed!
    exit /b
)

if /i "%command%"=="stop" (
    echo Stopping the Spark cluster...
    docker-compose down
    if errorlevel 1 (
        echo Failed to stop the Spark cluster.
        exit /b 1
    )
    echo Spark cluster successfuly stopped!
    exit /b
)

if /i "%command%"=="status" (
    echo Checking Spark cluster status..
    docker-compose ps
    exit /b
)

if /i "%command%"=="run" (
    if "%~1"=="" (
        echo No file specified to run.
        call :usage
    )

    set fileName=%~1
    set filePath=%BASE_PATH%/!fileName!

    echo Running Spark job from file: !fileName!
    docker exec -it spark-cluster-spark-master-1 spark-submit !filePath!

    if errorlevel 1 (
        echo Failed to execute Spark job: !fileName!
        exit /b 1
    )
    echo Successfully executed Spark job: !fileName!
    exit /b
)


echo Invalid command: %command%
call :help
