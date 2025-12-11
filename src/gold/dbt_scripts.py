import subprocess
import logging

def run_dbt_command(command: str):
    result = subprocess.run(
        ['dbt'] +  command.split(),
        cwd='/WeatherApiDE/dbt',
        capture_output=True,
        text=True   
    )

    return result

def run_dbt_gold_scripts(logger: logging.Logger):
    logger.info('\tRunning dim_locations')
    result_run = run_dbt_command('run -s dim_locations')
    if result_run.returncode != 0:
        logger.error(result_run.stdout)
        logger.error(result_run.stderr)
        raise Exception('Dbt run failed')
    
    logger.info('\tRunning dim_date')
    result_run = run_dbt_command('run -s dim_date')
    if result_run.returncode != 0:
        logger.error(result_run.stdout)
        logger.error(result_run.stderr)
        raise Exception('Dbt run failed')
    
    logger.info('\tRunning fact_weather')
    result_run = run_dbt_command('run -s fact_weather')
    if result_run.returncode != 0:
        logger.error(result_run.stdout)
        logger.error(result_run.stderr)
        raise Exception('Dbt run failed')
    
    logger.info('\tRunning tests')
    result_test = run_dbt_command('test')
    if result_test.returncode != 0:
        logger.error(result_test.stdout)
        logger.error(result_test.stderr)
        raise Exception('Dbt test failed')