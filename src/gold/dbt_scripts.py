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

def run_dim_location(logger: logging.Logger):
    result_run = run_dbt_command('run -s dim_locations')
    if result_run.returncode != 0:
        logger.error(result_run.stdout)
        logger.error(result_run.stderr)
        raise Exception('Dbt run failed')
    
def run_dim_date(logger: logging.Logger):
    result_run = run_dbt_command('run -s dim_date')
    if result_run.returncode != 0:
        logger.error(result_run.stdout)
        logger.error(result_run.stderr)
        raise Exception('Dbt run failed')
    
def run_fact_weather(logger: logging.Logger, upsert_from: str = None):
    if upsert_from is None:
        result_run = run_dbt_command('run -s fact_weather')
    else:
        result_run = run_dbt_command('''run -s fact_weather --vars '{ "upsert_from": "%s"}'''%(upsert_from))

    if result_run.returncode != 0:
        logger.error(result_run.stdout)
        logger.error(result_run.stderr)
        raise Exception('Dbt run failed')
    
def run_fact_koppen_model(logger: logging.Logger):
    result_run = run_dbt_command('run -s fact_koppen_model')
    if result_run.returncode != 0:
        logger.error(result_run.stdout)
        logger.error(result_run.stderr)
        raise Exception('Dbt run failed')
    
def run_tests(logger: logging.Logger):
    result_test = run_dbt_command('test')
    if result_test.returncode != 0:
        logger.error(result_test.stdout)
        logger.error(result_test.stderr)
        raise Exception('Dbt test failed')
    
def run_dbt_gold_scripts(logger: logging.Logger):
    logger.info('\tRunning dim_locations')
    run_dim_location(logger)
    
    logger.info('\tRunning dim_date')
    run_dim_date(logger)
    
    logger.info('\tRunning fact_weather')
    run_fact_weather(logger)

    logger.info('\tRunning fact_koppen_model')
    run_fact_koppen_model(logger)
    
    logger.info('\tRunning tests')
    run_tests(logger)
