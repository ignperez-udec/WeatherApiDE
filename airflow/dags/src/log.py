import sys
import logging

sys.path.append("/opt/airflow/src")

from utils.logger import config_log  # type: ignore

def get_logger(name):
    return config_log(name_log=name, level=logging.INFO)

def log_failure_error(context, logger):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('dag').dag_id
    exception = context.get('exception')

    logger.error('Dag=' + dag_id + ', Task=' + task_id )
    logger.error(exception, exc_info=True)

def log_retry_error(context, logger):
    task = context.get('task_instance')
    dag_id = context.get('dag').dag_id

    current = task.try_number - 1
    remaining = task.max_tries - current

    logger.warning('Dag=' + dag_id + ', Task=' + task.task_id)
    logger.warning('Retry #' + str(current) + ', Remaining=' + str(remaining))