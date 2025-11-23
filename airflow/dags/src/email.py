from airflow.utils.email import send_email # type: ignore

def notify_failure(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    error = context.get('exception')
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    subject = f"Airflow Alert: DAG {dag_id} - Task {task_id} Failed"
    html_content = f"""
    Ocurrio un error en la ejecución del DAG <strong>{dag_id}</strong>.<br><br>
    <strong>Detalles del error:</strong><br>
    - <strong>Tarea:</strong> {task_id}<br>
    - <strong>Fecha de Ejecución:</strong> {execution_date}<br>
    - <strong>Error:</strong> {error}<br>
    - <strong>Logs:</strong> <a href="{log_url}">Ver Logs</a><br><br>
    """

    send_email(to=['ignacio.perez.cerdeira@gmail.com'], subject=subject, html_content=html_content)

def notify_success(context):
    dag_id = context.get('dag').dag_id
    execution_date = context.get('execution_date')

    subject = f"Airflow Notification: DAG {dag_id} Succeeded"
    html_content = f"""
    El DAG <strong>{dag_id}</strong> se ha completado exitosamente.<br><br>
    <strong>Detalles de la ejecución:</strong><br>
    - <strong>Fecha de Ejecución:</strong> {execution_date}<br><br>
    """

    send_email(to=['ignacio.perez.cerdeira@gmail.com'], subject=subject, html_content=html_content)