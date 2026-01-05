import subprocess

def run_koppen_model():
    cmd = [
        'spark-submit',
        '--master', 'spark://spark-master:7077',
        '--deploy-mode', 'client',
        '--jars', '/opt/spark/jars/postgresql-42.6.0.jar',
        '--driver-class-path', '/opt/spark/jars/postgresql-42.6.0.jar',
        '/WeatherApiDE/src/silver/koppen_model.py'
    ]

    subprocess.run(cmd, check=True)