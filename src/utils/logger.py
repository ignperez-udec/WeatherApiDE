import logging
import os
from utils.config import load_variables  # type: ignore

def config_log(name_log: str = 'app',
           level: str = logging.DEBUG):
    
    CONFIG_VARS = load_variables()

    os.makedirs(CONFIG_VARS['LOG_PATH'], exist_ok=True)

    logger = logging.getLogger(name_log)
    logger.setLevel(level)

    if not logger.handlers:
        file_handler = logging.FileHandler(os.path.join(CONFIG_VARS['LOG_PATH'], name_log) + '.log', mode='w')
        file_handler.setLevel(level)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


        
        

