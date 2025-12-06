from utils.config import load_variables
import os

def write_init_flag(flag: str):
    CONFIG_VARS = load_variables()

    path_flag = os.path.join(CONFIG_VARS['LOG_PATH'], 'init_done')
    with open(path_flag, 'w') as f:
        f.write(flag)
