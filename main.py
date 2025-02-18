import argparse
import os
import logging
import datetime as dt
from utils.common_func import download_static_files, download_rt_files
from utils.config import STATIC_URL, REALTIME_URL, PATH_STATIC, PATH_RT

def main(type):

    if type == 'static':
        file_name = dt.datetime.strftime(dt.datetime.now(), "%Y%m%d")
        logging.info(f"Start static cronjob at: {file_name}")
        download_static_files(STATIC_URL, os.path.join(PATH_STATIC, file_name))
    else:
        file_name = dt.datetime.strftime(dt.datetime.now(), "%Y%m%d_%H%M")
        logging.info(f"Start real-time cronjob at: {file_name})")
        download_rt_files(REALTIME_URL, os.path.join(PATH_RT, file_name))

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_type', choices=['real_time', 'static'], type=str, default='real_time',
                        help="Data type must be either 'real_time' or 'static'.")
    
    args = parser.parse_args()
    main(args.data_type)