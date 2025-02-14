import os
import datetime as dt
from utils.common_func import download_static_files, download_rt_files
from utils.config import STATIC_URL, REALTIME_URL, PATH_STATIC, PATH_RT

def main():
    
    file_name = dt.datetime.strftime(dt.datetime.now(), "%Y%m%d_%H%M")
    
    download_static_files(STATIC_URL, os.path.join(PATH_STATIC, file_name))
    download_rt_files(REALTIME_URL, os.path.join(PATH_RT, file_name))

if __name__ == '__main__':
    main()