# -*- coding: utf-8 -*-
import os
import schedule
import time

if __name__ == "__main__":
    #
    def main():
        os.system("start python " + r"D:\pythonworkspace\github\XtQuant\src\app_xt2vn.py")
    #
    if "08:45" < time.strftime('%H:%M') < "16:11":
        main()
    schedule.every().day.at("08:45").do(main)
    print("xtdata 定时任务 进程")
    while True:
        schedule.run_pending()
        time.sleep(1)