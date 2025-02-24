from logs.logger import logger_deltam_checker
from db.models import get_active_config, get_checker_time, create_loan_checker_leads_api, check_error_leads_api, \
    create_loan_checker_crm, check_error_crm, send_global_error, send_gms_error
from datetime import datetime
from config import telegram_bot, nykodiuk_id
import requests

if __name__ == "__main__":
    try:
        # Витягуємо всі активні конфігурації з БД
        active_conf = get_active_config()
        print(f"Час запуску: {datetime.now()}")
        print("--------------------------------------------------------")
        for j in active_conf:
            print(f"Запуск перевірки з конфігу № {j[0]}")
            # Поточний час
            current_time = datetime.now().time()
            res = get_checker_time(j[0])
            # Час запуску перевірки
            time_start = datetime.strptime(res[0], '%H:%M:%S')
            # Час зупинки перевірки
            time_end = datetime.strptime(res[1], '%H:%M:%S')

            #Якщо статус конфігу = 1, а також поточний час > за час початку перевірки а також поточний час < за час закінчення перевірки
            if current_time >= time_start.time() and current_time <= time_end.time():
                # Якщо БД leads_api (91)
                # Зміна для розуміння, чи це має бути тиха відправка, чи ні
                if current_time.hour >= res[2] or current_time.hour < res[3]:
                    silent_send = 1
                else:
                    silent_send = 0

                if int(j[1]) == 1:
                    check_error_leads_api(create_loan_checker_leads_api(j[0]), silent_send)
                # Якщо БД crm (92)
                elif int(j[1]) == 2:
                    result = create_loan_checker_crm(j[0])
                    if result is not None:
                        for i in result:
                            check_error_crm(i, silent_send)
                elif int(j[1]) == 3:
                    send_gms_error(silent_send)

        print("--------------------------------------------------------")


    except ValueError as err:
        logger_deltam_checker.error("Помилка даних main.py: " + str(err))
        send_global_error(err)
    except Exception as err:
        logger_deltam_checker.error("Помилка: " + str(err))
        send_global_error(err)
    except EnvironmentError as err:
        logger_deltam_checker.error("Помилка Environment main.py: " + str(err))
        send_global_error(err)
