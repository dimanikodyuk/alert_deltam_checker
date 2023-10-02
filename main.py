from logs.logger import logger_deltam_checker
from db.models import get_active_config, get_checker_time, create_loan_checker_leads_api, check_error_leads_api, \
    create_loan_checker_crm, check_error_crm
from datetime import datetime


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

            # Якщо статус конфігу = 1, а також поточний час > за час початку перевірки а також поточний час < за час закінчення перевірки
            if current_time >= time_start.time() and current_time <= time_end.time():
                # Якщо БД leads_api
                if int(j[1]) == 1:
                    check_error_leads_api(create_loan_checker_leads_api(j[0]))
                # Якщо БД crm
                elif int(j[1]) == 2:
                    result = create_loan_checker_crm(j[0])
                    for i in result:
                        check_error_crm(i)
        print("--------------------------------------------------------")

    except ValueError as err:
        logger_deltam_checker.error("Помилка даних main.py: " + str(err))
    except Exception as err:
        logger_deltam_checker.error("Помилка: " + str(err))
