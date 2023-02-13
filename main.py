from logs.logger import logger_deltam_checker
from db.models import create_loan_checker, check_error, get_checker_time, get_active_config
from datetime import datetime


if __name__ == "__main__":
    try:

        active_conf = get_active_config()
        for j in active_conf:
            print(f"Запуск перевірки з конфігу № {j}")
            # Поточний час
            current_time = datetime.now().time()

            # Статус, час запуску, час закінчення перевірок з конфігу за ід
            res = get_checker_time(j)
            time_start = datetime.strptime(res[1], '%H:%M:%S')
            time_end = datetime.strptime(res[2], '%H:%M:%S')

            # Якщо поточний час > за час початку перевірки а також поточний час < за час закінчення перевірки
            if res[0] == 1 and current_time >= time_start.time() and current_time <= time_end.time():
                check_error(create_loan_checker(j))

    except ValueError as err:
        logger_deltam_checker.error("Помилка даних main.py: " + str(err))
    except Exception as err:
        logger_deltam_checker.error("Помилка: " + str(err))
