from logs.logger import logger_deltam_checker
from db.models import create_loan_checker, check_error

if __name__ == "__main__":
    try:
        check_error(create_loan_checker(1))
        check_error(create_loan_checker(2))

    except ValueError as err:
        logger_deltam_checker.error("Помилка даних main.py: " + str(err))
    except Exception as err:
        logger_deltam_checker.error("Помилка: " + str(err))
