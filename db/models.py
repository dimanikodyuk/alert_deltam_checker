import pymysql
from config import host_delta, user_delta, password_delta, database_delta, telegram_bot, nykodiuk_id, pavluchenko_id, goncharuk_id
from logs.logger import logger_deltam_checker
conn = pymysql.connect(host=host_delta, port=3306, user=user_delta, passwd=password_delta, db=database_delta,
                       autocommit=True, charset="utf8")
import telebot
bot = telebot.TeleBot(telegram_bot)


# Процедура перевірки
def create_loan_checker(p_type_id):
    check = conn.cursor()
    check_sql = f"CALL leads_api.alert_deltam_checker({p_type_id});"
    check.execute(check_sql)
    res = check.fetchall()
    check.close()
    return res[0]


# Генерація тексту помилки і відправка
def check_error(result_data):
    if result_data[0] == 1:
        error_text = result_data[1]
        error_type = result_data[2]
        logger_deltam_checker.info(f"Виявлено помилку: {error_text}")

        message = f"""❗❗❗<b>Виявлено помилку</b>❗❗❗
-------------------------------------------------------------------------
↪  <b>Сервіс:</b> <i>{error_type}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
-------------------------------------------------------------------------
"""
        bot.send_message(nykodiuk_id, message, parse_mode="HTML")

