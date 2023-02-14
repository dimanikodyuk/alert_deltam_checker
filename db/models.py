import pymysql
import pymssql
import telebot
from config import host_delta, user_delta, password_delta, database_delta, telegram_bot, nykodiuk_id, pavluchenko_id\
    , goncharuk_id, rovnyi_id, host_dlm, user_dlm, passowrd_dlm, database_dlm
from logs.logger import logger_deltam_checker
conn = pymysql.connect(host=host_delta, port=3306, user=user_delta, passwd=password_delta, db=database_delta, charset="utf8")
conn_mssql = pymssql.connect(server=host_dlm, user=user_dlm, password=passowrd_dlm, database=database_dlm, charset='cp1251')

bot = telebot.TeleBot(telegram_bot)


def get_active_config():
    id_conf = []
    conf = conn.cursor()
    conf_sql = f"select id, db_id from leads_api.alert_deltam_config where is_active = 1"
    conf.execute(conf_sql)
    res = conf.fetchall()
    conf.close()
    for i in res:
        id_conf.append(i)
    return id_conf


def get_checker_time(p_type_id):
    checker = conn.cursor()
    checker_sql = f"""SELECT CAST(start_check_dt AS VARCHAR(100)) as dt_start, CAST(end_check_dt AS varchar(100)) AS dt_end
                        FROM leads_api.alert_deltam_config
                        WHERE id = {p_type_id};"""
    checker.execute(checker_sql)
    res = checker.fetchone()
    checker.close()
    return res


# Процедура перевірки наявності помилок в БД leads_api (91 server)
def create_loan_checker_leads_api(p_type_id):
    check = conn.cursor()
    check_sql = f"CALL leads_api.alert_deltam_checker({p_type_id});"
    check.execute(check_sql)
    res = check.fetchall()
    check.close()
    return res[0]


# Процедура перевірки наявності помилок в БД crm (92 server)
def create_loan_checker_crm(p_type_id):
    checker = conn_mssql.cursor()
    checker_sql = f"EXEC crm..[alert_deltam_checker] {p_type_id}"
    checker.execute(checker_sql)
    res = checker.fetchall()
    checker.close()
    return res

# Генерація тексту помилки і відправка з 91 серверу
def check_error_leads_api(result_data):
    if result_data[0] == 1:
        error_type = result_data[1]
        error_text = result_data[2]
        logger_deltam_checker.info(f"Виявлено помилку: {error_text}")

        message = f"""❗❗❗<b>Виявлено помилку</b>❗❗❗

🟦  <b>Сервіс:</b> <i>{error_type}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
"""
        bot.send_message(nykodiuk_id, message, parse_mode="HTML")
        bot.send_message(pavluchenko_id, message, parse_mode="HTML")
        bot.send_message(goncharuk_id, message, parse_mode="HTML")
        bot.send_message(rovnyi_id, message, parse_mode="HTML")


# Генерація тексту помилки і відправка з 92 серверу
def check_error_crm(result_data):
    if result_data[0] == 1:
        error_type = result_data[1]
        error_text = result_data[2]
        error_lead = result_data[3]
        error_contract_num = result_data[4]
        logger_deltam_checker.info(f"Виявлено помилку: {error_text}")

        message = f"""❗❗❗<b>Виявлено помилку</b>❗❗❗

🟦  <b>Сервіс:</b> <i>{error_type}</i>

✏  <b>LEAD_ID:</b> <i>{error_lead}</i>

📄  <b>Договір:</b> <i>{error_contract_num}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """
        bot.send_message(nykodiuk_id, message, parse_mode="HTML")
        bot.send_message(pavluchenko_id, message, parse_mode="HTML")
        bot.send_message(goncharuk_id, message, parse_mode="HTML")
        bot.send_message(rovnyi_id, message, parse_mode="HTML")
