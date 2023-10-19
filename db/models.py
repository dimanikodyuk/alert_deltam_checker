import pymysql
import pymssql
import telebot
from config import host_delta, user_delta, password_delta, database_delta, telegram_bot, host_dlm, user_dlm, passowrd_dlm, database_dlm, group_id
from logs.logger import logger_deltam_checker
conn = pymysql.connect(host=host_delta, port=3306, user=user_delta, passwd=password_delta, db=database_delta, charset="utf8")
conn_mssql = pymssql.connect(server=host_dlm, user=user_dlm, password=passowrd_dlm, database=database_dlm, charset='cp1251'
                             , autocommit=True)

bot = telebot.TeleBot(telegram_bot)
#bot.send_message(group_id, "Hello FinX", parse_mode="HTML")


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


def update_error_send_status(p_lead_id, p_error_id):
    upd = conn_mssql.cursor()
    #upd_sql = f"""UPDATE crm..finx_error_leads_bot set send_status = 1, dt_mod = getdate()
    #                where lead_id = {p_lead_id} and id = {p_error_id};""" # error_type = {p_error_type};"""
    upd_sql = f"EXEC crm..[alert_deltam_update] {p_lead_id}, {p_error_id}"
    print(f"upd_sql: {upd_sql}")
    upd.execute(upd_sql)
    upd.close()


# Процедура перевірки наявності помилок в БД crm (92 server)
def create_loan_checker_crm(p_type_id):
    checker = conn_mssql.cursor()
    checker_sql = f"EXEC crm..[alert_deltam_checker] {p_type_id}"
    print(f"checker_sql: {checker_sql}")
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

        message = f"""❗❗❗<b>Помилка</b>❗❗❗

🟦  <b>Сервіс:</b> <i>{error_type}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
"""
        bot.send_message(group_id, message, parse_mode="HTML")


# Генерація тексту помилки і відправка з 92 серверу
def check_error_crm(result_data):
    print(result_data)
    if result_data[0] == 1:
        error_type = result_data[1]
        error_text = result_data[2]
        error_lead = result_data[3]
        error_contract_num = result_data[4]
        error_type_report = result_data[5]
        error_check_type = result_data[6]
        error_id = result_data[7]
        error_inn = result_data[8]
        error_dt = result_data[9]
        logger_deltam_checker.info(f"Виявлено помилку: {error_text}")

        if error_type_report == 1:
            message = f"""❗❗❗<b>Помилка</b>❗❗❗

🟦  <b>Сервіс:</b> <i>{error_type}</i>

✏  <b>LEAD_ID:</b> <i>{error_lead}</i>

📄  <b>Договір:</b> <i>{error_contract_num}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """
        elif error_type_report == 2:
            message = f"""❗❗❗<b>Помилка</b>❗❗❗

🟦  <b>Сервіс:</b> <i>{error_type}</i>

🟨  <b>ІПН:</b> <i>{error_inn}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """

        elif error_type_report == 3:
            message = f"""❗❗❗<b>Помилка</b>❗❗❗

🟦  <b>Сервіс:</b> <i>{error_type}</i>

🟪  <b>Дата і час помилки:</b> <i>{error_dt}</i>

🟨  <b>Лід:</b> <i>{error_lead}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """

        bot.send_message(group_id, message, parse_mode="HTML")
        # Оновлення статусу відправки помилки по ліду з таблиці crm..finx_error_leads_bot
        update_error_send_status(error_lead, error_id)

# @bot.message_handler(content_types=['text'])
# def get_text_message(message):
#     if message.text == "test":
#         bot.send_message(message.from_user.id, message.chat.id, parse_mode="HTML")


#if __name__ == "__main__":
#    bot.polling()