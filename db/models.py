import pymysql
import pymssql
import telebot
from config import (host_delta, user_delta, password_delta, database_delta, telegram_bot, host_dlm, user_dlm,
                    passowrd_dlm, database_dlm, group_id, nykodiuk_id, rovnyi_id, petrenko_id, harchenko_id)
from logs.logger import logger_deltam_checker

conn = pymysql.connect(host=host_delta, port=3306, user=user_delta, passwd=password_delta, db=database_delta,
                       charset="utf8")
conn_mssql = pymssql.connect(server=host_dlm, user=user_dlm, password=passowrd_dlm, database=database_dlm,
                             charset='cp1251', autocommit=True)

bot = telebot.TeleBot(telegram_bot)

# -- ШАБЛОНИ ПОВІДОМЛЕНЬ
template0 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i>

"""

# Аналог шаблону 0, але додатково показує процедуру для перевірки помилки чи щось інше
template10 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i>

<blockquote>Процедура перевірки: <code>{test_procedure}</code></blockquote>

"""

template1 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟪  <b>Дата і час помилки:</b> <i>{error_dt}</i>

✏  <b>LEAD_ID:</b> <i>{error_lead}</i>

📄  <b>Договір:</b> <i>{error_contract_num}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """

template2 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟪  <b>Дата і час помилки:</b> <i>{error_dt}</i>

🟨  <b>ІПН:</b> <i>{error_inn}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """

template3 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟪  <b>Дата і час помилки:</b> <i>{error_dt}</i>

🟨  <b>Лід:</b> <i>{error_lead}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """

template4 = """❗❗❗<b> УБКІ </b>❗❗❗
            
{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟪  <b>К-ть кредитів:</b> <i>{error_lead}</i>

🟨  <b>К-ть надісланих:</b> <i>{error_inn}</i>

🟥  <b>К-ть з критичною помилкою:</b> <i>{error_contract_num}</i>"""

template5 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟪  <b>Дата і час помилки:</b> <i>{error_dt}</i>

🟩  <b>Дата зміни:</b> <i>{error_data}</i>

🟨  <b>ІПН:</b> <i>{error_inn}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """

# Виключно під crm..cabinet_alert
template6 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟥 <b>Текст помилки:</b> <i>Крок {error_step}. Відхилення <b>{error_value}</b>%. Граничний показник <b>{error_check_value}</b>%. Показник на вчора <b>{error_yest_value}</b>%, сьогодні <b>{error_today_value}</b>%</i>"""

# Окремий костиль для Ровного, Петренка та Харченка

template7 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟪  <b>Дата і час помилки:</b> <i>{error_dt}</i>

🟨  <b>Лід:</b> <i>{error_lead}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """

template8 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟨 <b>Лід:</b> <i>{error_lead}</i>

🚹 <b>Клієнт:</b> <i>{client_id}</i>

🟥 <b>Тип:</b> <i>{error_text}</i> 
    """

# Шаблон під помилки в таблиці DeltaTellBox..WorkItems по полю KeyId
template9 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟨 <b>DialFlowId:</b> <i>{dial_flow_id}</i>

🟪 <b>WorkItemId:</b> <i>{work_item_id}</i>

🟥 <b>Тип:</b> <i>{error_text}</i> 
    """


def get_active_config():
    id_conf = []
    conf = conn_mssql.cursor()
    conf_sql = "SELECT id, db_id FROM crm..alert_deltam_config;"
    conf.execute(conf_sql)
    res = conf.fetchall()
    conf.close()
    for i in res:
        id_conf.append(i)
    return id_conf


def get_checker_time(p_type_id):
    checker = conn_mssql.cursor()
    checker_sql = f"""SELECT 
            CAST(LEFT(start_dt_check,8) AS VARCHAR(100)) as dt_start
        ,	CAST(LEFT(end_dt_check  ,8) AS VARCHAR(100)) as dt_end
        ,   hour_start_silent_send
        ,   hour_end_silent_send
        FROM crm..alert_deltam_config
        WHERE id = {p_type_id};"""
    checker.execute(checker_sql)
    res = checker.fetchone()
    checker.close()
    return res


# Функція перевірки на повторність запису по підпису документа за день
def check_repeat_type(p_repeat_type):
    res = ''
    if p_repeat_type == 0:
        res = '⚠'
    else:
        res = '♻'

    return res


# Процедура перевірки наявності помилок в БД leads_api (91 server)
def create_loan_checker_leads_api(p_type_id):
    try:
        check = conn.cursor()
        check_sql = f"CALL leads_api.alert_deltam_checker({p_type_id});"
        check.execute(check_sql)
        res = check.fetchall()
        check.close()
        return res[0]

    except ValueError as err:
        logger_deltam_checker.error("Помилка даних models.py - create_loan_checker_leads_api: " + str(err))
    except Exception as err:
        logger_deltam_checker.error("Помилка create_loan_checker_leads_api: " + str(err))
    except pymysql.Error as err:
        logger_deltam_checker.error("Помилка pymysql.Error: " + str(err))
    except pymysql.MySQLError as err:
        logger_deltam_checker.error("Помилка pymysql.MySQLError: " + str(err))


def update_error_send_status(p_lead_id, p_error_id):
    upd = conn_mssql.cursor()
    upd_sql = f"EXEC crm..alert_deltam_update {p_lead_id}, {p_error_id}"
    upd.execute(upd_sql)
    upd.close()


# Процедура перевірки наявності помилок в БД crm (92 server)
def create_loan_checker_crm(p_type_id):
    try:
        checker = conn_mssql.cursor()
        checker_sql = f"EXEC crm..alert_deltam_checker {p_type_id}"
        print(f"checker_sql: {checker_sql}")
        checker.execute(checker_sql)
        res = checker.fetchall()
        checker.close()
        return res
    except ValueError as err:
        logger_deltam_checker.error("Помилка даних models.py- create_loan_checker_crm: " + str(err))
    except Exception as err:
        logger_deltam_checker.error("Помилка create_loan_checker_crm: " + str(err))
    except pymssql.Error as err:
        logger_deltam_checker.error("Помилка pymssql.Error: " + str(err))
    except pymssql.DatabaseError as err:
        logger_deltam_checker.error("Помилка pymssql.DatabaseError: " + str(err))


# Генерація тексту помилки і відправка з 91 серверу
def check_error_leads_api(result_data, p_silent_send):
    if result_data[0] == 1:
        error_type = result_data[1]
        error_text = result_data[2]
        logger_deltam_checker.info(f"Виявлено помилку: {error_text}")

        message = template0.format(error_type=error_type, error_text=error_text, repeat_id=1, repeat_type='⚠')

        if p_silent_send == 1:
            bot.send_message(group_id, message, parse_mode="HTML", disable_notification=True)
        else:
            bot.send_message(group_id, message, parse_mode="HTML")

# Генерація тексту помилки і відправка з 92 серверу
def check_error_crm(result_data, p_silent_send):
    print(result_data)

    if result_data[0] == 1:
        error_type = result_data[1]
        error_text = result_data[2]
        error_lead = result_data[3]
        error_contract_num = result_data[4]
        error_type_report = result_data[5]
        #error_check_type = result_data[6]
        error_id = result_data[7]
        error_inn = result_data[8]
        error_dt = result_data[9]
        error_repeat = result_data[10]
        repeat_type = check_repeat_type(error_repeat)
        repeat_id = result_data[11]
        error_data = result_data[12]
        par1 = result_data[13]
        par2 = result_data[14]
        par3 = result_data[15]
        par4 = result_data[16]
        par5 = result_data[17]
        client_id = result_data[18]
        dial_flow_id = result_data[19]
        work_item_id = result_data[20]
        test_procedure = result_data[21]
        logger_deltam_checker.info(f"Виявлено помилку: {error_text}")

        if error_type_report == 0:
            if test_procedure is None:
                message = template0.format(error_type=error_type, error_text=error_text,
                                           repeat_type=repeat_type, repeat_id=repeat_id)
            else:
                message = template10.format(error_type=error_type, error_text=error_text,
                                           repeat_type=repeat_type, repeat_id=repeat_id, test_procedure=test_procedure)

        if error_type_report == 1:
            message = template1.format(error_type=error_type, error_lead=error_lead, error_dt=error_dt,
                                       error_contract_num=error_contract_num, error_text=error_text,
                                       repeat_type=repeat_type, repeat_id=repeat_id)

        elif error_type_report == 2:
            message = template2.format(error_type=error_type, error_inn=error_inn, error_text=error_text,
                                       repeat_type=repeat_type, repeat_id=repeat_id, error_dt=error_dt)

        elif error_type_report == 3:
            message = template3.format(error_type=error_type, error_dt=error_dt, error_lead=error_lead,
                                       error_text=error_text, repeat_type=repeat_type, repeat_id=repeat_id)

        elif error_type_report == 4:
            message = template4.format(error_type=error_type, error_lead=error_lead, error_inn=error_inn,
                                       error_contract_num=error_contract_num, repeat_type=repeat_type,
                                       repeat_id=repeat_id)

        elif error_type_report == 5:
            message = template5.format(error_type=error_type, error_inn=error_inn, error_text=error_text,
                                       repeat_type=repeat_type, repeat_id=repeat_id, error_dt=error_dt, error_data=error_data)
            bot.send_message(rovnyi_id, message, parse_mode="HTML")
            bot.send_message(petrenko_id, message, parse_mode="HTML")
            bot.send_message(harchenko_id, message, parse_mode="HTML")

        elif error_type_report == 6:
            message = template6.format(repeat_type=repeat_type, error_type=error_type, repeat_id=repeat_id,
                                       error_step=par1, error_value=par2, error_check_value=par3,
                                       error_yest_value=par4, error_today_value=par5)

        elif error_type_report == 7:
            message = template7.format(error_type=error_type, error_dt=error_dt, error_lead=error_lead,
                                       error_text=error_text, repeat_type=repeat_type, repeat_id=repeat_id)
            bot.send_message(rovnyi_id, message, parse_mode="HTML")
            bot.send_message(petrenko_id, message, parse_mode="HTML")
            bot.send_message(harchenko_id, message, parse_mode="HTML")

        elif error_type_report == 8:
            message = template7.format(error_type=error_type, error_dt=error_dt, error_lead=error_lead, client_id=client_id,
                                       error_text=error_text, repeat_type=repeat_type, repeat_id=repeat_id)
            bot.send_message(rovnyi_id, message, parse_mode="HTML")
            bot.send_message(petrenko_id, message, parse_mode="HTML")
            #bot.send_message(nykodiuk_id, message, parse_mode="HTML")
            #bot.send_message(harchenko_id, message, parse_mode="HTML")

        elif error_type_report == 9:
            message = template9.format(error_type=error_type, error_dt=error_dt, dial_flow_id=dial_flow_id, work_item_id=work_item_id,
                                       error_text=error_text, repeat_type=repeat_type, repeat_id=repeat_id)

        print(f"SILENT_MODE: {p_silent_send}")
        if p_silent_send == 1:
            bot.send_message(group_id, message, parse_mode="HTML", disable_notification=True)
        else:
            bot.send_message(group_id, message, parse_mode="HTML")
        # Оновлення статусу відправки помилки по ліду з таблиці crm..finx_error_leads_bot
        update_error_send_status(error_lead, error_id)

# bot.send_sticker(nykodiuk_id, 'CAACAgIAAxkBAAEK6QdlcHvOrfQRI-XsU2xHhBhSoQ1UnQACIQAD_wzODBFb9FtzRu_LMwQ')
# @bot.message_handler(content_types=['text'])
# def get_text_message(message):
#     if message.text == "test":
#         bot.send_message(message.from_user.id, message.chat.id, parse_mode="HTML")


# if __name__ == "__main__":
#    bot.polling()
