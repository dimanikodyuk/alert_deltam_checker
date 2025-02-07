import pymysql
import pymssql
import telebot
import os
from config import (host_delta, user_delta, password_delta, database_delta, telegram_bot, host_dlm, user_dlm,
                    passowrd_dlm, database_dlm, group_id, nykodiuk_id, rovnyi_id, petrenko_id, harchenko_id)
from logs.logger import logger_deltam_checker

conn = pymysql.connect(host=host_delta, port=3306, user=user_delta, passwd=password_delta, db=database_delta,
                       charset="utf8")
conn_mssql = pymssql.connect(server=host_dlm, user=user_dlm, password=passowrd_dlm, database=database_dlm,
                             charset='cp1251', autocommit=True)
import requests

bot = telebot.TeleBot(telegram_bot)
#ubki_img = open('db/ubki.png', 'rb')

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

template11 = """❗❗❗<b>Помилка</b>❗❗❗

{repeat_type}  <b>Сервіс:</b> <i>{error_type} ({repeat_id})</i>

🟨 <b>Помилка:</b> <i>{par1}</i>

🟪 <b>Опис:</b> <i>{par2}</i>

🟥 <b>К-ть:</b> <i>{par3}</i> 
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
        send_global_error(err)
    except Exception as err:
        logger_deltam_checker.error("Помилка create_loan_checker_leads_api: " + str(err))
        send_global_error(err)
    except pymysql.Error as err:
        logger_deltam_checker.error("Помилка pymysql.Error: " + str(err))
        send_global_error(err)
    except pymysql.MySQLError as err:
        logger_deltam_checker.error("Помилка pymysql.MySQLError: " + str(err))
        send_global_error(err)


def send_global_error(p_error_text):
    url = f"https://api.telegram.org/bot{telegram_bot}/sendMessage?chat_id={nykodiuk_id}&text={p_error_text}"
    requests.get(url)


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
        send_global_error(err)
    except Exception as err:
        logger_deltam_checker.error("Помилка create_loan_checker_crm: " + str(err))
        send_global_error(err)
    except pymssql.Error as err:
        logger_deltam_checker.error("Помилка pymssql.Error: " + str(err))
        send_global_error(err)
    except pymssql.DatabaseError as err:
        logger_deltam_checker.error("Помилка pymssql.DatabaseError: " + str(err))
        send_global_error(err)


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
    try:
        if result_data[0] == 1:
            (
                error_type, error_text, error_lead, error_contract_num, error_type_report, _,
                error_id, error_inn, error_dt, error_repeat, repeat_id, error_data,
                par1, par2, par3, par4, par5, client_id, dial_flow_id, work_item_id,
                test_procedure, img
            ) = result_data[1:23]

            repeat_type = check_repeat_type(error_repeat)

            # Всі шаблони зберігаємо в словнику
            templates = {
                0: template0 if test_procedure is None else template10,
                1: template1,
                2: template2,
                3: template3,
                4: template4,
                5: template5,
                6: template6,
                7: template7,
                8: template7,
                9: template9,
                11: template11,
            }

            #image_folder = "images"
            image_filename = img if img is not None else ""
            #image_path = os.path.join(image_folder, image_filename)
            #print(f"Зображення: {image_path}. Папка: {image_folder}, картинка: {image_filename}. Оригінал img: {img}")

            # Отримуємо шаблон за ключем
            message_template = templates.get(error_type_report, "❌ Невідомий тип помилки")
            message = message_template.format(**locals())  # Використовуємо тільки доступні змінні

            if error_type_report == 5 or error_type_report == 7:
                bot.send_message(rovnyi_id, message, parse_mode="HTML")
                bot.send_message(petrenko_id, message, parse_mode="HTML")
                bot.send_message(harchenko_id, message, parse_mode="HTML")

            if error_type_report == 8:
                bot.send_message(rovnyi_id, message, parse_mode="HTML")
                bot.send_message(petrenko_id, message, parse_mode="HTML")

            # Відправка фото, якщо файл існує
            if os.path.isfile(image_filename):
                with open(image_filename, "rb") as photo:
                    bot.send_photo(nykodiuk_id, photo, caption=message, parse_mode="HTML",
                                   disable_notification=bool(p_silent_send))
            else:
                print(f"❌ Файл {image_filename} не знайдено! Відправляємо лише текст.")
                bot.send_message(nykodiuk_id, message, parse_mode="HTML",
                                 disable_notification=bool(p_silent_send))

            # Оновлення статусу відправки помилки по ліду з таблиці crm..finx_error_leads_bot
            update_error_send_status(error_lead, error_id)

    except ValueError as err:
        logger_deltam_checker.error("Помилка даних models.py-check_error_crm: " + str(err))
        send_global_error(err)
    except Exception as err:
        logger_deltam_checker.error("Помилка models.py-check_error_crm: " + str(err))
        send_global_error(err)
    except EnvironmentError as err:
        logger_deltam_checker.error("Помилка Environmental models.py-check_error_crm: " + str(err))
        send_global_error(err)


# bot.send_sticker(nykodiuk_id, 'CAACAgIAAxkBAAEK6QdlcHvOrfQRI-XsU2xHhBhSoQ1UnQACIQAD_wzODBFb9FtzRu_LMwQ')
# @bot.message_handler(content_types=['text'])
# def get_text_message(message):
#     if message.text == "test":
#         bot.send_message(message.from_user.id, message.chat.id, parse_mode="HTML")


# if __name__ == "__main__":
#    bot.polling()
