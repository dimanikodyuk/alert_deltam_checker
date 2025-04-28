import pymysql
import pymssql
import telebot
import platform
import subprocess
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

template12 = """❗❗❗<b>Помилка</b>❗❗❗

🟪 <b>Сервіс:</b> <i>{error_type}</i>

🟨 <b>Помилка:</b> <i>{error_text}</i>
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

🟥 <b>Текст помилки:</b> <i>Крок {par1}. Відхилення <b>{par2}</b>%. Граничний показник <b>{par3}</b>%. Показник на вчора <b>{par4}</b>%, сьогодні <b>{par5}</b>%</i>"""

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
    try:
        with conn_mssql.cursor() as conf:
            conf.execute("SELECT id, db_id FROM crm..alert_deltam_config;")
            return conf.fetchall()  # fetchall() вже повертає список кортежів
    except Exception as e:
        print(f"Помилка під час виконання SQL-запиту: {e}")
        return []  # Повертаємо порожній список у разі помилки


def get_checker_time(p_type_id):

    try:

        with conn_mssql.cursor() as checker:

            checker_sql = """SELECT 
                       CAST(LEFT(start_dt_check, 8) AS VARCHAR(100)) as dt_start,
                       CAST(LEFT(end_dt_check, 8) AS VARCHAR(100)) as dt_end,
                       hour_start_silent_send,
                       hour_end_silent_send
                   FROM crm..alert_deltam_config
                   WHERE id = %s;"""
            checker.execute(checker_sql, (p_type_id,))  # Параметризований запит
            res = checker.fetchone()
            if res:
                return res
            else:
                logger_deltam_checker.warning(f"Немає запису з id = {p_type_id}")
                return None
    except Exception as e:
        logger_deltam_checker.error(f"Помилка отримання даних: {e}")
        return None


def get_chat_id(p_type_id):
    try:
        with conn_mssql.cursor() as checker:
            checker_sql = "select chat_id from crm..alert_deltam_config where id = %s;"
            checker.execute(checker_sql, (p_type_id,))  # Параметризований запит
            res = checker.fetchall()
            if res:
                chat_ids = res[0][0]  # '538001061,502287136,310797108'
                chat_id_list = chat_ids.split(',')

                return chat_id_list
            else:
                logger_deltam_checker.warning(f"GET_CHAT_ID -> Немає запису з id = {p_type_id}")
                return None
    except Exception as e:
        logger_deltam_checker.error(f"GET_CHAT_ID -> Помилка отримання даних: {e}")
        return None

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
        with conn.cursor() as check:
            # Використовуємо параметризований запит для безпеки
            check_sql = "CALL leads_api.alert_deltam_checker(%s);"
            check.execute(check_sql, (p_type_id,))

            res = check.fetchall()

            if res:
                return res[0]
            else:
                logger_deltam_checker.warning(f"Процедура не повернула результат для id = {p_type_id}")
                return None

    except pymysql.MySQLError as err:
        logger_deltam_checker.error("Помилка pymysql.MySQLError: " + str(err))
        send_global_error(err)
    except ValueError as err:
        logger_deltam_checker.error("Помилка даних models.py - create_loan_checker_leads_api: " + str(err))
        send_global_error(err)
    except Exception as err:
        logger_deltam_checker.error("Помилка create_loan_checker_leads_api: " + str(err))
        send_global_error(err)


def send_global_error(p_error_text):
    #url = f"https://api.telegram.org/bot{telegram_bot}/sendMessage?chat_id={nykodiuk_id}&text={p_error_text}"
    #requests.get(url)
    message = f"""❗❗❗<b>Глобальна помилка БОТУ</b>❗❗❗
    
🟥 <b>Текст:</b> <i>{p_error_text}</i> 
"""
    bot.send_message(nykodiuk_id, message, parse_mode="HTML")


def update_error_send_status(p_lead_id, p_error_id):
    try:
        with conn_mssql.cursor() as upd:
            # Параметризований виклик процедури
            upd_sql = "EXEC crm..alert_deltam_update %s, %s"
            upd.execute(upd_sql, (p_lead_id, p_error_id))
            conn_mssql.commit()  # Коміт транзакції
    except Exception as err:
        logger_deltam_checker.error(f"Помилка під час оновлення статусу відправки помилки: {err}")
        send_global_error(err)


def ping_gms_host(host: str) -> str:
    """
    Виконує команду ping для вказаного хоста і повертає результат.

    :param host: Доменне ім'я або IP-адреса хоста
    :return: Результат виконання команди ping
    """
    try:
        # Перевірка операційної системи і вибір параметрів для ping
        system_type = platform.system().lower()

        if system_type == 'windows':
            # Для Windows використовуємо -n і -w
            result = subprocess.run(
                ["ping", "-n", "4", "-w", "10000", host],  # -w в мілісекундах для Windows
                capture_output=True,
                text=True,
                encoding="cp866",  # Для правильного відображення тексту в Windows
                check=False  # Не викликати виняток при помилці
            )
        else:
            # Для Linux використовуємо -c і -W
            result = subprocess.run(
                ["ping", "-c", "4", "-W", "10", host],  # -W в секундах для Linux
                capture_output=True,
                text=True,
                check=False  # Не викликати виняток при помилці
            )

        output = result.stdout
        print(f"""---------------------------------------------
    PING GMS: {output}""")

        # Перевіряємо наявність відповіді в результаті ping
        if "bytes from" in output:
            return 0  # Успішний пінг
        else:
            return 1  # Хост не відповідає
    except Exception as e:
        logger_deltam_checker.error(f"Помилка виконання ping: {e}")
        return 2  # Випадок, якщо команда взагалі не запускається


def send_gms_error(p_silent_send):
    host_name = "proxy.hyber.im"
    check_gms_ping = ping_gms_host(host_name)
    logger_deltam_checker.info(f"Викоанння ping: {check_gms_ping}")
    if check_gms_ping != 0:
        if check_gms_ping == 1:
            msg_error = f"Хост {host_name} - не відповідає"
        else:
            msg_error = f"Помилка виконання ping по хосту {host_name}"

        logger_deltam_checker.error(msg_error)
        message = template12.format(error_type="Перевірка GMS", error_text=msg_error)

            # Відправлення повідомлення
        bot.send_message(
            group_id,
            message,
            parse_mode="HTML",
            disable_notification=bool(p_silent_send)
        )


# Процедура перевірки наявності помилок в БД crm (92 server)
def create_loan_checker_crm(p_type_id):
    try:
        with conn_mssql.cursor() as checker:
            # Параметризований виклик з захистом від SQL-ін'єкції
            checker_sql = "EXEC crm..alert_deltam_checker %s"
            print(f"checker_sql: {checker_sql} with p_type_id = {p_type_id}")
            checker.execute(checker_sql, (p_type_id,))

            res = checker.fetchall()

            if res:
                return res
            else:
                #logger_deltam_checker.warning(f"Процедура не повернула результат для id = {p_type_id}")
                return None

        conn_mssql.commit()  # Зберігаємо транзакцію після виконання
    except pymssql.DatabaseError as err:
        logger_deltam_checker.error("Помилка pymssql.DatabaseError: " + str(err))
        send_global_error(err)
    except pymssql.Error as err:
        logger_deltam_checker.error("Помилка pymssql.Error: " + str(err))
        send_global_error(err)
    except ValueError as err:
        logger_deltam_checker.error("Помилка даних models.py - create_loan_checker_crm: " + str(err))
        send_global_error(err)
    except Exception as err:
        logger_deltam_checker.error("Помилка create_loan_checker_crm: " + str(err))
        send_global_error(err)


# Генерація тексту помилки і відправка з 91 серверу
def check_error_leads_api(result_data, p_silent_send):
    #print(f"RESULT_DATA: {result_data} , SILENT_SEND: {p_silent_send}")
    try:
        # Перевіряємо, чи є необхідні дані
        if not result_data or len(result_data) < 3:
            logger_deltam_checker.warning("Неправильний формат даних для перевірки помилок.")
            return

        if result_data[0] == 1:
            error_type = result_data[1]
            error_text = result_data[2]
            logger_deltam_checker.info(f"Виявлено помилку: {error_text}")

            message = template0.format(
                error_type=error_type, error_text=error_text, repeat_id=1, repeat_type='⚠'
            )

            # Відправлення повідомлення з урахуванням режиму
            bot.send_message(
                group_id,
                message,
                parse_mode="HTML",
                disable_notification=bool(p_silent_send)
            )
    except IndexError as err:
        logger_deltam_checker.error(f"IndexError у check_error_leads_api: {err}")
        send_global_error(err)
    except Exception as err:
        logger_deltam_checker.error(f"Помилка в check_error_leads_api: {err}")
        send_global_error(err)


# Функція запису sql запиту в файл і його відправка
def send_sql_file(p_bot, chat_id, sql_query, message_text):
    # Зберігаємо SQL-запит у файл
    file_path = "sql_query.sql"
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(sql_query)

    # Відправляємо повідомлення з файлом
    with open(file_path, "rb") as file:
        p_bot.send_document(
            chat_id,
            file,
            caption=message_text,  # Повідомлення разом із файлом
            parse_mode="HTML"
        )

    # Видаляємо файл після відправлення (не обов'язково)
    os.remove(file_path)



# Генерація тексту помилки і відправка з 92 серверу
def check_error_crm(result_data, p_silent_send):
    print(result_data)
    try:
        # Перевіряємо достатність даних
        if not result_data or len(result_data) < 23:
            logger_deltam_checker.warning("Недостатньо даних у result_data для обробки помилки.")
            return

        if result_data[0] == 1:
            (
                error_type, error_text, error_lead, error_contract_num, error_type_report, error_check_type,
                error_id, error_inn, error_dt, error_repeat, repeat_id, error_data,
                par1, par2, par3, par4, par5, client_id, dial_flow_id, work_item_id,
                test_procedure, img
            ) = result_data[1:23]

            repeat_type = check_repeat_type(error_repeat)

            templates = {
                0: template0 if test_procedure is None else template10,
                1: template1, 2: template2, 3: template3, 4: template4, 5: template5,
                6: template6, 7: template7, 8: template7, 9: template9, 11: template11,
            }

            message_template = templates.get(error_type_report, None)
            if not message_template:
                logger_deltam_checker.error(f"❌ Невідомий тип помилки: {error_type_report}")
                return

            message = message_template.format(**locals())

            # Відправка повідомлення конкретним користувачам
            # recipients = {
            #     4: [rovnyi_id, nykodiuk_id],
            #     5: [rovnyi_id, petrenko_id, harchenko_id],
            #     7: [rovnyi_id, petrenko_id, harchenko_id],
            #     8: [rovnyi_id, petrenko_id],
            #     10: [rovnyi_id, nykodiuk_id]
            # }.get(error_type_report, [])
            #
            # for recipient in recipients:
            #     bot.send_message(recipient, message, parse_mode="HTML")

            v_chat_id = get_chat_id(error_check_type)
            for chat_id in v_chat_id:
                print(chat_id)

                # Отримуємо абсолютний шлях до папки, де знаходиться main.py
                #BASE_DIR = os.path.dirname(os.path.abspath(__file__))
                # Якщо шлях відносний – робимо його абсолютним
                #if not os.path.isabs(img):
                #    img_path = os.path.join(BASE_DIR, img)
                #else:
                #    img_path = img  # Якщо шлях вже абсолютний, використовуємо його

                if message_template == template10 and par1 == "file_send":
                    sql_query = par2
                    # print(f"SQL_QUERY: {sql_query}")
                    # print(f"Відправляємо файл: ")
                    send_sql_file(bot, chat_id, sql_query, message)

                else:

                    # Отримуємо абсолютний шлях до директорії скрипта
                    script_dir = os.path.dirname(os.path.abspath(__file__))

                    # Формуємо шлях до зображення

                    image_filename = os.path.join(script_dir, "..", img)  # Піднятися на рівень вище
                    image_filename = os.path.abspath(image_filename)  # Отримати коректний абсолютний шлях

                    #logger_deltam_checker.info(f"img: {img}")  # Дізнаємось, що саме містить змінна
                    #logger_deltam_checker.info(f"image_filename: {image_filename}")  # Дізнаємось, який шлях формується

                    if img:
                        #image_filename = img
                        if os.path.isfile(image_filename):
                            with open(image_filename, "rb") as photo:
                                bot.send_photo(
                                    chat_id, photo, caption=message, parse_mode="HTML",
                                    disable_notification=bool(p_silent_send)
                                )
                        else:
                            err_msg = f"❌ Файл {image_filename} не знайдено! Відправляємо лише текст."
                            logger_deltam_checker.warning(err_msg)
                            send_global_error(err_msg)
                            bot.send_message(chat_id, message, parse_mode="HTML",
                                             disable_notification=bool(p_silent_send))
                    else:
                        #logger_deltam_checker.warning(f"Фото {image_filename}")
                        bot.send_message(chat_id, message, parse_mode="HTML",
                                         disable_notification=bool(p_silent_send))

                update_error_send_status(error_lead, error_id)

            # Відправка фото, якщо файл існує
            # if img:
            #     image_filename = img
            #     if os.path.isfile(image_filename):
            #         with open(image_filename, "rb") as photo:
            #             bot.send_photo(
            #                 group_id, photo, caption=message, parse_mode="HTML",
            #                 disable_notification=bool(p_silent_send)
            #             )
            #     else:
            #         err_msg = f"❌ Файл {image_filename} не знайдено! Відправляємо лише текст."
            #         logger_deltam_checker.warning(err_msg)
            #         bot.send_message(group_id, message, parse_mode="HTML",
            #                          disable_notification=bool(p_silent_send))
            # else:
            #     bot.send_message(group_id, message, parse_mode="HTML",
            #                      disable_notification=bool(p_silent_send))

            # Оновлення статусу відправки


    except IndexError as err:
        logger_deltam_checker.error(f"IndexError у check_error_crm: {err}")
        send_global_error(err)
    except ValueError as err:
        logger_deltam_checker.error(f"ValueError у check_error_crm: {err}")
        send_global_error(err)
    except EnvironmentError as err:
        logger_deltam_checker.error(f"EnvironmentError у check_error_crm: {err}")
        send_global_error(err)
    except Exception as err:
        logger_deltam_checker.error(f"Невідома помилка у check_error_crm: {err}")
        send_global_error(err)


# bot.send_sticker(nykodiuk_id, 'CAACAgIAAxkBAAEK6QdlcHvOrfQRI-XsU2xHhBhSoQ1UnQACIQAD_wzODBFb9FtzRu_LMwQ')
# @bot.message_handler(content_types=['text'])
# def get_text_message(message):
#     if message.text == "test":
#         bot.send_message(message.from_user.id, message.chat.id, parse_mode="HTML")


# if __name__ == "__main__":
#    bot.polling()
