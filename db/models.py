import pymysql
import pymssql
import telebot
import matplotlib.pyplot as plt
from config import host_delta, user_delta, password_delta, database_delta, telegram_bot, host_dlm, user_dlm, passowrd_dlm, database_dlm, group_id, nykodiuk_id
from logs.logger import logger_deltam_checker
conn = pymysql.connect(host=host_delta, port=3306, user=user_delta, passwd=password_delta, db=database_delta, charset="utf8")
conn_mssql = pymssql.connect(server=host_dlm, user=user_dlm, password=passowrd_dlm, database=database_dlm, charset='cp1251'
                             , autocommit=True)

bot = telebot.TeleBot(telegram_bot)
#bot.send_message(group_id, "Hello FinX", parse_mode="HTML")


arr_message = [
    ["""❗❗❗<b>Помилка</b>❗❗❗
 
🟦  <b>Сервіс:</b> <i>{error_type}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
"""],

    ["""❗❗❗<b>Помилка</b>❗❗❗

🟦  <b>Сервіс:</b> <i>{error_type}</i>

🗃  <b>Тип запису:</b> <i>{repeat_type}</i>

✏  <b>LEAD_ID:</b> <i>{error_lead}</i>

📄  <b>Договір:</b> <i>{error_contract_num}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """],

    ["""❗❗❗<b>Помилка</b>❗❗❗

🟦  <b>Сервіс:</b> <i>{error_type}</i>

🟨  <b>ІПН:</b> <i>{error_inn}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """],

    ["""❗❗❗<b>Помилка</b>❗❗❗

🟦  <b>Сервіс:</b> <i>{error_type}</i>

🟪  <b>Дата і час помилки:</b> <i>{error_dt}</i>

🟨  <b>Лід:</b> <i>{error_lead}</i>

🟥 <b>Текст помилки:</b> <i>{error_text}</i> 
    """],

    ["""❗❗❗<b> УБКІ </b>❗❗❗
            
🟦  <b>Сервіс:</b> <i>{error_type}</i>

🟪  <b>К-ть кредитів:</b> <i>{error_lead}</i>

🟨  <b>К-ть надісланих:</b> <i>{error_inn}</i>

🟥  <b>К-ть з критичною помилкою:</b> <i>{error_contract_num}</i> 
    """]
]


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

def get_month_error_count():
    x = []
    y = []
    getter = conn_mssql.cursor()
    getter_sql = """SELECT 
	CAST(dt_ins AS DATE) as dt
,	COUNT(1) AS cou_error
FROM crm..finx_error_leads_bot 
WHERE CAST(dt_ins AS DATE) >= DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)
GROUP BY CAST(dt_ins AS DATE)
;
"""
    getter.execute(getter_sql)
    res = getter.fetchall()
    #print(res)
    for i in res:
        x.append(i[0])
        y.append(i[1])
    #print(x)
    #print(y)

    plt.plot(x, y, 'r')
    plt.title('Графік помилок', fontsize=10)
    plt.xlabel('Дата', fontsize=10)
    plt.ylabel('К-ть помилок', fontsize=10)
    plt.grid(True)
    plt.savefig('graf.png')
#    plt.legend(loc='best', fontsize=8)
    plt.show()

#get_month_error_count()

def get_checker_time(p_type_id):
    checker = conn_mssql.cursor()
    checker_sql = f"""SELECT 
            CAST(LEFT(start_dt_check,8) AS VARCHAR(100)) as dt_start
        ,	CAST(LEFT(end_dt_check  ,8) AS VARCHAR(100)) as dt_end
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
        res = '🆕'
    else:
        res = '🔁'

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
    upd_sql = f"EXEC crm..alert_deltam_update {p_lead_id}, {p_error_id}"
    upd.execute(upd_sql)
    upd.close()


# Процедура перевірки наявності помилок в БД crm (92 server)
def create_loan_checker_crm(p_type_id):
    checker = conn_mssql.cursor()
    checker_sql = f"EXEC crm..alert_deltam_checker {p_type_id}"
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

        message = arr_message[0][0].format(error_type=error_type, error_text=error_text)
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
        error_repeat = result_data[10]
        repeat_type = check_repeat_type(error_repeat)
        logger_deltam_checker.info(f"Виявлено помилку: {error_text}")

        if error_type_report == 1:
            message = arr_message[1][0].format(error_type=error_type,error_lead=error_lead,
                                               error_contract_num=error_contract_num,error_text=error_text,
                                               repeat_type=repeat_type)

        elif error_type_report == 2:
            message = arr_message[2][0].format(error_type=error_type, error_inn=error_inn, error_text=error_text)

        elif error_type_report == 3:
            message = arr_message[3][0].format(error_type=error_type, error_dt=error_dt, error_lead=error_lead,
                                               error_text=error_text)

        elif error_type_report == 4:
            message = arr_message[4][0].format(error_type=error_type, error_lead=error_lead, error_inn=error_inn,
                                               error_contract_num=error_contract_num)

        bot.send_message(group_id, message, parse_mode="HTML")
        # Оновлення статусу відправки помилки по ліду з таблиці crm..finx_error_leads_bot
        update_error_send_status(error_lead, error_id)

# @bot.message_handler(content_types=['text'])
# def get_text_message(message):
#     if message.text == "test":
#         bot.send_message(message.from_user.id, message.chat.id, parse_mode="HTML")


#if __name__ == "__main__":
#    bot.polling()