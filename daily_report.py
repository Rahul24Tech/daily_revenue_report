from datetime import datetime, date, timedelta
from calendar import monthrange
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text
from urllib.parse import quote
from sqlalchemy.orm import sessionmaker
import json
import redis
import time
import pytz
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
from decimal import Decimal
import csv
import os


def mailsender(html, msg):
    sender_email = "fec.report.card@gmail.com"
    receiver_email = "tech3@fecdirect.net"
    # receiver_email = "palashp@fecdirect.net"
    password = "rqoeirngqumuwsdt"

    message = MIMEMultipart("alternative")
    message["Subject"] = f"{msg} "
    message["From"] = f"Overview Teamwise Daily Revenue Report <fec.report.card@gmail.com>"
    message["To"] = receiver_email
    text = """Daily Revenue Report """
    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

    # Create secure connection with server and send email
    context1 = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context1) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
    print("Mail sent")


try:
    start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    print(f"Task started at: {str(start_time)}")
    today = start_time.date()
    today_day_number = today.day
    today_week_no = today.weekday()
    today_str = str(today)
    month_days = monthrange(today.year, today.month)[1]
    yesterday = today - timedelta(days=1)
    yesterday_str = str(yesterday)
    yesterday = datetime.strptime(yesterday_str, "%Y-%m-%d")

    # Fetch the day from the datetime object
    day = yesterday.day
    print("day########################", day)

    print("yesterday_str", yesterday_str)
    current_t_isp_list = ('AA_gmail', 'AA_yahoo', 'AA_hotmail', 'AA_aol', 'AM_gmail', 'AM_yahoo', 'AM_hotmail',
                          'AM_aol', 'RR_gmail', 'RR_yahoo', 'RR_hotmail', 'RR_aol', 'FX_gmail', 'FX_yahoo',
                          'FX_hotmail', 'FX_aol', 'BB_gmail', 'BB_yahoo', 'BB_hotmail', 'BB_aol')

    # team_entity_dict = {"AA": "TEAM-AA", "AM": "TEAM-AM", "RR":"TEAM-RR", "FX":"TEAM-FX", "BB":"TEAM-BB"}
    team_entity_list = ["AA", "AM", "RR",  "FX", "BB"]

    where_str_yesterday = f" where timestamp >= '{yesterday}' and timestamp < '{today}' "
    if today_day_number == 1:
        from_mtd = yesterday.replace(day=1)
    else:
        from_mtd = today.replace(day=1)
    where_str_mtd = f" where timestamp >= '{from_mtd}' and timestamp < '{today}' "

    # custom_range = f" where timestamp >= '2023-04-01' and timestamp < '2023-04-23' and offer_idx = '133940'"
    custom_range = f" where timestamp >= '2023-05-13' and timestamp < '2023-05-14' "



    ch_email_db_conx_details = {
        "RR_gmail": {
            "host": "10.160.0.4",
            "port": 13040,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "RR_yahoo": {
            "host": "10.160.0.4",
            "port": 13140,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "RR_aol": {
            "host": "10.160.0.4",
            "port": 13240,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "RR_hotmail": {
            "host": "10.160.0.4",
            "port": 13340,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "AA_gmail": {
            "host": "10.160.0.3",
            "port": 10040,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "AA_yahoo": {
            "host": "10.160.0.3",
            "port": 10140,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "AA_aol": {
            "host": "10.160.0.3",
            "port": 10240,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "AA_hotmail": {
            "host": "10.160.0.3",
            "port": 10340,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "AM_gmail": {
            "host": "10.160.0.8",
            "port": 11040,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "AM_yahoo": {
            "host": "10.160.0.8",
            "port": 11140,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "AM_aol": {
            "host": "10.160.0.8",
            "port": 11240,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "AM_hotmail": {
            "host": "10.160.0.8",
            "port": 11340,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "BB_gmail": {
            "host": "10.160.0.7",
            "port": 12040,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "BB_yahoo": {
            "host": "10.160.0.7",
            "port": 12140,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "BB_aol": {
            "host": "10.160.0.7",
            "port": 12240,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "BB_hotmail": {
            "host": "10.160.0.7",
            "port": 12340,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "FX_gmail": {
            "host": "10.160.0.9",
            "port": 14040,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "FX_yahoo": {
            "host": "10.160.0.9",
            "port": 14140,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "FX_aol": {
            "host": "10.160.0.9",
            "port": 14240,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        },

        "FX_hotmail": {
            "host": "10.160.0.9",
            "port": 14340,
            "db_user": "default",
            "db_passwd": "",
            "db_name": "email"
        }
    }

    ch_email_db_conns = {}
    # Creating connections to ch dbs
    for team_isp, details in ch_email_db_conx_details.items():
        host = details["host"]
        port = details["port"]
        db_user = details["db_user"]
        db_passwd = details["db_passwd"]
        db_name = details["db_name"]
        database_uri = f'clickhouse+native://{db_user}:{quote(db_passwd)}@{host}:{port}/{db_name}'
        team_isp_db_engine = create_engine(database_uri, echo=False, pool_size=2, max_overflow=0, pool_recycle=3600,
                                        pool_pre_ping=True)
        ch_email_db_conns[team_isp] = sessionmaker(team_isp_db_engine)()

    # Checking connection to all ch dbs
    ch_cont_check_query = "SELECT today()"
    for t_ISP in ch_email_db_conns:
        try:
            query_out = ch_email_db_conns[t_ISP].execute(ch_cont_check_query).fetchone()

        except SQLAlchemyError as e:
            ch_email_db_conns[t_ISP].rollback()
            error = f"SQLAlchemyError clickhouse while checking connection to {t_ISP} : {e}"
            raise Exception(error)

        except Exception as e:
            error = f"Exception occurred clickhouse while checking connection to {t_ISP} : {e}"
            raise Exception(error)

    print("Clickhouse connections created successfully")

    # Redis connections
    redis_logger_conx_details = {
        # -------------------------- Gmail ----------------------------------------
        "AA_gmail": {
            "host": "10.160.0.3",
            "db_port": 10000,
            "db_passwd": "jYadjh28y2jvsd0kkasd"
        },
        "AM_gmail": {
            "host": "10.160.0.8",
            "db_port": 11000,
            "db_passwd": "Giuvasd23ebFuSd",
        },
        "BB_gmail": {
            "host": "10.160.0.7",
            "db_port": 12000,
            "db_passwd": "hj7HJas8923vjsfvY"
        },
        "FX_gmail": {
            "host": "10.160.0.9",
            "db_port": 14000,
            "db_passwd": "oiasLop2702Veqaw"
        },
        "RR_gmail": {
            "host": "10.160.0.4",
            "db_port": 13000,
            "db_passwd": "Ituwbj29Hy2nasdlU"
        },
        # -------------------------- Yahoo ----------------------------------------
        "AA_yahoo": {
            "host": "10.160.0.3",
            "db_port": 10100,
            "db_passwd": "opUga84aGFG1"
        },
        "AM_yahoo": {
            "host": "10.160.0.8",
            "db_port": 11100,
            "db_passwd": "SWDJBkjwVSFDUkjd"
        },
        "FX_yahoo": {
            "host": "10.160.0.9",
            "db_port": 14100,
            "db_passwd": "lklkreero0o03ner"
        },
        "RR_yahoo": {
            "host": "10.160.0.4",
            "db_port": 13100,
            "db_passwd": "kUwloopr8001bgr"
        },
        "BB_yahoo": {
            "host": "10.160.0.7",
            "db_port": 12100,
            "db_passwd": "lOuv723gkIpisad"
        },
        # -------------------------- Hotmail ----------------------------------------
        "AA_hotmail": {
            "host": "10.160.0.3",
            "db_port": 10300,
            "db_passwd": "dEDwedoyba10866"
        },
        "AM_hotmail": {
            "host": "10.160.0.8",
            "db_port": 11300,
            "db_passwd": "oyTr7gBhadu82"
        },
        "FX_hotmail": {
            "host": "10.160.0.9",
            "db_port": 14300,
            "db_passwd": "Nget296kasyuba"
        },
        "RR_hotmail": {
            "host": "10.160.0.4",
            "db_port": 13300,
            "db_passwd": "Cedwr2072jdlsasAA"
        },
        "BB_hotmail": {
            "host": "10.160.0.7",
            "db_port": 12300,
            "db_passwd": "litr290BhasdeW"
        },
        # -------------------------- Aol ----------------------------------------
        "AA_aol": {
            "host": "10.160.0.3",
            "db_port": 10200,
            "db_passwd": "mHutg97gG82h"
        },
        "AM_aol": {
            "host": "10.160.0.8",
            "db_port": 11200,
            "db_passwd": "asjGasj3298tsdfkgsfs"
        },
        "FX_aol": {
            "host": "10.160.0.9",
            "db_port": 14200,
            "db_passwd": "lTRe96G23mnq"
        },
        "RR_aol": {
            "host": "10.160.0.4",
            "db_port": 13200,
            "db_passwd": "mkasdYuw96213h"
        },
        "BB_aol": {
            "host": "10.160.0.7",
            "db_port": 12200,
            "db_passwd": "Uigasdgj862Gol"
        }
    }

    redis_logger_conx_dict = {}


    def create_core_redis_connection():
        global redis_logger_conx_dict
        for TEAM_isp in redis_logger_conx_details:
            try:
                r_conn_core = redis.Redis(
                    host=redis_logger_conx_details[TEAM_isp]["host"],
                    port=redis_logger_conx_details[TEAM_isp]["db_port"],
                    password=redis_logger_conx_details[TEAM_isp]["db_passwd"],
                    db=4,
                    socket_timeout=1 * 60 + 5,
                    charset="utf-8",
                    decode_responses=True
                )
                # Checking redis conn
                r_conn_core.set("a", "b")
                redis_logger_conx_dict[TEAM_isp] = r_conn_core

            except Exception as e:
                error = f"Exception while creating redis connection to {TEAM_isp}: {e}"
                # print(error)
                raise Exception(error)


    create_core_redis_connection()
    print("Redis connections created successfully")

    # Checking trigger
    def check_trigger(selected_team_isp):
        trigger_date_str = redis_logger_conx_dict[selected_team_isp].get("revenue_api_trigger")
        if trigger_date_str == str(today):
            return "triggered"
        else:
            print(f"Revenue Api trigger not yet received for {selected_team_isp}")
            return "not_yet_triggered"

    for team_ISP in current_t_isp_list:
        revenue_trigger = check_trigger(team_ISP)
        c = 1
        while revenue_trigger != "triggered" and c < 12:
            c += 1
            time.sleep(300)
            print(f"Revenue Api Trigger({revenue_trigger}) not received for {team_ISP}, checking again")
            revenue_trigger = check_trigger(team_ISP)
        if revenue_trigger != "triggered":
            error = f"Revenue Api Trigger not received for {team_ISP} even after 1 hour "
            error += f"Current trigger value {revenue_trigger}"
            raise Exception(error)

    print("Trigger received")

    temp_query = f"select sum(revenue) from true_user_resp_tbl"
    temp_query+= where_str_yesterday

    mtd_record = f"select sum(revenue) from true_user_resp_tbl"
    mtd_record+= where_str_mtd

    print("temp_query", temp_query)
    print("mtd_query", mtd_record)

    # isp_dict = {"gmail":"gm", "yahoo": "yah", "hotmail": "hot", "aol": "aol"}


    daily_revenue = {}
    mtd_revenue_data = {}
    for team_ISP in current_t_isp_list:
        try:
            custom_data_recd = ch_email_db_conns[team_ISP].execute(temp_query).fetchall()
            mtd_data_record = ch_email_db_conns[team_ISP].execute(mtd_record).fetchall()

            if custom_data_recd:
                c_info_list_of_dict = [row._asdict() for row in custom_data_recd]
                daily_revenue[team_ISP] = c_info_list_of_dict

            if mtd_data_record:
                c__mtd_info_list_of_dict = [row._asdict() for row in mtd_data_record]
                mtd_revenue_data[team_ISP] = c__mtd_info_list_of_dict


        except SQLAlchemyError as e:
            ch_email_db_conns[team_ISP].rollback()
            error = f"SQLAlchemyError while getting yest revenue data for team_isp {team_ISP}: {str(e)}"
            raise Exception(error)

        except Exception as e:
            error = f"Exception while getting yest revenue data for team_isp {team_ISP}: {str(e)}"
            raise Exception(error)

    isp_revenue = {
    'gmail': Decimal('0'),
    'yahoo': Decimal('0'),
    'hotmail': Decimal('0'),
    'aol': Decimal('0')
    }

    mtd_revenue = {
    'gmail': Decimal('0'),
    'yahoo': Decimal('0'),
    'hotmail': Decimal('0'),
    'aol': Decimal('0')
    }

    mtd_team_revenue = {
    'AA': Decimal('0'),
    'AM': Decimal('0'),
    'BB': Decimal('0'),
    'RR': Decimal('0'),
    'FX': Decimal('0')
    }



    # Iterate over the data and calculate the total revenue for each ISP
    for key, value in daily_revenue.items():
        team, isp = key.split('_')
        revenue = value[0]['sum(revenue)']
        isp_revenue[isp] += revenue

    for key, value in mtd_revenue_data.items():
        team, isp = key.split('_')
        revenue = value[0]['sum(revenue)']
        mtd_revenue[isp] += revenue



    for key, value in mtd_revenue_data.items():
        team, isp = key.split('_')
        revenue = value[0]['sum(revenue)']
        mtd_team_revenue[team] += revenue


    # Create the HTML table
    table_html = '<div class="table-responsive">'
    table_html += '<table class="revenue-table">'
    table_html += '<thead><tr><th>ISP</th><th>AA</th><th>AM</th><th>RR</th><th>BB</th><th>FX</th><th>SUM</th><th>MTD</th><th>AVG MTD</th></tr></thead>'
    table_html += '<tbody>'

    mtd_per_team_revenue = {}

    for isp in ['gmail', 'yahoo', 'hotmail', 'aol']:
        table_html += f'<tr><td>{isp}</td>'
        for team in ['AA', 'AM', 'RR', 'BB', 'FX']:
            team_revenue = daily_revenue.get(f'{team}_{isp}', [{'sum(revenue)': Decimal('0')}])[0]['sum(revenue)']

            table_html += f'<td>{team_revenue}</td>'
        table_html += f'<td>{isp_revenue[isp]}</td>'
        table_html += f'<td>{mtd_revenue[isp]}</td>'


        table_html += f'<td>{mtd_revenue[isp]/day:.2f}</td></tr>'




    # Add the Total row after AOL
    table_html += '<tr class="total-row"><td>Total</td>'
    for team in ['AA', 'AM', 'RR', 'BB', 'FX']:
        team_total = sum(daily_revenue.get(f'{team}_{isp}', [{'sum(revenue)': Decimal('0')}])[0]['sum(revenue)'] for isp in ['gmail', 'yahoo', 'hotmail', 'aol'])
        table_html += f'<td>{team_total}</td>'
    table_html += f'<td>{sum(isp_revenue.values())}</td>'
    table_html += f'<td>{sum(mtd_revenue.values())}</td></tr>'
    table_html += '<tr class="mtd-row"><td>MTD</td>'
    for team in ['AA', 'AM', 'RR', 'BB', 'FX']:
        mtd_team_total = mtd_team_revenue.get(team, Decimal('0'))
        table_html += f'<td>{mtd_team_total}</td>'

    table_html+= '</tr>'
    table_html += '<tr class="mtd-row"><td>AVG MTD</td>'
    for team in ['AA', 'AM', 'RR', 'BB', 'FX']:
        table_html += f'<td>{mtd_team_revenue[team]/day:.2f}</td>'
    table_html+= '</tr>'
    table_html += '</tbody></table></div>'

    # CSS code for the table
    css_code = '''
    <style>
    .revenue-table {
        width: 100%;
        border-collapse: collapse;
        border-spacing: 0;
    }
    .revenue-table th,
    .revenue-table td {
        padding: 8px;
        text-align: left;
    }
    .revenue-table thead th {
        background-color: #f2f2f2;
        font-weight: bold;
    }
    .revenue-table tbody td {
        border-bottom: 1px solid #ddd;
    }
    .revenue-table tbody tr:nth-child(even) {
        background-color: #f9f9f9;
    }
    .revenue-table tbody tr:hover {
        background-color: #e9e9e9;
    }
    .table-responsive {
        overflow-x: auto;
        overflow-y: hidden;
        -webkit-overflow-scrolling: touch;
        max-width: 100%;
    }
    @media screen and (max-width: 768px) {
        .revenue-table {
            font-size: 12px;
        }
    }
    </style>
    '''



    report_date = str(datetime.now(pytz.timezone('Asia/Kolkata')).date() - timedelta(days=1))
    msg_part = "Daily Revenue Report " +  report_date
    html = f'{css_code}\n{table_html}'
    html_part = f"""\
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        {css_code}
    </head>
    <body>
        <p>Hi Team,</p>
        <p>Please find daily revenue report attached below</p>

        {table_html}

        <p>Thanks and Regards,</p>
        <p>Tech3</p>
    </body>
    </html>
    """


    mailsender(html_part, msg_part)






except Exception as e:
    error_str = f"Exception while generating report : {str(e)}"
    print(error_str)

    html_part = f"""\
    <!DOCTYPE html>
    <html lang="en">
            <head>
            <meta charset="UTF-8">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            </head>
    <body>
            <h1>Daily Revenue Report Not updated</h1>
            <p>Error: { str(e) }</p>
    </body>
    </html>
    """

    msg_part = ""

    try:
        mailsender(html_part, msg_part)
        pass
    except Exception as e:
        print('error while sending mail :', e)
