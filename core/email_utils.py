import smtplib
import os
from email.message import EmailMessage
from email.utils import formataddr
from config.config import SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, FROM_ADDR, TO_ADDRS
from core.logger import logger
import socket
import re

def validate_email_fields():
    errors = []
    if not FROM_ADDR or '@' not in FROM_ADDR:
        errors.append('FROM_ADDR не заполнен или некорректен')
    if not TO_ADDRS or not all('@' in addr for addr in TO_ADDRS):
        errors.append('TO_ADDRS не заполнен или содержит некорректные адреса')
    if not SMTP_SERVER:
        errors.append('SMTP_SERVER не заполнен')
    if not SMTP_PORT:
        errors.append('SMTP_PORT не заполнен')
    if not SMTP_USER:
        errors.append('SMTP_USER не заполнен')
    if not SMTP_PASSWORD:
        errors.append('SMTP_PASSWORD не заполнен')
    if errors:
        raise ValueError('Ошибка email-конфигурации: ' + '; '.join(errors))

def send_report_email(subject, body, attachment_path=None, html_body=None):
    validate_email_fields()
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = formataddr(("MySQL Perf Monitor", FROM_ADDR))
    msg['To'] = ', '.join(TO_ADDRS)
    msg.set_content(body)
    if html_body:
        msg.add_alternative(html_body, subtype='html')

    # Добавляем вложение
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, 'rb') as f:
            file_data = f.read()
            file_name = os.path.basename(attachment_path)
        msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

    try:
        logger.info(f"Подключение к SMTP серверу {SMTP_SERVER}:{SMTP_PORT}...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=10) as server:
            logger.info("Установлено соединение с SMTP сервером. Пробую начать TLS...")
            server.starttls()
            logger.info("TLS соединение установлено. Пробую авторизоваться...")
            server.login(SMTP_USER, SMTP_PASSWORD)
            logger.info("Авторизация успешна. Отправляю письмо...")
            server.send_message(msg)
        logger.info(f"Письмо с отчетом отправлено: {attachment_path}")
        print(f"Письмо с отчетом отправлено: {attachment_path}")
    except (smtplib.SMTPException, socket.timeout, Exception) as e:
        logger.error(f"Ошибка при отправке email: {e}")
        print(f"[EMAIL ERROR] {e}")
        raise

def build_html_report_email(date_str):
    return f"""
    <html>
      <body style='font-family: Arial, sans-serif; color: #222;'>
        <h2>Добрый день, Рутем!</h2>
        <p>Во вложении — автоматический отчет о производительности MySQL за <b>{date_str}</b>.</p>
        <ul>
          <li>В отчете содержатся пики нагрузки, топ-5 долгих запросов и рекомендации.</li>
          <li>Если потребуется дополнительная детализация — дайте знать.</li>
        </ul>
        <p style='margin-top:20px;'>С уважением,<br>MySQL Perf Monitor<br><a href='https://github.com/zart227/mysql_perf_monitor'>Проект на GitHub</a></p>
      </body>
    </html>
    """ 