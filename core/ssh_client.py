import paramiko
from paramiko import SSHException
from config.config import SSH_CONFIG
from core.logger import logger
import socket

class SSHClient:
    def __init__(self):
        self.client = None

    def connect(self):
        try:
            logger.info(f"Попытка SSH подключения к {SSH_CONFIG['user']}@{SSH_CONFIG['host']}:{SSH_CONFIG['port']}...")
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.client.connect(
                hostname=SSH_CONFIG['host'],
                port=SSH_CONFIG['port'],
                username=SSH_CONFIG['user'],
                password=SSH_CONFIG['password'],
                allow_agent=False,
                look_for_keys=False,
                disabled_algorithms={
                    'pubkeys': ['rsa-sha2-256', 'rsa-sha2-512']
                }
            )
            logger.info(f"SSH подключение к {SSH_CONFIG['host']} успешно установлено.")
        except Exception as e:
            logger.error(f"Ошибка SSH-подключения: {e}")
            raise

    def reconnect(self):
        """Попытка переподключения."""
        logger.warning("SSH сессия не активна. Попытка переподключения...")
        self.close()
        try:
            self.connect()
            logger.info("Переподключение прошло успешно.")
            return True
        except Exception as e:
            logger.error(f"Не удалось переподключиться: {e}")
            return False

    def exec_command(self, command, retries=1, timeout=10):
        if not self.is_connected():
            if not self.reconnect():
                return None
        
        for attempt in range(retries + 1):
            try:
                if not self.client:
                    logger.error("SSH client не инициализирован.")
                    return None
                stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
                output = stdout.read().decode('utf-8')
                error = stderr.read().decode('utf-8')
                if error:
                    logger.warning(f"Ошибка при выполнении '{command}': {error}")
                return output
            except SSHException as e:
                logger.warning(f"Исключение при выполнении команды (попытка {attempt + 1}): {e}")
                if attempt < retries:
                    if not self.reconnect():
                        logger.error("Не удалось переподключиться. Прерываю попытки.")
                        return None
                else:
                    logger.error("Превышено количество попыток переподключения.")
                    raise e
            except EOFError as e:
                logger.warning(f"EOFError при выполнении команды (попытка {attempt + 1}): {e}")
                if attempt < retries:
                    if not self.reconnect():
                        logger.error("Не удалось переподключиться после EOFError. Прерываю попытки.")
                        return None
                else:
                    logger.error("Превышено количество попыток переподключения после EOFError.")
                    return None
            except socket.timeout as e:
                logger.error(f"Таймаут при выполнении команды '{command}': {e}")
                return None
            except Exception as e:
                logger.error(f"Не удалось выполнить команду '{command}': {e}", exc_info=True)
                return None

    def is_connected(self):
        """Проверяет, активно ли SSH соединение."""
        if self.client:
            transport = self.client.get_transport()
            if transport and transport.is_active():
                return True
        return False

    def close(self):
        """Закрывает SSH соединение."""
        if self.client:
            self.client.close()
            logger.info("SSH соединение закрыто.") 