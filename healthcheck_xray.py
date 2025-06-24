import socket
import sys
from config.config import ENABLE_PROXY

def check_xray_proxy(proxy_host='127.0.0.1', proxy_port=1080, timeout=2):
    try:
        with socket.create_connection((proxy_host, proxy_port), timeout=timeout):
            return True
    except Exception:
        return False

if __name__ == '__main__':
    if ENABLE_PROXY:
        if check_xray_proxy():
            print('Xray proxy is UP')
        else:
            print('Xray proxy is DOWN')
        sys.exit(0 if check_xray_proxy() else 1) 