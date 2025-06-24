import os
import requests
import logging
from config.config import ENABLE_AI

OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
OPENAI_API_URL = os.getenv('OPENAI_API_URL', 'https://api.openai.com/v1/chat/completions')
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo')
PROXY_URL = os.getenv('PROXY_URL', '')  # Например, socks5h://127.0.0.1:1080
PROXY_TYPE = os.getenv('PROXY_TYPE', 'socks5h')  # socks5h или http

logger = logging.getLogger(__name__)

def send_to_ai_advisor(prompt: str) -> str:
    if not ENABLE_AI:
        return 'AI отключён настройками.'
    if not OPENAI_API_KEY:
        logger.error('OPENAI_API_KEY не задан!')
        return 'AI-интеграция не настроена.'
    headers = {
        'Authorization': f'Bearer {OPENAI_API_KEY}',
        'Content-Type': 'application/json'
    }
    data = {
        'model': OPENAI_MODEL,
        'messages': [
            {'role': 'system', 'content': 'Ты — опытный администратор MySQL. Дай рекомендации по оптимизации и устранению проблем на основе предоставленных метрик.'},
            {'role': 'user', 'content': prompt}
        ],
        'max_tokens': 800,
        'temperature': 0.3
    }
    proxies = None
    if PROXY_URL:
        proxies = {
            'http': f'{PROXY_TYPE}://{PROXY_URL}' if '://' not in PROXY_URL else PROXY_URL,
            'https': f'{PROXY_TYPE}://{PROXY_URL}' if '://' not in PROXY_URL else PROXY_URL
        }
    try:
        resp = requests.post(OPENAI_API_URL, headers=headers, json=data, timeout=30, proxies=proxies)
        resp.raise_for_status()
        result = resp.json()
        return result['choices'][0]['message']['content'].strip()
    except Exception as e:
        logger.error(f'Ошибка при обращении к AI: {e}')
        return f'Ошибка AI: {e}' 