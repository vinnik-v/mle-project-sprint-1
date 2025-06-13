from airflow.providers.telegram.hooks.telegram import TelegramHook
import os
from dotenv import load_dotenv

load_dotenv()

def send_telegram_failure_message(context):
    token = os.getenv('TG_TOKEN_ACCESS')
    chat_id = os.getenv('TG_NOTIFY_CHAT_ID')

    hook = TelegramHook(token=token, chat_id=chat_id)
    dag = context['dag']
    run_id = context['run_id']
    task_key = context['task_instance_key_str']
    exception = context.get('exception', 'Неизвестная ошибка')

    message = (
        f'❌ Ошибка при выполнении DAG `{dag}`\n'
        f'Run ID: `{run_id}`\n'
        f'Task: `{task_key}`\n'
        f'Ошибка: `{exception}`'
    )

    hook.send_message({
        'chat_id': chat_id,
        'text': message
    })

def send_telegram_success_message(context):
    token = os.getenv('TG_TOKEN_ACCESS')
    chat_id = os.getenv('TG_NOTIFY_CHAT_ID')

    hook = TelegramHook(token=token, chat_id=chat_id)
    dag = context['dag']
    run_id = context['run_id']

    message = (
        f'✅ DAG `{dag}` успешно завершён!\n'
        f'Run ID: `{run_id}`'
    )

    hook.send_message({
        'chat_id': chat_id,
        'text': message
    })
