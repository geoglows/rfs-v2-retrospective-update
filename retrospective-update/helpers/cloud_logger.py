import datetime
import os

import requests


class CloudLog:
    """
    Posts logging messages to a given webhook URL to a logging channel
    """
    start_time: str
    url: str
    messages: list

    def __init__(self) -> None:
        self.url = os.getenv('WEBHOOK_LOG_SILENT', '')

    @staticmethod
    def now() -> str:
        return datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")

    def add_message(self, message: str) -> None:
        self.messages.append(f'{self.now()} - {message}')

    def clear_messages(self) -> None:
        self.messages = []

    def flush(self):
        # todo each script should set some metadata about the logging first. which task is posting the message?
        # todo if era5 files are already existing before running the script, raise a warning. Should there be a flag
        #  to clear era5 first?
        message_json = {'text': '\n'.join(self.messages)}
        self.clear_messages()
        print(message_json["text"])  # print so it gets sent to the local log file also

        try:
            response = requests.post(
                self.url,
                headers={"Content-Type": "application/json"},
                json=message_json,
                timeout=10
            )

            if response.status_code != 200:
                print(f"Failed to post to webhook: {response.status_code}, {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Error sending message to webhook: {e}")
