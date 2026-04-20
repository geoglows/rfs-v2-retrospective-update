import datetime
import os
import socket

import requests

_TS_FMT = "%Y-%m-%d %H:%M:%S"


class CloudLog:
    """
    Accumulate log lines for a single pipeline step and post them as one
    message to the webhook on flush(). Each message gets a header with
    step name, host, pid, start/finish times, and elapsed duration.
    """

    def __init__(self, url: str, step: str) -> None:
        self.url = url
        self.step = step
        self.host = socket.gethostname()
        self.pid = os.getpid()
        self.start_time = datetime.datetime.now()
        self.messages: list[str] = []

    def add_message(self, message: str) -> None:
        self.messages.append(f'{datetime.datetime.now().strftime(_TS_FMT)} - {message}')

    def flush(self) -> None:
        finished = datetime.datetime.now()
        elapsed = finished - self.start_time
        header = (
            f'=== {self.step} ===\n'
            f'host:     {self.host} (pid {self.pid})\n'
            f'started:  {self.start_time.strftime(_TS_FMT)}\n'
            f'finished: {finished.strftime(_TS_FMT)}\n'
            f'elapsed:  {elapsed}'
        )
        body = '\n'.join(self.messages) if self.messages else '(no messages)'
        payload = {'text': f'{header}\n{body}'}
        self.messages = []
        print(payload['text'])

        try:
            response = requests.post(
                self.url,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=10,
            )
            if response.status_code != 200:
                print(f"Failed to post to webhook: {response.status_code}, {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error sending message to webhook: {e}")
