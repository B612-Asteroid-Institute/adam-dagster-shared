import random
from functools import wraps

import requests
from dagster import RetryRequested


def retry_on_preemption(func):
    """
    Decorator to retry a function if the underlying node is preempted.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SystemExit as e:
            print(f"SystemExit: {e}")

            # Only error code 15 represents a preemption.
            # Error code 1 typically represents an OOM error that we don't
            # want to retry.
            if e.code == 15:
                # Wait between 5 minutes to 10 minutes before retrying
                # If we restart immediately we risk getting preempted again.
                seconds_to_wait = random.randint(300, 600)
                raise RetryRequested(
                    max_retries=100, seconds_to_wait=seconds_to_wait
                ) from e

            # As a secondary check, ask Google if we've been preempted.
            # If we've been preempted, the metadata server will return a
            # 200 response with the string "TRUE".
            preempted = requests.get(
                "http://metadata.google.internal/computeMetadata/v1/instance/preempted",
                headers={"Metadata-Flavor": "Google"},
            )
            # print preempted status and error
            print(f"Preempted: {preempted.text}")
            # print response object
            print(f"Response: {preempted}")
            print(preempted)

            if preempted.text == "TRUE":
                # Wait between 5 minutes to 10 minutes before retrying
                # If we restart immediately we risk getting preempted again.
                seconds_to_wait = random.randint(300, 600)
                raise RetryRequested(
                    max_retries=100, seconds_to_wait=seconds_to_wait
                ) from e

            raise

    return wrapper
