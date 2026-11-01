import os
import sys

# Terminals like Git Bash (MINGW64) are not a Windows console, so Python buffers
# stdout and prints nothing until the buffer fills. Flush on every line instead.
sys.stdout.reconfigure(line_buffering=True)

MY_NAME = "roman_mirzoian"

BUILDING_SENSORS_TOPIC = f"{MY_NAME}_building_sensors"
ALERTS_TOPIC = f"{MY_NAME}_streaming_alerts"

ALERTS_CONDITIONS_FILE = "alerts_conditions.csv"

# Connection settings come from environment variables so that no credentials
# are stored in the repository. Defaults point to a local broker.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

kafka_config = {
    "bootstrap_servers": BOOTSTRAP_SERVERS.split(","),
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
}

if os.getenv("KAFKA_SASL_MECHANISM"):
    kafka_config.update({
        "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
        "sasl_plain_username": os.getenv("KAFKA_USERNAME"),
        "sasl_plain_password": os.getenv("KAFKA_PASSWORD"),
    })
