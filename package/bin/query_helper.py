import json
import logging
import requests
from requests.auth import HTTPBasicAuth

import import_declare_test
from solnlib import conf_manager, log
from solnlib.modular_input import checkpointer
from splunklib import modularinput as smi


ADDON_NAME = "solarwinds_ta"

def logger_for_input(input_name: str) -> logging.Logger:
    return log.Logs().get_logger(f"{ADDON_NAME.lower()}_{input_name}")


def get_account_property(session_key: str, account_name: str, property_name: str):
    cfm = conf_manager.ConfManager(
        session_key,
        ADDON_NAME,
        realm=f"__REST_CREDENTIAL__#{ADDON_NAME}#configs/conf-solarwinds_ta_account",
    )
    account_conf_file = cfm.get_conf("solarwinds_ta_account")
    return account_conf_file.get(account_name).get(property_name)


def get_data_from_api(logger: logging.Logger, username, password, url):
    logger.info("Getting data from an external API")
    response = requests.get(
        url,
        auth=HTTPBasicAuth(username, password)
    )

    logger.debug(f"Response status code: {response.status_code}")
    logger.debug(f"Response: {response.json()}")

    return response


def validate_input(definition: smi.ValidationDefinition):
    return


def stream_events(inputs: smi.InputDefinition, event_writer: smi.EventWriter):

    for input_name, input_item in inputs.inputs.items():
        normalized_input_name = input_name.split("/")[-1]
        logger = logger_for_input(normalized_input_name)
        try:
            session_key = inputs.metadata["session_key"]
            kvstore_checkpointer = checkpointer.KVStoreCheckpointer(
                "solarwinds_query_checkpointer",
                session_key,
                ADDON_NAME,
            )
            # checkpointer_key_name = input_name.split("/")[-1]
            log_level = conf_manager.get_log_level(
                logger=logger,
                session_key=session_key,
                app_name=ADDON_NAME,
                conf_name="solarwinds_ta_settings",
            )
            logger.setLevel(log_level)
            log.modular_input_start(logger, normalized_input_name)
            server = get_account_property(session_key, input_item.get("account"), "server")
            port = get_account_property(session_key, input_item.get("account"), "port")
            username = get_account_property(session_key, input_item.get("account"), "username")
            password = get_account_property(session_key, input_item.get("account"), "password")
            query = input_item.get("Query", "")
            checkpoint_field = input_item.get("checkpoint_field", "")


            query_formatted = query.replace(" ", "+")
            request_url = f"{server}/SolarWinds/InformationService/v3/Json/Query?query={query_formatted}"
            logger.debug(f"Query URL: {request_url}")
            response = get_data_from_api(logger, username, password, request_url)
            sourcetype="json"
            event_counter = 0
            if response.status_code == 200:
                data = response.json()['results']
                for event in data:
                    checkpoint = event[checkpoint_field]
                    if kvstore_checkpointer.get(checkpoint) is None:
                        event_writer.write_event(
                            smi.Event(
                                data=json.dumps(event, ensure_ascii=False, default=str),
                                index=input_item.get("index"),
                                sourcetype=sourcetype
                            )
                        )
                        event_counter += 1
                        kvstore_checkpointer.update(checkpoint, 'saved')
                    else:
                        logger.debug(f"Checkpoint already exists for {checkpoint}, skipping event")

            log.events_ingested(
                logger,
                input_name,
                sourcetype,
                event_counter,
                input_item.get("index"),
                account=input_item.get("account"),
            )
            log.modular_input_end(logger, normalized_input_name)
        except Exception as e:
            log.log_exception(logger, e, "Ingestion error", msg_before=f"Exception raised while ingesting data for input: {normalized_input_name}")
