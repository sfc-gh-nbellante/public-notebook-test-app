# ingest_proto_pb.py

import os
import logging
from streamlit.proto.ForwardMsg_pb2 import ForwardMsgList
from streamlit.runtime.scriptrunner_utils.script_run_context import get_script_run_ctx

logging.basicConfig(level=logging.INFO)


def read_binary_protobuf(file_path):
    """
    Read the binary protobuf data from the given file.
    Returns the binary data as bytes.
    """
    with open(file_path, "rb") as file:
        binary_data = file.read()
    return binary_data


def parse_protos_from_binary(binary_data):
    """
    Parse the binary protobuf data into a ForwardMsgList object.
    Returns a list of ForwardMsg objects.
    """
    msg_list = ForwardMsgList()
    msg_list.ParseFromString(binary_data)
    return msg_list.messages


def enqueue_protos(protos):
    """
    Enqueue each ForwardMsg object into the Streamlit app context.
    """
    ctx = get_script_run_ctx()
    for msg in protos:
        ctx.enqueue(msg)


def main():
    """
    Main function to execute the ingestion process.
    """
    # Determine the directory where the current script resides
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to the protos.pb file
    proto_pb_path = os.path.join(script_dir, "protos.pb")

    # Check if protos.pb exists
    if not os.path.isfile(proto_pb_path):
        logging.error(f"Binary protobuf file '{proto_pb_path}' does not exist.")
        return

    # Read the binary protobuf data
    binary_data = read_binary_protobuf(proto_pb_path)

    # Parse the protobuf messages
    protos = parse_protos_from_binary(binary_data)

    # Enqueue the protobuf messages into the Streamlit app
    enqueue_protos(protos)


if __name__ == "__main__":
    main()
