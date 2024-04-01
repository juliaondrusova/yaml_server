#!/usr/bin/env python3

import os
import yaml
import socket


def process_get_request(data):
    filename = ""
    fieldname = ""

    for header in data:
        key_value = header.split(":", 1)
        if len(key_value) == 2:
            key = key_value[0].strip()
            value = key_value[1].strip()

            if key == "Key":
                if any(c in value for c in (' ', ':', '/')):
                    return "300 Bad request\n"
                else:
                    filename = value + ".yaml"
            elif key == "Field":
                if ' ' in value:
                    return "300 Bad request\n"
                else:
                    fieldname = value
            else:
                return "300 Bad request\n"
        else:
            return "300 Bad request\n"

    if filename:
        file_found = filename in os.listdir('data')
        if file_found:
            file_path = os.path.join('data', filename)
            try:
                with open(file_path, mode='r') as f:
                    dict = yaml.safe_load(f)
                    yaml_repr = yaml.dump(dict[fieldname])
                    size = len(yaml_repr.encode('utf-8'))
                    if fieldname in dict:
                        response = f"100 OK\nContent-length: {size}\n{yaml_repr}"
                        return response
                    else:
                        return "204 No Such field\n"
            except OSError:
                return "201 Read error\n"
            except yaml.error.YAMLError:
                return "202 File format error\n"
        else:
            return "200 No such key\n"
    else:
        return "200 No such key\n"


def process_keys_request(data):
    yaml_files = [f for f in os.listdir('data') if f.endswith('.yaml')]
    yaml_files_without_extension = [os.path.splitext(f)[0] for f in yaml_files]
    size = len(yaml_files_without_extension.encode('utf-8'))
    response = f"100 OK\nContent-length: {size}\n{yaml_files_without_extension}"
    return response


def process_fields_request(data):
    return


def process_request(data):
    lines = data.splitlines()

    if lines:
        method = lines[0]
        lines = data.splitlines()[1:]
        if method == "GET":
            return process_get_request(lines)
        elif method == "KEYS":
            return process_keys_request(data)
        elif method == "FIELD":
            return process_fields_request(data)
        else:
            return "203 Unkown method\n"

    else:
        return "203 Unkown method\n"


def server_program():
    # Create a TCP/IP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Bind the socket to the address and port
    server_address = ('localhost', 9999)
    server_socket.bind(server_address)

    # Start listening for incoming connections
    server_socket.listen(1)

    while True:
        # Wait for a connection
        print('Waiting for a connection...')
        connection, client_address = server_socket.accept()

        try:
            print('Connection from', client_address)

            # Receive the data in small chunks and retransmit it
            while True:
                data = connection.recv(1024).decode("utf-8")
                if data:
                    response = process_request(data)
                    connection.sendall(response.encode("utf-8"))  # send data to the client
                    connection.close()  # close the connection
                else:
                    print('No more data from', client_address)
                    break

        finally:
            # Clean up the connection
            connection.close()



if __name__ == '__main__':
    server_program()
