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
                    return "300 Bad request\n\n"
                else:
                    filename = value + ".yaml"
            elif key == "Field":
                if ' ' in value:
                    return "300 Bad request\n\n"
                else:
                    fieldname = value
            else:
                return "300 Bad request\n\n"
        else:
            return "300 Bad request\n\n"
    if filename:
        if os.path.exists('data') and os.path.isdir('data'):
            file_found = filename in os.listdir('data')
            if file_found:
                file_path = os.path.join('data', filename)
                try:
                    with open(file_path, mode='r') as f:
                        dict = yaml.safe_load(f)
                        if fieldname in dict:
                            yaml_repr = yaml.dump(dict[fieldname])
                            size = len(yaml_repr.encode('utf-8'))
                            response = f"100 OK\nContent-length: {size}\n\n{yaml_repr}"
                            return response
                        else:
                            return "204 No Such field\n\n"
                except OSError:
                    return "201 Read error\n"
                except yaml.error.YAMLError:
                    return "202 File format error\n\n"
            else:
                return "200 No such key\n\n"
        else:
            return "300 Bad request\n\n"
    else:
        return "300 Bad request\n\n"


def process_keys_request(data):
    if os.path.exists('data') and os.path.isdir('data'):
        yaml_files = [f for f in os.listdir('data') if f.endswith('.yaml')]
        yaml_files_without_extension = [os.path.splitext(f)[0] for f in yaml_files]
        nazov = yaml.dump(yaml_files_without_extension)
        size = len(nazov.encode('utf-8'))
        response = f"100 OK\nContent-length: {size}\n\n{nazov}"
        return response
    else:
        return "201 Read error\n\n"


def process_fields_request(data):
    filename = ""
    fieldname = ""

    for header in data:
        print("Header " + header)
        key_value = header.split(":", 1)
        print(key_value)
        if len(key_value) == 2:
            key = key_value[0].strip()
            value = key_value[1].strip()
            print("Key "+key+"="+value)
            if key == "Key":
                if any(c in value for c in (' ', ':', '/')):
                    return "300 Bad request\n\n"
                else:
                    filename = value + ".yaml"
            else:
                return "300 Bad request\n\n"
        else:
            return "300 Bad request\n\n"

    if filename:
        file_found = filename in os.listdir('data')
        if file_found:
            file_path = os.path.join('data', filename)
            try:
                with open(file_path, mode='r') as f:
                    dict = yaml.safe_load(f)
                    keys = list(dict)
                    string = yaml.safe_dump(keys)
                    size = len(string.encode('utf-8'))
                    response = f"100 OK\nContent-length: {size}\n\n{string}"
                    return response
            except OSError:
                return "201 Read error\n\n"
            except yaml.error.YAMLError:
                return "202 File format error\n\n"
        else:
            return "200 No such key\n\n"
    else:
        return "200 No such key\n\n"

def process_request(data):
    lines = data.splitlines()
    if lines:
        method = lines[0]
        lines = data.splitlines()[1:]
        if method == "GET":
            return process_get_request(lines)
        elif method == "KEYS":
            return process_keys_request(lines)
        elif method == "FIELDS":
            return process_fields_request(lines)
        else:
            return "203 Unkown method\n\n"

    else:
        return "203 Unkown method\n\n"

def server_program():
    host = "localhost"
    port = 9999  # initiate port no above 1024
    request = ""
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(2)
    conn, address = server_socket.accept()  # accept new connection
    print("Connection from: " + str(address))
    
    step = 0
    request = ""
    while True:
        # receive data stream. it won't accept data packet greater than 1024 bytes
        lines = conn.recv(1024).decode("utf-8")
        print("Lines: "+lines)
        if not lines:
            print("koniec spojenia")
            break
        lines = lines.splitlines()
        if len(lines)==0:
            response = process_request(request)
            conn.sendall(response.encode("utf-8"))  # send data to the client
            step = 0
            request = ""
            continue
    
        for data in lines:
            print("Prijate data: "+data)
        
            #kontrolujeme metodu
            if step == 0:
                if data not in ["GET", "FIELDS", "KEYS"]:
                    response = "203 Unkown method\n\n"
                    conn.sendall(response.encode("utf-8"))
                    step = 0
                    print("Bad method")
                    request = ""
                    break
            #kontrolujeme hlavicku
            else:
                if data == "":
                    print("Request:" + request)
                    response = process_request(request)
                    conn.sendall(response.encode("utf-8"))  # send data to the client
                    step = 0
                    request = ""
                    break
                else:
                    header = data.split(":", 1)
                    if len(header) == 2:
                        key = header[0].strip()
                        if key not in ["Key", "Field"]:
                            response = "300 Bad request\n\n"
                            conn.sendall(response.encode("utf-8"))
                            step = 0
                            break
                    else:
                        response = "300 Bad request\n\n"
                        conn.sendall(response.encode("utf-8"))
                        step = 0
                        break   
            request= request + data + "\n"            
            step=step+1

    conn.close()  # close the connection




if __name__ == '__main__':
    server_program()
