import socket
import multiprocessing
import logging
import os
import yaml

class BadRequest(Exception):

    pass

class ConnectionClosed(Exception):

    pass

STATUS_OK=(100,'OK')
STATUS_READ_ERROR=(201,'Read error')
STATUS_FORMAT_ERROR=(202,'File format error')
STATUS_NO_KEY=(200,'No such key')
STATUS_UKNOWN_METHOD=(203,'Unkown method')
STATUS_NO_FIELD = (204, 'No such field')
STATUS_BAD_REQUEST=(300,'Bad request')


logging.basicConfig(level=logging.DEBUG)

class Request:

    def __init__(self,f):
        
        lines=[]
        while True:
            line=f.readline()
            line=line.decode('utf-8')
            if line=='':
                if not lines:
                    raise ConnectionClosed
                else:
                    logging.error('Klient zavrel spojenie priskoro')
                    raise ConnectionClosed
            if line=='\n':
                break
            line=line.rstrip()
            logging.debug(f'Client sent {line}')
            lines.append(line)
        if not lines: # nic neposlal
            raise BadRequest
        self.method=lines[0]
        self.content=lines[1:]
        
class Response:

    def __init__(self,status,content=[]):

        self.status=status
        self.content=content

    def send(self,f):
        f.write(f'{self.status[0]} {self.status[1]}\n'.encode('utf-8'))
        if self.status == STATUS_OK:
            if self.content:
                response_content = yaml.dump(self.content)
                size = len(response_content.encode('utf-8'))
                f.write(f"Content-length: {size}\n".encode('utf-8') + '\n'.encode('utf-8') + response_content.encode('utf-8'))
        else:
            f.write('\n'.encode('utf-8'))   
        f.flush()

    def __repr__(self):

        return f'''Response(
            {self.status[0]} {self.status[1]},
            {self.content})'''
        

def method_GET(request,stack):

    if not request.content:
        return Response(STATUS_BAD_REQUEST)
    
    filename = ""
    fieldname = ""

    for header in request.content:
        key_value = header.split(":", 1)
        if len(key_value) == 2:
            key = key_value[0].strip()
            value = key_value[1].strip()
            if key == "Key":
                if any(c in value for c in (' ', ':', '/')):
                    return Response(STATUS_BAD_REQUEST)
                else:
                    filename = value + ".yaml"
            elif key == "Field":
                if ' ' in value:
                    return Response(STATUS_BAD_REQUEST)
                else:
                    fieldname = value
            else:
                 return Response(STATUS_BAD_REQUEST)
        else:
             return Response(STATUS_BAD_REQUEST)
    if filename:

        if not os.path.exists('data') or not os.path.isdir('data'):
            return Response(STATUS_BAD_REQUEST)

        file_path = os.path.join('data', filename)
    
        try:
            with open(file_path, mode='r') as f:
                try:
                    dict_data = yaml.safe_load(f)
                except yaml.error.YAMLError:
                    return Response(STATUS_FORMAT_ERROR)
                except OSError:
                    return Response(STATUS_READ_ERROR)

                if fieldname in dict_data:
                    return Response(STATUS_OK, dict_data[fieldname])
                else:
                    return Response(STATUS_NO_FIELD)

        except FileNotFoundError:
            return Response(STATUS_NO_KEY)

        except OSError:
            return Response(STATUS_READ_ERROR)
    else:
        return Response(STATUS_BAD_REQUEST)





def method_KEYS(request,stack):

    if os.path.exists('data') and os.path.isdir('data'):
        yaml_files = [f for f in os.listdir('data') if f.endswith('.yaml')]
        yaml_files_without_extension = [os.path.splitext(f)[0] for f in yaml_files]
        nazov = yaml.dump(yaml_files_without_extension)
        size = len(nazov.encode('utf-8'))
        return Response(STATUS_OK, yaml_files_without_extension)
    else:
        return Response(STATUS_READ_ERROR)
    

def method_FIELD(request,stack):

    if not request.content:
        return Response(STATUS_BAD_REQUEST)

    key_value = request.content[0].split(":", 1)
    if len(key_value) == 2:
        key = key_value[0].strip()
        value = key_value[1].strip()
    else:
        return Response(STATUS_BAD_REQUEST)

    if key != "Key":
        return Response(STATUS_BAD_REQUEST)
    
    if any(c in value for c in (' ', ':', '/')):
        return Response(STATUS_BAD_REQUEST)
    
    filename = value + ".yaml"

    if os.path.exists('data') and os.path.isdir('data'):
        file_path = os.path.join('data', filename)
        try:
            with open(file_path) as f:
                data_dict = yaml.safe_load(f)
                return_value = list(data_dict)
                return Response(STATUS_OK, return_value)
        except FileNotFoundError:
            return Response(STATUS_NO_KEY)
        except OSError:
            return Response(STATUS_READ_ERROR)
        except yaml.error.YAMLError:
            return Response(STATUS_FORMAT_ERROR)
        except (KeyError, TypeError):
            return Response(STATUS_NO_FIELD)
    else:
        return Response(STATUS_NO_KEY)



METHODS={
    'GET':method_GET,
    'KEYS':method_KEYS,
    'FIELDS':method_FIELD
}


def handle_client(client_socket,addr):

    stack=[]

    logging.info(f'handle_client {addr} start')
    f=client_socket.makefile('rwb')
    try:
        while True:
            try:
                req=Request(f)
                logging.info(f'Request: {req.method} {req.content}')
            except BadRequest:
                logging.info(f'Bad request {addr}')
                break 
            except ConnectionClosed:
                logging.info(f'Connection closed {addr}')
                break
            if req.method in METHODS:
                response=METHODS[req.method](req,stack)
            else:
                response=Response(STATUS_UKNOWN_METHOD)
            logging.info(f'{response}')
            response.send(f)
           
        logging.info(f'handle_client {addr} stop')
    except KeyboardInterrupt:
        client_socket.close()
    #client_socket.close()

s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('',9999))
s.listen(5)

while True:

    cs,addr=s.accept()
    process=multiprocessing.Process(target=handle_client,args=(cs,addr))
    process.daemon=True
    process.start()
    cs.close()