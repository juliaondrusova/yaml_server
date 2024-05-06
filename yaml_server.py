import socket
import multiprocessing
import logging
import os
import yaml

logging.basicConfig(level=logging.DEBUG)


class BadRequest(Exception):
    pass

class ConnectionClosed(Exception):
    pass

class ErrorResponse(Exception):
    def __init__(self, response):
        self.response = response


class Request:

    def __init__(self, f):

        lines = []
        lenght = 0
        contentLengthFound = False
        enterCounter = 0
        while True:

            if contentLengthFound and enterCounter == 1:
                content = f.read((int)(lenght))
                lines.append(content)
                break
                     
            line = f.readline()
            line = line.decode('utf-8')

            if "Content-length" in line:
                    riadok = line.split(":", 1)
                    if len(riadok) == 2:
                        lenght = riadok[1].strip()
                    contentLengthFound = True

            if line == '':
                if not lines:
                    raise ConnectionClosed
                else:
                    logging.error('Klient zavrel spojenie priskoro')
                    raise ConnectionClosed
            if line == '\n':

                if contentLengthFound:
                    enterCounter+=1
                    continue 
                else:
                    break

            line = line.rstrip()
            logging.debug(f'Client sent {line}')
            lines.append(line)
                
        if not lines:
            raise BadRequest
        
        self.method = lines[0]
        self.content = lines[1:]

        
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
        else:
            f.write('\n'.encode('utf-8'))   
        f.flush()

    def __repr__(self):

        return f'''Response(
            {self.status[0]} {self.status[1]},
            {self.content})'''


class YamlObject(dict):
    def load(self, key, lock):
        if not key:
            raise ErrorResponse(Response(STATUS_BAD_REQUEST))

        filename = f"{key}.yaml"
        file_path = os.path.join('data', filename)

        if not os.path.exists(file_path):
            raise ErrorResponse(Response(STATUS_NO_KEY))

        with lock:
            try:
                with open(file_path, mode='r', encoding = "utf-8") as f:
                    try:
                        dict_data = yaml.safe_load(f)
                    except yaml.error.YAMLError:
                        raise ErrorResponse(Response(STATUS_FORMAT_ERROR))

                    self.clear()
                    self.update(dict_data)
                    return self
            except OSError:
                raise ErrorResponse(Response(STATUS_READ_ERROR))

    def save(self, key, lock):
        if not key:
            raise ErrorResponse(Response(STATUS_BAD_REQUEST))
        filename = f"{key}.yaml"
        file_path = os.path.join('data', filename)
        with lock:
            try:
                with open(file_path, mode='w', encoding="utf-8") as f:
                    yaml.dump(dict(self), f, default_flow_style=False)            
            except IOError:
                logging.error("WRITE ERROR")
                raise ErrorResponse(Response(STATUS_WRITE_ERROR))


STATUS_OK=(100,'OK')
STATUS_READ_ERROR=(201,'Read error')
STATUS_FORMAT_ERROR=(202,'File format error')
STATUS_NO_KEY=(200,'No such key')
STATUS_UKNOWN_METHOD=(203,'Unkown method')
STATUS_NO_FIELD = (204, 'No such field')
STATUS_WRITE_ERROR = (205, 'Write error')
STATUS_YAML_ERROR = (206, 'YAML error')
STATUS_NOT_A_MAPPING = (207, 'Not a mapping')
STATUS_BAD_REQUEST=(300,'Bad request')

        
def method_GET(request, stack, lock):
    if not request.content:
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))

    filename = ""
    fieldname = ""

    for header in request.content:
        key_value = header.split(":", 1)
        if len(key_value) == 2:
            key = key_value[0].strip()
            value = key_value[1].strip()
            if key == "Key":
                if any(c in value for c in (' ', ':', '/')):
                    raise ErrorResponse(Response(STATUS_BAD_REQUEST))
                else:
                    filename = value
            elif key == "Field":
                if ' ' in value:
                    raise ErrorResponse(Response(STATUS_BAD_REQUEST))
                else:
                    fieldname = value
            else:
                raise ErrorResponse(Response(STATUS_BAD_REQUEST))
        else:
            raise ErrorResponse(Response(STATUS_BAD_REQUEST))

    if filename:
        if not os.path.exists('data') or not os.path.isdir('data'):
            raise ErrorResponse(Response(STATUS_BAD_REQUEST))

        yaml_obj = YamlObject()
        dict_data = yaml_obj.load(filename, lock)

        if fieldname in dict_data:
            return Response(STATUS_OK, dict_data[fieldname])
        else:
            raise ErrorResponse(Response(STATUS_NO_FIELD))
    else:
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))
    

def method_KEYS(request,stack,lock):

    with lock:
        if os.path.exists('data') and os.path.isdir('data'):
            yaml_files = [f for f in os.listdir('data') if f.endswith('.yaml')]
            yaml_files_without_extension = [os.path.splitext(f)[0] for f in yaml_files]
            nazov = yaml.dump(yaml_files_without_extension)
            size = len(nazov.encode('utf-8'))
            return Response(STATUS_OK, yaml_files_without_extension)
        else:
            raise ErrorResponse(Response(STATUS_READ_ERROR))
    

def method_FIELD(request,stack,lock):

    if not request.content:
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))

    key_value = request.content[0].split(":", 1)
    if len(key_value) == 2:
        key = key_value[0].strip()
        value = key_value[1].strip()
    else:
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))

    if key != "Key":
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))
    
    if any(c in value for c in (' ', ':', '/')):
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))
    
    filename = value

    if os.path.exists('data') and os.path.isdir('data'):
        yaml_obj = YamlObject()
        data_dict = yaml_obj.load(filename, lock)
        
        return_value = list(data_dict)
        return Response(STATUS_OK, return_value)
    else:
        raise ErrorResponse(Response(STATUS_NO_KEY))


def method_PUT(request, stack, lock):
    if not request.content:
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))

    key_header = None
    field_header = None
    content_length_header = None
    bolObsah = False

    for header in request.content:
        
        if isinstance(header, bytes):
            key_value = header
            bolObsah = True
    
        else:     
            key_value = header.split(":", 1)
        if isinstance(key_value, list) and len(key_value) == 2:
            key = key_value[0]
            value = key_value[1]
            if key == "Key":
                if any(c in value for c in (' ', ':', '/')):
                    raise ErrorResponse(Response(STATUS_BAD_REQUEST))
                else:
                    key_header = value
            elif key == "Field":
                if ' ' in value:
                    raise ErrorResponse(Response(STATUS_BAD_REQUEST))
                else:
                    field_header = value
            elif key == "Content-length":
                try:
                    content_length_header = int(value)
                except ValueError:
                    raise ErrorResponse(Response(STATUS_BAD_REQUEST))
            else:
                raise ErrorResponse(Response(STATUS_BAD_REQUEST))
        else:
            if not key_header or not field_header or not content_length_header: #musia byt vsetky tri hlavicky
                raise ErrorResponse(Response(STATUS_BAD_REQUEST))
           
            try: #kontrola, ci content je validny ako hodnota do yaml file
                obj = yaml.safe_load(header)
            except:
                raise ErrorResponse(Response(STATUS_YAML_ERROR))
            yaml_obj = YamlObject()
            yaml_obj.load(key_header, lock)  #nacitanie suboru
           
            try:
                yaml_obj[field_header] = obj #zmena v zadanom fielde na novu value, ktora prisla ako content
                yaml_obj.save(key_header, lock)
                return Response(STATUS_OK)
            
            except ErrorResponse as e:
                raise ErrorResponse(Response(STATUS_WRITE_ERROR))

    if bolObsah: 
        return Response(STATUS_OK)
    else: #za hlavickami ma byt obsah, inak bad request
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))


def method_POST(request, stack, lock):
    if not request.content:
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))

    key_header = None
    content_length_header = None
    bolObsah = False

    for header in request.content:
        
        if isinstance(header, bytes):
            key_value = header
            bolObsah = True
    
        else:     
            key_value = header.split(":", 1)
        if isinstance(key_value, list) and len(key_value) == 2:
            key = key_value[0]
            value = key_value[1]
            if key == "Key":
                if any(c in value for c in (' ', ':', '/')):
                    raise ErrorResponse(Response(STATUS_BAD_REQUEST))
                else:
                    key_header = value
            elif key == "Content-length":
                try:
                    content_length_header = int(value)
                except ValueError:
                    raise ErrorResponse(Response(STATUS_BAD_REQUEST))
            else:
                raise ErrorResponse(Response(STATUS_BAD_REQUEST))
        else:
            if not key_header or not content_length_header: #musia byt obidve hlavicky
                raise ErrorResponse(Response(STATUS_BAD_REQUEST))
           
            try: #konvertovanie  ak sa to neda, tak zly yaml format
                obj = yaml.safe_load(header) 
            except:
                raise ErrorResponse(Response(STATUS_YAML_ERROR))
            if not isinstance(obj,dict): #ak zkonvertovany obj nema typ dict, tak mapping error
                raise ErrorResponse(Response(STATUS_NOT_A_MAPPING))
            
            filename = f"{key_header}.yaml"
            file_path = os.path.join('data', filename)
            with lock:
                yaml_data = yaml.dump(dict(obj), default_flow_style=False) #konvertovanie na retazec
                try:
                    with open(file_path, mode='w', encoding="utf-8") as f:
                        f.write(yaml_data)
                except IOError:
                    raise ErrorResponse(Response(STATUS_WRITE_ERROR))
            
                except ErrorResponse as e:
                    raise ErrorResponse(Response(STATUS_WRITE_ERROR))

    if bolObsah:
        return Response(STATUS_OK)
    else: #za hlavickami musi byt obsah, inak bad request
        raise ErrorResponse(Response(STATUS_BAD_REQUEST))

METHODS={
    'GET':method_GET,
    'KEYS':method_KEYS,
    'FIELDS':method_FIELD,
    'PUT': method_PUT,
    'POST': method_POST
}

def handle_client(client_socket, addr, lock):
    stack = []
    logging.info(f'handle_client {addr} start')
    f = client_socket.makefile('rwb')
    try:
        while True:
            try:
                req = Request(f)
                logging.info(f'Request: {req.method} {req.content}')
            except BadRequest:
                logging.info(f'Bad request {addr}')
                break
            except ConnectionClosed:
                logging.info(f'Connection closed {addr}')
                break
            try:
                if req.method in METHODS:
                    
                    METHODS[req.method](req, stack, lock).send(f)
                   
                else:
                    raise ErrorResponse(Response(STATUS_UKNOWN_METHOD))
            except ErrorResponse as exc:
                exc.response.send(f)
        logging.info(f'handle_client {addr} stop')
    except KeyboardInterrupt:
        client_socket.close()


s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
s.bind(('',9999))
s.listen(5)
lock=multiprocessing.Lock()

while True:

    cs,addr=s.accept()
    process=multiprocessing.Process(target=handle_client,args=(cs,addr,lock))
    process.daemon=True
    process.start()
    cs.close()