import asyncio


class ServerError(Exception):
    pass


class ServerErrorPut(ServerError):
    pass


class ServerErrorGet(ServerError):
    pass


class ServerErrorCommand(Exception):
    pass


class Parser:
    def parse(data):
        lst = []
        commands = data.split('\n')
        for command in commands:
            if not command:
                continue
            try:
                if command[:3] == 'put':
                    name, value, timestamp = command[4:].split()
                    lst.append(('put', name, float(value), int(timestamp)))
                elif command[:3] == 'get':
                    name = command[4:]
                    lst.append(('get', name))
                else:
                    raise ServerErrorCommand('Ошибка в put или get')
            except ValueError:
                raise ServerError('Получены не те данные')

        return lst


class Worker:
    def __init__(self):
        self.storage = {}

    def run(self, command):
        if command[0] == 'put':
            try:
                if command[1] not in self.storage.keys():
                    self.storage[command[1]] = [f'{command[1]} {command[2]} {command[3]}\n']
                else:
                    for_del = 0
                    for item in self.storage[command[1]]:
                        if item.split()[2] == str(command[3]):
                            for_del = item
                            break
                    if for_del != 0:
                        self.storage[command[1]].remove(for_del)
                    self.storage[command[1]].append(f'{command[1]} {command[2]} {command[3]}\n')
                return 'ok\n\n'
            except:
                raise ServerErrorPut('Отправлен не тот формат значений')
        elif command[0] == 'get':
            if command[1] in self.storage.keys() or command[1] == '*':
                if self.storage != dict():
                    result = "ok\n"
                    if command[1] != '*':
                        for item in self.storage[command[1]]:
                            result += item
                    else:
                        for key in self.storage.keys():
                            for item in self.storage[key]:
                                result += item
                    return result + '\n'
                else:
                    return 'ok\n\n'
            else:
                return 'ok\n\n'


class ClientServerProtocol(asyncio.Protocol):
    worker = Worker()

    def __init__(self):
        self.data = b''

    def connection_made(self, transport):
        self.transport = transport

    def process_data(self, data):
        commands = Parser.parse(data)
        results = []
        for command in commands:
            result = self.worker.run(command)
            results.append(result)
        return (results)

    def data_received(self, data):
        self.data += data
        try:
            decoded = self.data.decode()
        except:
            return

        if not decoded.endswith('\n'):
            return

        self.data = b''
        try:
            resp = self.process_data(decoded)

        except ServerErrorPut:
            self.transport.write(f"error\nОшибка в отправленных метриках\n\n".encode())
            return
        except ServerErrorCommand:
            self.transport.write(f"error\nНеправильно сформулирован запрос put или get\n\n".encode())
            return
        except ServerErrorGet:
            self.transport.write(f"error\nТакого ключа нет\n\n".encode())
        except ServerError:
            self.transport.write(f"error\nНеправильные данные в запросе\n\n".encode())
            return

        resp = ''.join(resp)
        self.transport.write(resp.encode(encoding='UTF-8'))


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(
        ClientServerProtocol,
        host, port
    )
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    run_server('127.0.0.1', 8888)
