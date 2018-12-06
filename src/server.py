from gevent.server import StreamServer
from src.command_parser import parse_command
import socket
from src.command_handler import handle_command, resp_error


def read_from_client(s, address):
    """
    处理客户端传入命令的函数，将他们发送至命令解析器中解析并最后用命令处理器处理。
    最后，它将输出到客户端
    :param s: 客户端传入的命令
    :param address:
    :return:
    """
    while True:
        try:
            data = s.recvfrom(65536) # 从客户端读取套接字
            if data is not None and data[0] is not None:
                try:
                    command_arr = parse_command(data[0].decode('utf-8'), 0) # 解析命令
                    response = handle_command(command_arr) # 处理命令
                    s.send(bytes(response, 'utf-8')) # 向客户端发送套接字回复
                except socket.error:
                    raise
                except Exception as e:
                    s.send(bytes(resp_error("An unspecified error occurred. {0}".format(str(e))), 'utf-8'))
        except socket.error:
            print(socket.error)
            break
    s.close() # 关闭套接字


def bind_server(ip, port, spawn_limit):
    """
    创建服务
    :return:
    """
    try:
        server = StreamServer((ip, port), read_from_client, spawn=spawn_limit)  # 创建新的服务器
        # server.start()  # 开始接受新的连接
        server.serve_forever() #启动服务器，一直等待，直到终端或服务器停止
    except Exception as e:
        print(str(e))
        server.close() if server is not None and server.started else None
