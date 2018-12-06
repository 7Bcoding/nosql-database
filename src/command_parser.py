"""
解析RESP兼容输入的解析函数
基于 https://redis.io/topics/protocol
"""

def parse_array(array, size):
    """
    通过接受RESP编码对象数组生成RESP兼容数组
    :param array:
    :param size:
    :return:
    """
    arr = []

    for i in range(0, len(array), 2):
        if command_map.get(array[i][0]) is not None:
            arr.append(command_map.get(array[i][0])(array[i:i+2]))
        else:
            print(array[i])

    return arr


def parse_simple_string(array): # 解析简单字符串
    return array[1]


def parse_bulk_string(array): # 解析复杂字符串
    string_byte_len = int(array[0][1:])
    if string_byte_len > 0:
        return array[1]
    else:
        return None


def parse_error(array): # 解析错误
    return array[1]


def parse_int(array):   # 解析整数
    return int(array[1])


# 数据类型映射，根据对应符号选择解析函数
command_map = {
    '*': parse_array,
    '+': parse_simple_string,
    '$': parse_bulk_string,
    '-': parse_error,
    ':': parse_int
}

# 命令解析主函数
def parse_command(str, index):
    """
    每次用户输入命令时调用它，将命令字符串解析为RESP数组，然后传递给命令处理程序。
    :param str: 命令字符串
    :param index:
    :return: command_arr: 命令数组
    """
    items = str.split("\r\n") # 去除CRLF
    items = list(filter(lambda x: x, items)) # 生成列表，返回的是items中为True的值
    array_size = int(items[0][1:])  # 数组大小
    command_arr = parse_array(items[1:], array_size) # 解析数组
    return command_arr
