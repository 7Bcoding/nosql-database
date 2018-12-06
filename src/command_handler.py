"""
命令处理程序提供了一个通用的 handle_command，它使用字符串->函数映射来确定适当的命令。
执行它，并返回结果
"""

import gevent
import time
from src.memory import memory


def output_commands():
    """
    返回定义可用命令的字符串。
    :return:
    """
    return resp_array(
            [resp_array(
                [resp_string("\"get\""),
                 resp_string("(integer) 2"),
                 resp_string("1) readonly"),
                 resp_string("(integer) 1"),
                 resp_string("(integer) 1"),
                 resp_string("(integer) 1")]),
             resp_array(
                [resp_string("\"set\""),
                 resp_string("(integer) -3"),
                 resp_string("3) 1) \"write\""),
                 resp_string("   2) \"denyoom\""),
                 resp_string("(integer) 1"),
                 resp_string("(integer) 1"),
                 resp_string("(integer) 1")])
            ])


def set_command(key, args):
    """
    实现对一个key的value简单集合
    :param key: 对集合的键
    :param args: arg[0] 是值
    :return:
    """
    memory.volatile[key] = args[0]
    return resp_bulk_string("OK")


def get_command(key):
    """
    实现从给定key中提取值
    :param key: 想获得值的key
    :return:
    """
    return resp_bulk_string(memory.volatile[key])


def sadd_command(key, args):
    """
    给集合添加value,如果集合不存在则创建改集合
    :param key: 需要添加的集合的key
    :param args: args[0] 是要被添加的值
    :return:
    """
    if memory.volatile.get(key) is None:  # 如果key不存在
        memory.volatile[key] = set(args)    # 创建集合
        return resp_integer(len(memory.volatile[args])) # 返回集合长度
    else:   # key存在
        if isinstance(memory.volatile.get(key), set):  # 集合已存在
            r = len(memory.volatile.get(key).intersection(set(args))) # 返回的r是原集合和输入参数集合的共有元素集合的长度
            memory.volatile[key] = memory.volatile.get(key).union(set(args)) # 将现有集合和输入集合做并集
            if r == 0: # 没有重复元素
                return resp_integer(1)
            else:   # 有重复元素
                return resp_integer(0)
        else:  # key存在，但并不是集合
            return resp_error("KEY {0} IS NOT A SET.".format(args[1]))


def spop_command(key):
    """
    :param args: args[1] is the key, args[2]
    :return:
    """
    memory.volatile[key] = memory.volatile.get(key)

    if isinstance(memory.volatile.get(key), set):
        if len(memory.volatile.get(key)) == 0:
            return resp_error("KEY {0} HAS NO ITEMS".format(key))
        else:
            return resp_string(memory.volatile[key].pop())
    else:
        return resp_error("KEY {0} IS NOT A SET.".format(key))


def sdiff_command(key, args):
    """
    用第一个集合与后面所有的集合做差集
    表达式：args1.diff(args2).diff(args3)...
    :param key: 第一个集合
    :param args: 后面的集合
    :return:
    """
    starting_set = memory.volatile[key]

    for st in args: # 遍历参数中的集合
        if st in memory.volatile: # 如果键为st存在
            if not isinstance(memory.volatile[st], set): # st不是集合
                return resp_error("KEY {0} IS NOT A SET.".format(st))
            else: # st是集合
                starting_set = starting_set.difference(memory.volatile[st])
        else: #键st不存在
            return resp_error("NO SUCH KEY {0} EXISTS".format(st))

    final_set = [] # 创建空的差集
    for item in starting_set:
        final_set.append(resp_string(item))
    return resp_array(final_set) # 返回差集


def sinter_command(key, args):
    """
    :param key: key to start set intersection from
    :param args: list of all the other the other keys to intersect
    :return:
    """
    starting_set = memory.volatile[key]
    if len(args) == 0: # then we have no additional sets to perform intersection with
        return resp_error("SINTER REQUIRES MORE THAN ONE SET TO BE SPECIFIED")
    for st in args:
        if st in memory.volatile:
            if not isinstance(memory.volatile[st], set):
                return resp_error("KEY {0} IS NOT A SET.".format(st))
            else:
                starting_set = starting_set.intersection(memory.volatile[st])
        else:
            return resp_error("NO SUCH KEY {0} EXISTS".format(st))

    final_set = []
    for item in starting_set:
        final_set.append(resp_string(item))
    return resp_array(final_set)


def sunion_command(key, args):
    """
    :param key: key to start set union from
    :param args: list of all the other the other keys to union
    :return:
    """
    starting_set = memory.volatile[key]

    for st in args:
        if st in memory.volatile:
            if not isinstance(memory.volatile[st], set):
                return resp_error("KEY {0} IS NOT A SET.".format(st))
            else:
                starting_set = starting_set.union(memory.volatile[st])
        else:
            return resp_error("NO SUCH KEY {0} EXISTS".format(st))

    final_set = []
    for item in starting_set:
        final_set.append(resp_string(item))
    return resp_array(final_set)


def lpush_command(key, args):
    """
    push元素到列表，如果列表不存在创建该列表
    :param key: 执行push操作的列表
    :param args: 一个或多个元素
    :return:
    """
    if key not in memory.volatile: # key不存在，创建列表
        memory.volatile[key] = []
    memory.volatile[key] = args + memory.volatile[key] # 将values加入到列表
    return resp_integer(len(memory.volatile[key])) # 返回列表的长度


def flush_command():
    memory.volatile = {}
    memory.expiring = {}
    return resp_string("OK")


def save_command():
    memory.save_state()
    return resp_string("OK")


def exists_command(key):
    return resp_integer(1 if key in memory.volatile else 0)


def del_command(key):
    """
    在memory中删除指定key
    :param key: the key to delete
    :return:
    """
    del memory.volatile[key]
    return resp_bulk_string("OK")


def expire_command(key, args):
    """
    赋予指定key存活时间，key到期后会自动执行删除函数
    :param key: 设置期限的key
    :param args: args[0]是设定的期限
    :return:
    """
    def delete_when_expired(k): # 删除方法
        del memory.volatile[k]
        del memory.expiring[k]

    if key in memory.volatile: # key存在
        memory.expiring[key] = time.time() + int(args[0]) # 到期的时间
        gevent.spawn_later(int(args[0]), delete_when_expired, key) # 时间到期时对key执行删除方法
        return resp_bulk_string("OK")
    else: # key不存在
        return no_such_key(args)


def ttl_command(key):
    """
    返回指定key的剩余时间，如果key不存在则返回一个error
    :param key: 需要返回剩余时间的key
    :return:
    """
    if key in memory.expiring: # 如果key存在于expiring中
        return resp_integer(int(memory.expiring[key]-time.time())) # 返回剩余时间
    else: # key不存在于expiring中
        return resp_error("NO KEY MATCHING {0} HAS AN EXPIRATION SET".format(key))


def lpop_command(key):
    """
    :param key: the list to pop an entry off of
    :return:
    """
    l = memory.volatile[key]
    if len(l) > 0:
        return resp_bulk_string(l.pop(0))
    else:
        return resp_error("LIST WITH KEY {0} IS EMPTY.".format(l))


def lindex_command(key, args):
    """
    :param key: key of the list
    :param args: args[0] index in the list to get
    :return:
    """
    l = memory.volatile[key]
    if args[0] is not None:
        i = int(args[0])
        if i > len(l):
            return resp_error("INDEX OUT OF RANGE FOR LIST WITH KEY {0}".format(key))
        return resp_string(l[i])
    else:
        return resp_error("INDEX NOT SPECIFIED FOR LIST WITH KEY {0}".format(key))


def llen_command(key):
    """
    :param key: key of the list
    :return:
    """
    l = memory.volatile[key]
    return resp_integer(len(l))


def hset_command(key, args):
    """
    将哈希表的值设为value，如果key不存在则创建它
    :param key: 哈希表的key
    :param args: value
    :return:
    """
    hm = memory.volatile.get(key, None)  # 检查哈希表是否存在
    if hm is None:  # 哈希表不存在
        memory.volatile[key] = {args[0]: args[1]}  # 创建它并设置第一个键值对
        hm = memory.volatile[key]
        return resp_integer(1)
    elif args[0] in hm: # 域已存在，新值覆盖旧值
        hm[args[0]] = args[1]
        return resp_integer(0)
    else: # 创建了新值域
        hm[args[0]] = args[1]
        return resp_integer(1)


def hget_command(key, args):
    """
    在哈希表中返回给定key的值
    :param key: 哈希表的key
    :param args: 值域
    :return:
    """
    hm = memory.volatile[key] # 得到哈希表
    if args[0] in hm: # 如果值域在哈希表中存在
        return resp_bulk_string(hm[args[0]]) # return it
    else: # 如果值域在哈希表中不存在
        return no_such_key(args) # give an error if the key does not exist


def hmget_command(key, args):
    """
    :param key: this is the hashmap key
    :param args: args[0].. is the key in the hashmap, can be multiple keys
    :return:
    """
    hm = memory.volatile.get(key, None) # get the hashmap from memory
    arr = []
    for val in args: # iterate over the arguments of keys
        if val in hm: # if we find the value
            arr.append( # add it to the array
                resp_bulk_string(
                    hm[val]
                ))
        else:
            arr.append("$-1\r\n")  # else append (nil) as per the redis spec for this command
    return resp_array(arr)


def hmset_command(key, args):
    """
    :param key:
    :param args:
    :return:
    """
    hm = memory.volatile.get(key, None) # check to see if hashmap exists
    if hm is None: # if it does not exist
        memory.volatile[key] = {}  # create it
        hm = memory.volatile[key]

    for i in range(0, len(args), 2): # iterate through this 2 records at a time, to grab key,value
        k = args[i]  # key is the first value
        v = args[i+1]  # value is second
        hset_command(key, [k, v])  # utilize the hset_command to set the key
    return resp_bulk_string("OK")


def hget_all_command(key):
    """
    :param key:
    :return:
    """
    hm = memory.volatile.get(key, None)
    return_arr = []
    for k,v in hm.items():
        return_arr.append(resp_string(k))
        return_arr.append(resp_string(v))
    return resp_array(return_arr)


def not_implemented_command(): # 没有实现此命令
    return resp_string("NOT IMPLEMENTED")


def no_such_key(key): # 不存在此key
    return resp_error("NO SUCH KEY {0} EXISTS".format(key))


"""
command_map 是将命令映射到函数的数据结构。

如果添加一个新命令，可以在这里添加它为它绑定相应的函数。
当进入客户端的命令行时，它将由handle_command来执行。
此实现方式符合'pythonic'原则。
min - 指定参数的最小数目。
max - 指定参数的最大数目。
如果 max 为-1，意味着可以添加任意数量的参数。
如果 max 为-3，意味着可以添加任意数量的参数，但总数必须是奇数。

"""

command_map = {
    "COMMAND": {"min": 0, "max": 0, "function": output_commands},
    "SET": {"min": 2, "max": 2, "function": set_command},
    "GET": {"min": 1, "max": 1, "function": get_command},
    "SADD": {"min": 2, "max": -1, "function": sadd_command},
    "SPOP": {"min": 1, "max": 1, "function": spop_command},
    "SDIFF": {"min": 1, "max": -1, "function": sdiff_command},
    "SINTER": {"min": 2, "max": -1, "function": sinter_command},
    "SUNION": {"min": 2, "max": -1, "function": sunion_command},
    "FLUSH": {"min": 0, "max": 0, "function": flush_command},
    "SAVE": {"min": 0, "max": 0, "function": save_command},
    "EXISTS": {"min": 1, "max": 1, "function": exists_command},
    "EXPIRE": {"min": 2, "max": 2, "function": expire_command},
    "TTL": {"min": 1, "max": 1, "function": ttl_command},
    "DEL": {"min": 1, "max": 1, "function": del_command},
    "LPUSH": {"min": 2, "max": -1, "function": lpush_command},
    "LPOP": {"min": 1, "max": 1, "function": lpop_command},
    "LINDEX": {"min": 2, "max": 2, "function": lindex_command},
    "LLEN": {"min": 1, "max": 1, "function": llen_command},
    "HSET": {"min": 3, "max": 3, "function": hset_command},
    "HGET": {"min": 2, "max": 2, "function": hget_command},
    "HMGET": {"min": 2, "max": -1, "function": hmget_command},
    "HMSET": {"min": 3, "max": -3, "function": hmset_command},
    "HGETALL": {"min": 1, "max": 1, "function": hget_all_command}
}


def list_get(L, i, v=None):
    try: return L[i]
    except IndexError: return v


def handle_command(command_with_args):
    """
    命令处理函数，使用command_map来决定执行哪一个命令
    :param : 带有参数的命令
    :return: RESP规范响应发送到客户端
    """
    command = str(command_with_args[0]).upper() # 将命令统一大写
    if command not in command_map:  # 命令不存在
        return not_implemented_command()
    matched_command = command_map[command] # 根据command_map映射对应命令

    args = command_with_args[2:] or []  # args参数
    key = list_get(command_with_args, 1, None)  # key参数
    total_arg_length = len(args) + (1 if key is not None else 0)  # key和args参数的总长度

    # 检查参数的长度是否符合对应命令的要求
    if total_arg_length < matched_command["min"]: # 输入参数长度小于最小长度
        return resp_error("Not enough arguments for command {0}, minimum {1}".format(command, matched_command["min"]))

    if matched_command["max"] >= 0: # max>=0
        if total_arg_length > matched_command["max"]: # 如果输入参数长度大于最大参数
            return resp_error("Too many arguments for command {0}, maximum {1}".format(command, matched_command["min"]))
    else:   # max<0，意味着可以添加任意数量的参数。
            # max=-1，任意数量的参数，max=-3，总数为奇数的参数
        if matched_command["max"] == -3 and not total_arg_length % 2:   # 如果max=-3且参数总数为偶数
            return resp_error("Not enough arguments or an invalid number of arguments was specified")


    # 执行命令
    if len(args) > 0:
        return command_map[command]["function"](key, args)
    elif key is not None:
        return command_map[command]["function"](key)
    else:
        return command_map[command]["function"]()


"""
以下是生成RESP有效输出的函数
基于 https://redis.io/topics/protocol
"""

def resp_string(val):   # 字符串
    return "+"+val+"\r\n"


def resp_bulk_string(val):  # 复杂字符串
    return "$"+str(len(val))+"\r\n"+val+"\r\n"


def resp_error(val): # 错误
    return "-"+val+"\r\n"


def resp_integer(val): # 整型
    return ":"+str(val)+"\r\n"

def resp_array(arr):  # 数组条目必须是RESP编码的字符串或整型
    val = "*"+str(len(arr))+"\r\n"
    for item in arr:
        if isinstance(val, list):
            val += resp_array(item)
        else:
            val += item
    return val
