import time
import pickle
import os.path
import gevent


class Memory:
    """
    一个简单的内存类用于封装项目操纵的数据对象
    volatile是未保存的内存变量
    expiring是由keys,arrays和tlls组成的列表
    """
    volatile = {}
    expiring = {} # 带有期限的数据列表

    def __init__(self):
        self.load_state() # 创建时启动载入函数

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.save_state() # 退出时启用保存

    def load_state(self):
        if os.path.exists('dump.rdb'): # 如果存在rdb快照
            state = pickle.load(open('dump.rdb', 'rb')) # 载入快照
            # 分别读取分序列化后相应的值
            self.volatile = state['volatile']
            self.expiring = state['expiring']
            now = time.time() # 以当前时间为判断标准
            expired = set()
            for entry, ttl in self.expiring.items():
                if ttl <= now: # ttl的时间未到
                    expired.add(entry)
                    if entry in self.volatile:
                        del self.volatile[entry]
                else: # tll的时间已到
                    def delete_when_expired(e): # 定义删除函数
                        del memory.volatile[e]
                        del memory.expiring[e]

                    # 通过spawn_later实现：到了ttl时间自动执行删除函数
                    gevent.spawn_later(ttl, delete_when_expired, entry)

            # 删除已经过期了的key
            for expired_key in expired:
                del self.expiring[expired_key]
            #print("dump.rdb loaded into volatile memory")

    # 执行save命令：同步保存操作，将当前redis实例的所有数据快照以rdb的形式保存到硬盘。
    def save_state(self):
        pickle.dump({'volatile': self.volatile, 'expiring': self.expiring}, open('dump.rdb', 'wb'))
        #print("dump.rdb saved to disk")


memory = Memory() # 实例化

