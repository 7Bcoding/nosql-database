import unittest
from src.command_handler import set_command, get_command, resp_bulk_string, resp_integer, llen_command, lpush_command
from src.memory import memory


class CommandHandlerTests(unittest.TestCase):
    def test_set(self): # 测试set处理函数
        response = set_command("key", ["value"])
        self.assertEqual(memory.volatile["key"], "value")

    def test_get(self): # 测试get处理函数
        set_command("key", ["value"])
        response = get_command("key")
        self.assertEqual(response, resp_bulk_string("value"))

    def test_llen(self): # 测试llen处理函数
        lpush_command("list", ["1", "2", "3"])
        response = llen_command("list")
        self.assertEqual(response, resp_integer(3))
