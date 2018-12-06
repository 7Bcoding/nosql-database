import unittest
from src.command_parser import parse_command


class CommandParserTests(unittest.TestCase):
    def test_parse_commandt(self): # 测试COMMAND命令
        response = parse_command("*1\r\n$7\r\nCOMMAND\r\n", 0)
        self.assertEqual(response[0], "COMMAND")

    def test_parse_set_command(self): # 测试解析set命令
        response = parse_command("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", 0)
        self.assertEqual(response[0], "SET")
        self.assertEqual(response[1], "key")
        self.assertEqual(response[2], "value")