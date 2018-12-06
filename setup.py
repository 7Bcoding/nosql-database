from setuptools import setup, find_packages
from codecs import open
from os import path, environ

here = path.abspath(path.dirname(__file__)) #

with open(path.join(here, 'README.md'), encoding='utf-8') as f: # 打开README.md文件
    long_description = f.read()

setup(
    name='PyRedis', # 库的名字
    version='1.0.0', # 版本
    description='Redis Python Implementation', # 简介
    long_description=long_description,  # 长文本介绍
    author='ZP',    # 作者
    author_email='zeashan@gmail.com',   # 作者邮箱
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),   # 导入模块
    install_requires=['gevent'],    # 此项目需要的第三方库
    entry_points={
        'console_scripts': [
            'pyredis=src.pyredis:main', # 运行src/pyredis.py/main()
        ],
    }
)