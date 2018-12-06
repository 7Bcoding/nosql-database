import sys, os, time, atexit, signal

class Daemon:
    """
    python3 类实现 unix的守护进程
    Usage: 继承守护进程类并重写run()方法
    """
    def __init__(self, pidfile):
        self.pidfile = pidfile # pidfile是控制进程的文件

    def daemonize(self):

        try:
            pid = os.fork() # 第一次fork，生成子进程，脱离父进程
            # 父进程(会话组头领进程)退出，这意味着一个非会话组头领进程永远不能重新获得控制终端。
            if pid > 0:
                sys.exit(0)  # 父进程退出
        except OSError as err:
            sys.stderr.write('fork #1 failed: {0}\n'.format(err))
            sys.exit(1)

        # 从母体环境脱离
        os.chdir('/') # 修改工作目录
        os.setsid()  # 设置新的会话连接
        os.umask(0)  # 重新设置文件创建权限

        # 执行第二次fork
        try:
            pid = os.fork()  # 第二次fork，禁止进程打开终端
            if pid > 0:
                sys.exit(0) # 第二个父进程退出
        except OSError as err:
            sys.stderr.write('fork #2 failed: {0}\n'.format(err))
            sys.exit(1)

        # 进程已经是守护进程了，重定向标准文件描述符
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(os.devnull, 'r')
        so = open('/tmp/test.txt', 'a+')
        se = open(os.devnull, 'a+')

        # dup2函数原子化关闭和复制文件描述符
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # 注册退出函数
        atexit.register(self.delpid)
        # 根据文件pid判断是否存在进程
        pid = str(os.getpid())
        # 写入pidfile
        with open(self.pidfile, 'w+') as f:
            f.write(pid + '\n')

    def delpid(self): # 退出函数
        os.remove(self.pidfile)

    def start(self):
        '''
        启动守护进程
        '''
        # 首先，检查pid文件是否存在以探测守护进程守护已运行
        try:
            with open(self.pidfile, 'r') as pf:
                pid = int(pf.read().strip())

        except IOError:
            pid = None

        if pid: # pid文件存在，代表守护进程已经在运行
            message = "pidfile {0} already exist. " + \
                      "Daemon already running?\n"
            sys.stderr.write(message.format(self.pidfile))
            sys.exit(1)

        # 守护进程没有运行，启动守护进程
        self.daemonize()
        self.run()

    def stop(self):
        '''
        关闭守护进程
        '''
        # 从pid文件中获取pid
        try:
            with open(self.pidfile, 'r') as pf:
                pid = int(pf.read().strip())
        except IOError:
            pid = None

        if not pid:  # 守护进程没有运行
            message = "pidfile {0} does not exist. " + \
                      "Daemon not running?\n"
            sys.stderr.write(message.format(self.pidfile))
            return  # not an error in a restart

        # 使用kill来关闭进程
        try:
            while 1:
                os.kill(pid, signal.SIGTERM) # 信号
                time.sleep(0.1)
        except OSError as err:
            e = str(err.args)
            if e.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print(str(err.args))
                sys.exit(1)

    def restart(self):
        '''
        重启守护进程
        '''
        self.stop() # 关闭守护进程
        self.start() # 启动守护进程

    def run(self):
        '''
        当你使用子类来继承Daemon，你可以重构这个方法，
        他可以通过start()和restart()方法来成为守护进程。
        '''

