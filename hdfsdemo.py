# -*-coding:utf8-*-
# "__author__" = "Black Hawk"
# @Time : 2018/1/21 17:12
# @Software: PyCharm
from hdfs3 import HDFileSystem

host = '192.168.85.133'  # 设置IP （因人而异）
port = 9000  # 设置端口


def hdfs_exists(hdfsClient):  # 编写判断hdfs中是否存在文件的方法
    path = '/test/hdfs'
    if hdfsClient.exists(path):  # 判断文件是否存在path路径
        hdfsClient.rm(path)
    hdfsClient.makedirs(path)


def hdfs_write_read(hdfsClient):  # 编写判断hdfs读写方法
    data = b'what do you want to do?' * 20  # 将测试语句转换为二进制
    file_a = '/test/hdfs/file_a'
    with hdfsClient.open(file_a, 'wb', replication=1) as f:  # 写入data文件。并设置副本数为1
        f.write(data)

    with hdfsClient.open(file_a, 'rb')as f:  # 读file_a文件
        out = f.read(len(data))
        assert out == data  # 断言 out=data 如果不符则会报错


def hdfs_readlines(hdfsClient):  # 编写判断hdfs按行读方法
    file_b = '/test/hdfs/file_b'
    with hdfsClient.open(file_b, 'wb', replication=1) as f:
        f.write(b'hello\nworld!')
    with hdfsClient.open(file_b, 'rb')as f:
        lines = f.readlines()
        assert len(lines) == 2


if __name__ == '__main__':
    hdfsClient = HDFileSystem(host=host, port=port)  # 连接上HDFS系统

    hdfs_exists(hdfsClient)

    hdfs_write_read(hdfsClient)

    hdfs_readlines(hdfsClient)

    hdfsClient.disconnect()  # 断开连接
    print '-' * 20
    print 'Perform the end'
