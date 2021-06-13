# 知识点

1. 优雅关闭: 先阻止新连接进来, 再遍历所有连接进行关闭。

2. Redis 自 2.0 版本起使用了统一的协议 RESP (REdis Serialization Protocol)，该协议易于实现，计算机可以高效的进行解析且易于被人类读懂。
- 二进制安全的文本协议
- 客户端和服务器发送的命令或数据一律以 `\r\n` （CRLF）结尾
- ... 详情见 [Redis 通信协议](https://www.cnblogs.com/Finley/p/11923168.html#redis-%E9%80%9A%E4%BF%A1%E5%8D%8F%E8%AE%AE)


