# 雪花算法

*[官方源码地址-scala](https://github.com/twitter-archive/snowflake/tree/snowflake-2010)


## id由以下内容组成：
* 时间 - 41位（毫秒精度w /自定义时代给我们69年）
* 配置的机器ID - 10位 - 最多可为我们提供1024台机器
* 序列号 - 12位 - 每台机器每4096个滚动一次（带有保护以避免在相同的时间内翻转）



