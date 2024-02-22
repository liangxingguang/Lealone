
### Lealone 是什么

* 是一个高性能的面向 OLTP 场景的关系数据库

* 也是一个兼容 MongoDB 的高性能文档数据库

* 同时还高度兼容 MySQL 和 PostgreSQL 的协议和 SQL 语法


### Lealone 有哪些特性

##### 高亮特性

* 并发写性能极其炸裂

* 全链路异步化，使用少量线程就能处理大量并发

* 可暂停的、渐进式的 SQL 引擎

* 基于 SQL 优先级的抢占式调度，慢查询不会长期霸占 CPU

* 创建 JDBC 连接非常快速，占用资源少，不再需要 JDBC 连接池
 
* 插件化存储引擎架构，内置 AOSE 引擎，采用新颖的异步化 B-Tree

* 插件化事务引擎架构，事务处理逻辑与存储分离，内置 AOTE 引擎

* 支持 Page 级别的行列混合存储，对于有很多字段的表，只读少量字段时能大量节约内存

* 支持通过 CREATE SERVICE 语句创建可托管的后端服务

* 只需要一个不到 2M 的 jar 包就能运行，不需要安装


##### 普通特性

* 支持索引、视图、Join、子查询、触发器、自定义函数、Order By、Group By、聚合


##### 云服务版

* 支持高性能分布式事务、支持强一致性复制、支持全局快照隔离

* 支持自动化分片 (Sharding)，用户不需要关心任何分片的规则，没有热点，能够进行范围查询

* 支持混合运行模式，包括4种模式: 嵌入式、Client/Server 模式、复制模式、Sharding 模式

* 支持不停机快速手动或自动转换运行模式: Client/Server 模式 -> 复制模式 -> Sharding 模式


### Lealone 文档

* [快速入门](https://github.com/lealone/Lealone-Docs/blob/master/应用文档/Lealone数据库快速入门.md)

* [文档首页](https://github.com/lealone/Lealone-Docs)


### Lealone 插件

* 兼容 MongoDB、MySQL、PostgreSQL 的插件

* [插件首页](https://github.com/lealone-plugins)


### Lealone 微服务框架

* 非常新颖的基于数据库技术实现的微服务框架，开发分布式微服务应用跟开发单体应用一样简单

* [微服务框架文档](https://github.com/lealone/Lealone-Docs#lealone-%E5%BE%AE%E6%9C%8D%E5%8A%A1%E6%A1%86%E6%9E%B6)


### Lealone ORM 框架

* 超简洁的类型安全的 ORM 框架，不需要配置文件和注解

* [ORM 框架文档](https://github.com/lealone/Lealone-Docs#lealone-orm-%E6%A1%86%E6%9E%B6)


### Lealone 名字的由来

* Lealone 发音 ['li:ləʊn] 这是我新造的英文单词， <br>
  灵感来自于办公桌上那些叫绿萝的室内植物，一直想做个项目以它命名。 <br>
  绿萝的拼音是 lv luo，与 Lealone 英文发音有点相同，<br>
  Lealone 是 lea + lone 的组合，反过来念更有意思哦。:)


### Lealone 历史

* 2012年从 [H2 数据库 ](http://www.h2database.com/html/main.html)的代码开始

* [Lealone 的过去现在将来](https://github.com/codefollower/My-Blog/issues/16)


### [Lealone License](https://github.com/lealone/Lealone/blob/master/LICENSE.md)

