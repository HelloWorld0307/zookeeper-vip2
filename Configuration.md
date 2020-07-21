配置Zookeeper源码遇到问题：

1），按照给的配置文档配置后发现无法找到zookeeper-server下的git.properties配置项，修改了对应的pom文件中git.propertis文件路径；
2）报错Unable to load jetty, not starting JettyAdminServer，修改了server目录下pom文件中的org.eclipse.jetty配置项的scope为compile即可。（https://blog.csdn.net/weixin_43827951/article/details/90572666）

配置教程：https://www.yuque.com/books/share/9f4576fb-9aa9-4965-abf3-b3a36433faa6/eqefnb