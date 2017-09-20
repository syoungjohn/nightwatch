# nightwatch简介
证券异常交易行为监控系统。九州证券系统开发部自主设计、研发的证券异常交易行为监控系统，采用Storm+Esper架构实现，支持金证交易系统的指令拦截 、复制与解析。

# 版权说明
> 简单陈述项目的开源协议，以及使用该项目的开源原则

# Overview
> 介绍异常交易行为监控系统的技术架构\<h3\><br />
异常交易行为监控系统主要分为交易指令处理层、事前风控子系统、事中风控子系统、事后风控子系统、风控的web子系统。异常交易行为监控系统直接拦截和旁路集中交易系统的用户交易指令，并交给不同的风控子系统处理。<br />
![Alt text](https://github.com/JiuzhouSec/nightwatch/raw/master/Screenshots/jg.png)
1.交易指令处理层\<h3\><br />
交易指令处理层主要是衔接集中交易系统和下游事前风控子系统与事中风控子系统的桥梁。对于事前风控子系统，交易指令处理层将集中交易系统中拦截到的交易指令发送给事前风控子系统，并将事前风控子系统的对用户本次交易行为的处理结果返回给集中交易系统的应答队列。对于事中风控之系统，交易指令处理层将从集中交易系统旁路出来的用户交易指令发送给事中风控分析接入层，事中风控子系统接到用户的交易指令后对用户的交易行为进行风控模型的计算。<br />
集中交易系统指令旁路及拦截拓扑图：\<h3\><br />
![Alt text](https://github.com/JiuzhouSec/nightwatch/raw/master/Screenshots/jytp.png)
消息队列	说明<br />
req_acc	应用请求消息队列<br />
resp_acc	应用应答消息队列<br />
req_fk	事前风控过滤后的请求消息队列<br />
resp_fk	应答消息队列<br />
req_replica	应用请求消息的复制队列<br />
2. 事前风控子系统（目前处于未上线状态）\<h3\><br />
事前风控可对交易指令先进行风险监控，通过监控的交易指令才提交给交易系统进行处理，未通过监控的交易指令将直接予以拒绝。事前风控要求处理时间很短（10ms以内）。业务应用方面，事前风控一般可用于对存在较高风险的特定接入渠道和特定投资者，在满足合规要求的前提下进行风险监控。
事前风控流程图：<br />
![Alt text](https://github.com/JiuzhouSec/nightwatch/raw/master/Screenshots/1.png)
3. 事中风控子系统<h3/><br />
事中风控是指交易指令在提交给交易系统进行处理的同时，旁路同样的指令到事中风控子系统进行分析处理；当触发事中风控规则后，系统自动进行报警，由人工进行处置。事中风控一般需要在短时间内（50ms以内）对交易数据做出分析结果。<br />
事中风控流程图：\<h3\><br />
![Alt text](https://github.com/JiuzhouSec/nightwatch/raw/master/Screenshots/2.png)
事中风控架构图：\<h3\><br />
![Alt text](https://github.com/JiuzhouSec/nightwatch/raw/master/Screenshots/3.png)

# 风控引擎
> 介绍Storm和Esper\<h3\><br />
storm介绍\<h3\><br />
Storm是一个免费并开源的分布式实时计算系统。利用Storm可以很容易做到可靠地处理无限的数据流，像Hadoop批量处理大数据一样，Storm可以实时处理数据。
在异常交易行为监控系统里，storm主要用在事中风控子系统里，负责人物的管理及分配。<br />
storm架构图：\<h3\><br />
![Alt text](https://github.com/JiuzhouSec/nightwatch/raw/master/Screenshots/storm.jpg)

Esper介绍\<h3\><br />
Esper是一种cep（复杂事件处理）引擎。从成熟度以及规则语法上手速度，我们选择了业界广泛使用的且有着类SQL语法的EsperTech Esper(规则用EPL语句表达)作为CEP开发引擎。其他的开源CEP引擎还有JBoss Drools Fusion/Siddhi等。<br />
数据窗口机制完善，Esper目前支持大约30种数据窗口，下面表格列出常用的几种：<br />
![Alt text](https://github.com/JiuzhouSec/nightwatch/raw/master/Screenshots/4.png)
EPL语句的语法与SQL相似，事件处理语言（EPL）是SQL标准语言并做了扩展，提供了SELECT、 FROM、 WHERE、 GROUP BY、HAVING和 ORDER BY等子句。<br />



# 风控模型
> 介绍开源的风控规则以及对应的法律法规


# 如何安装
> 介绍如何从源码编译、安装各个组件

# 如何使用
> 介绍如何使用异常交易行为监控系统的Web管理平台，以及如何编写EPL来自定义风控规则

# 联系我们
opensource@jzsec.com
