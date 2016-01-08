# dubbo扩展-流控
##使用:
	<dubbo:reference filter="flowControlFilter" /> <!-- 消费方调用过程拦截 -->
	<dubbo:consumer filter="flowControlFilter"/> <!-- 消费方调用过程缺省拦截器 -->
	<dubbo:service filter="flowControlFilter" /> <!-- 提供方调用过程拦截 -->
	<dubbo:provider filter="flowControlFilter"/> <!-- 提供方调用过程缺省拦截器 -->
