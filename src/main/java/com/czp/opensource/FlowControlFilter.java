package com.czp.opensource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;

/**
 * Function:扩展dubbo增加流控功能,模拟TCP滑动窗口
 * 
 * <pre>
 *   以下任一场景将会将窗口调小SLIDE_RATIO:<br>
 *      1.失败率大于设定的值 FAILD_PERCENT_THRESHOLD
 *              ServerStatus.failCount/requestCount>FAILD_PERCENT_THRESHOLD
 *      2.一段时间内慢响应次数大于SLOW_RESPONSE_THRESHOLD 
 *              ServerStatus.slowReqCount>SLOW_RESPONSE_THRESHOLD
 *    如果一个统计周期内既没有失败也没有慢响应,则将窗口调大SLIDE_RATIO
 * </pre>
 * 
 * @author: coder_czp@126.com
 * @date: 2016年1月8日
 * 
 */
public class FlowControlFilter implements Filter {

	/** 放在同一个文件,方便理解 */
	private static class ServerStatus {

		/** 失败或 慢响应开始统计的时间,用于定时清除计数器 */
		AtomicLong recordStart = new AtomicLong();

		/** 总失败次数 */
		AtomicInteger failCount = new AtomicInteger();

		/** 总请求次数 */
		AtomicInteger requestCount = new AtomicInteger();

		/** 慢响应次数 ,该值会定时清除 */
		AtomicInteger slowRespCount = new AtomicInteger();

		/** 周期内最大请求数,会随后端服务状态调整 */
		AtomicInteger maxRequest = new AtomicInteger(MIN_REQUEST);

		void resetCounter() {
			failCount.set(0);
			requestCount.set(0);
			slowRespCount.set(0);
			maxRequest.set(MIN_REQUEST);
		}
	}

	/** 滑动窗口的最大值 */
	private static final int MAX_REQUEST = 9000;

	/** 滑动窗口的最小值 */
	private static final int MIN_REQUEST = 1000;

	/** 滑动比例,每次增长或减缓按此比例进行 */
	private static final float SLIDE_RATIO = 0.3f;

	/** 清除计数的时间周期,单位:毫秒 */
	private static final int CLEAR_PEROID = 5 * 1000;

	/** 慢响应次数超过该值时下调滑动窗口 */
	private static final int SLOW_RESPONSE_THRESHOLD = 5;

	/** 失败比例阈值,下调滑动窗口 */
	private static final float FAILD_RATIO_THRESHOLD = 0.3f;

	/** 每次响应耗时大于该值时会统计,单位:毫秒 */
	private static final int RECORD_RESPONSE_THRESHOLD = 5 * 1000;

	private static Logger logger = LoggerFactory.getLogger(FlowControlFilter.class);

	/** 服务状态统计，这是线程安全的 */
	private ConcurrentHashMap<String, ServerStatus> statusMap = new ConcurrentHashMap<String, ServerStatus>();

	public Result invoke(Invoker<?> invoker, Invocation invocation)
			throws RpcException {
         
		long curTime = System.currentTimeMillis();

		/* 按IP:PORT统计,支持同一主机起多个服务 */
		String address = invoker.getUrl().getAddress();
		ServerStatus status = statusMap.get(address);
		if (status == null) {
			statusMap.putIfAbsent(address, new ServerStatus());
			status = statusMap.get(address);
		}
		int reqCount = status.requestCount.getAndIncrement();

		long recordStart = status.recordStart.get();
		if (recordStart > 0 && (curTime > recordStart + CLEAR_PEROID)) {
			/* 如果统计数据已过期,则重置计数器 */
			status.resetCounter();
		} else {
			/* 达到当前窗口的极限,中断请求 */
			int maxReq = status.maxRequest.get();
			if (reqCount > maxReq) {
				String info = createRequestInfo(address, invocation);
				logger.info("interrupt:{},maxRequest:{}", info, maxReq);
				throw new RpcException(address.concat(" is busy"));
			}
		}

		Result invoke = invoker.invoke(invocation);
		long times = (System.currentTimeMillis() - curTime);

		if (times > RECORD_RESPONSE_THRESHOLD) {
			status.slowRespCount.getAndIncrement();
			status.recordStart.compareAndSet(0, curTime);
		}
		if (invoke.hasException()) {
			status.failCount.getAndIncrement();
			status.recordStart.compareAndSet(0, curTime);
		}

		modifyMaxRequest(status, reqCount);
		return invoke;
	}

	/**
	 * 根据服务响应状态滑动最大请求窗口
	 * 
	 * @param status
	 * @param reqCount
	 */
	private void modifyMaxRequest(ServerStatus status, int reqCount) {

		int slowCount = status.slowRespCount.get();
		int fialCount = status.failCount.get();

		/* 响应过慢,则将窗口调小 SLIDE_PERCENT */
		if (slowCount > SLOW_RESPONSE_THRESHOLD) {
			decreaseMaxReq(status);
			return;
		}

		/* 失败率过高,则将窗口调小 SLIDE_PERCENT */
		if ((float) fialCount / reqCount > FAILD_RATIO_THRESHOLD) {
			decreaseMaxReq(status);
			return;
		}

		/* 如果没有失败也没有慢响应,则将窗口调大SLIDE_PERCENT */
		if (slowCount == 0 && fialCount == 0) {
			increaseMaxReq(status);
		}

	}

	/**
	 * 将窗口调大
	 * 
	 * @param status
	 */
	private void increaseMaxReq(ServerStatus status) {
		int lastVal = status.maxRequest.get();
		if (lastVal < MAX_REQUEST) {
			int newVal = (int) (lastVal + lastVal * SLIDE_RATIO);
			status.maxRequest.getAndSet(newVal);
			logger.info("increase maxRequest from:{} to:{}", lastVal, newVal);
		}
	}

	/**
	 * 将窗口调小
	 * 
	 * @param status
	 */
	private void decreaseMaxReq(ServerStatus status) {
		int lastVal = status.maxRequest.get();
		if (lastVal > MIN_REQUEST) {
			int newVal = (int) (lastVal - lastVal * SLIDE_RATIO);
			status.maxRequest.getAndSet(newVal);
			logger.info("decrease maxRequest from:{} to:{}", lastVal, newVal);
		}
	}

	/**
	 * 生成日志信息,记录请求的主机.类.方法
	 * 
	 * @param address
	 * @param invocation
	 * @return
	 */
	private String createRequestInfo(String address, Invocation invocation) {
		String method = invocation.getMethodName();
		Class<?> cls = invocation.getInvoker().getInterface();
		return address.concat(".").concat(cls.getName()).concat(".")
				.concat(method);
	}

}
