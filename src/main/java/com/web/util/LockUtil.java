package com.web.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * 用于跨系统或者跨服务器之间的锁，不能用于同一个虚拟机
 * @author jacky
 *
 */
public class LockUtil {
	// 声明静态属性
	private static CuratorFramework client = null;
	private static Logger logger = Logger.getLogger(LockUtil.class);

	protected static CountDownLatch latch = new CountDownLatch(1);

	protected static CountDownLatch shardLocklatch = new CountDownLatch(1);


	private static String selfIdentity=null;
	private static String selfNodeName= null;

	public static synchronized void init(String connectString) {
		if (client != null) // client存在就不需要去初始化了
			return;

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		client = CuratorFrameworkFactory.builder().connectString(connectString)
				.sessionTimeoutMs(10000).retryPolicy(retryPolicy)
				.namespace("LockService").build(); // 通过namespace来指定根目录
		client.start();

		// 创建锁目录
		try {
			if (client.checkExists().forPath("/ExclusiveLockDemo") == null) {
				client.create().creatingParentsIfNeeded()
						.withMode(CreateMode.PERSISTENT)
						.withACL(Ids.OPEN_ACL_UNSAFE)
						.forPath("/ExclusiveLockDemo");  // 创建的目录为： /LockService/ExclusiveLockDemo
			}
			// 创建锁监听（排它锁目录下）
			addChildWatcher("/ExclusiveLockDemo");

			if (client.checkExists().forPath("/ShardLockDemo") == null) {
				client.create().creatingParentsIfNeeded()
						.withMode(CreateMode.PERSISTENT)
						.withACL(Ids.OPEN_ACL_UNSAFE).forPath("/ShardLockDemo"); // 创建的目录为： /LockService/ShardLockDemo
			}
		} catch (Exception e) {
			logger.error("ZK服务器连接不上");
			throw new RuntimeException("ZK服务器连接不上");
		}
	}

	/**
	 * 排他锁： 只能允许一个线程获得，其它线程都需要等待已经获取的线程完成才能再次争抢锁资源
	 * zk实现：
	 *   获得锁： 通过构建一个目录，当叶子节点能创建成功，则认为获得锁，因为一旦一个节点被某个会话创建，其它会话再次创建这个节点，将会抛出异常，比如目录
	 *          /execlusiveLock/lock
	 *   释放锁： 删除节点 或者 会话失效
	 *
	 * 获取排他锁的机制方法 getExclusiveLock（）
	 */
	public static synchronized void getExclusiveLock() {
		while (true) {
			try {
				client.create().creatingParentsIfNeeded()
						.withMode(CreateMode.EPHEMERAL)
						.withACL(Ids.OPEN_ACL_UNSAFE)
						.forPath("/ExclusiveLockDemo/lock");
				logger.info("成功获取到锁");
				return;// 如果节点创建成功，即说明获取锁成功
			} catch (Exception e) {
				logger.info("此次获取锁没有成功");
				try {
					//如果没有获取到锁，需要重新设置同步资源值
					if(latch.getCount()<=0){
						latch = new CountDownLatch(1);
					}
					// 进行阻塞
					latch.await();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
					logger.error("", e1);
				}
			}
		}
	}

	/**
	 * 共享锁的场景
	 * – 有些数据，当有线程在读时，只允许别的线程再读，而不允许进行写操作
	 * – 有些数据，当有线程在写时，其它线程都不能再进行操作（包括读和写）
	 * – 比如͹数据库需要某种机制来保障数据的一致性并且尽量兼顾性能
	 *
	   – 定义
	 		• 读锁，如果前面线程使用的是读锁，则后面的线程可以以获取读锁，从而可以继续进行读操作（如果想要获取读锁，则前面线程有读锁，则可以获取，如果前面是写锁，
	          则必须进行等待）
	 		• 写锁，如果在线程打算获取锁从而进行操作时，无论前面已经有读锁或者写锁都必ீ须进入等待（如果想获取写锁，前面线程必须不能有任何锁，包括读锁和写锁，
	         否则进行等待）
	    – Zk实现͹
	 		• 获得读锁͹利用zk节点的顺序性，对于读操作，节点名称带一个R标识，如果前面存在序列数比自己小，并且都是带R标识，则说明前面加的都是读锁，
	          还可以继续获取读锁；否则，等待锁释放后有机会再抢
	 		• 获得写锁͹只有自己创建的节点序列最小，才能获得写锁，否则，进入等待，直到有锁资源被释放，然后再判断是否有机会得到锁
	 		• 释放锁͹删除节点或者会话失效


	 *
	 * @param type
	 *            0为读锁，1为写锁
	 * @param identity
	 *            获取当前锁的所有者（一般可以传入IP地址，作为唯一标识）
	 */
	public static boolean getShardLock(int type, String identity) {
		if (identity == null || "".equals(identity)) {
			throw new RuntimeException("identity不能为空");
		}
		if (identity.indexOf("-") != -1) {
			throw new RuntimeException("identity不能包含字符-");
		}
		if (type != 0 && type != 1) {
			throw new RuntimeException("type只能为0或者1");
		}
		String nodeName = null;
		if (type == 0) {
			nodeName = "R" + identity + "-"; // 读锁标识　如 R192.168.0.1-
		} else if (type == 1) {
			nodeName = "W" + identity + "-"; // 写锁标识
		}
		selfIdentity = nodeName;
		try {
			//if (client.checkExists().forPath("/ShardLockDemo/" + nodeName) == null)
			selfNodeName = client.create().creatingParentsIfNeeded()
					.withMode(CreateMode.EPHEMERAL_SEQUENTIAL) // 创建顺序节点
					.withACL(Ids.OPEN_ACL_UNSAFE)
					.forPath("/ShardLockDemo/" + nodeName);
			logger.info("创建节点:"+selfNodeName);
			List<String> lockChildrens = client.getChildren().forPath("/ShardLockDemo");

			if (!canGetLock(lockChildrens, type, nodeName.substring(0, nodeName.length() - 1),false)) {
				shardLocklatch.await();
			}
			// return;// 获得锁成功就返回
		} catch (Exception e) {
			logger.info("出现异常", e);
			return false;
		}

		logger.info("成功获取锁");
		return true;
	}

	/**
	 * 判断是否能够获取共享锁（读锁或者写锁）
	 * @param childrens
	 * @param type
	 * @param identity
	 * @param reps
     * @return
     */
	private static boolean canGetLock(List<String> childrens, int type, String identity,boolean reps) {
		boolean res = false;
		if(childrens.size()<=0)
			return true;

		try {
			String currentSeq = null;
			List<String> seqs = new ArrayList<String>();
			//List<String> identitys = new ArrayList<String>();
			Map<String,String> seqs_identitys = new HashMap<String,String>();
			for (String child : childrens) {
				String splits[] = child.split("-"); //解析出顺序节点的序号
				seqs.add(splits[1]);
				//identitys.add(splits[0]);
				seqs_identitys.put(splits[1], splits[0]);
				if (identity.equals(splits[0]))
					currentSeq = splits[1];  // 找到当前节点的顺序号
			}

			List<String> sortSeqs = new ArrayList<String>();
			sortSeqs.addAll(seqs);
			// 对节点的顺序号进行排序
			Collections.sort(sortSeqs);

			if (currentSeq.equals(sortSeqs.get(0))) { // 第一个节点就是自己，则无论是读锁还是写锁都可以获取
				res = true;
				logger.info("请求锁,因为是第一个请求锁的请求，所以获取成功");
				return res;
			} else {
				// 写锁
				if (type == 1) {
					res = false;
					//第一次请求取锁则设置监听，以后就不设置了，因为监听一直存在
					if(reps==false)
						addChildWatcher("/ShardLockDemo");
					logger.info("请求写锁，因为前面有其它锁，所以获取锁失败");
					return res;
				}
			}

			// 下面是获取读锁逻辑，需要判断前面有没有写锁
			// int index =-1;
			boolean hasW = true;
			for (String seq : sortSeqs) {
				// ++index;
				if (seq.equals(currentSeq)) {
					break;
				}
				if (!seqs_identitys.get(seq).startsWith("W"))
					hasW = false;
			}
			if (type == 0 && hasW == false) {
				res = true;
			} else if (type == 0 && hasW == true) {
				res = false;
			}
			if (res == false) { // 获取锁失败，添加监听
				// 添加监听
				addChildWatcher("/ShardLockDemo");
				logger.info("因为没有获取到锁，添加锁的监听器");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}

	/**
	 * 释放 排他锁： 通过删除节点
 	 */
	public static boolean unlockForExclusive() {
		try {
			if (client.checkExists().forPath("/ExclusiveLockDemo/lock") != null) {
				client.delete().forPath("/ExclusiveLockDemo/lock");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 释放 共享锁
	 * @return
     */
	public static boolean unlockForShardLock() {
		try {
			if (client.checkExists().forPath(selfNodeName) != null) {
				client.delete().forPath(selfNodeName);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static void addChildWatcher(String path) throws Exception {
		@SuppressWarnings("resource")
		final PathChildrenCache cache = new PathChildrenCache(client, path, true);
		cache.start(StartMode.POST_INITIALIZED_EVENT);// ppt中需要讲StartMode
		// System.out.println(cache.getCurrentData().size());
		cache.getListenable().addListener(new PathChildrenCacheListener() {
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				if (event.getType().equals(
						PathChildrenCacheEvent.Type.INITIALIZED)) {

				} else if (event.getType().equals(
						PathChildrenCacheEvent.Type.CHILD_ADDED)) {

				} else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
					String path = event.getData().getPath();
					System.out.println("收到监听"+path);
					if(path.contains("ExclusiveLockDemo")){
						logger.info("排他锁,收到锁释放通知");
						latch.countDown();
					}else if(path.contains("ShardLockDemo")){
						logger.info("共享锁,收到锁释放通知");
						//收到自己的通知就不处理
						if(path.contains(selfIdentity))
							return;
						List<String> lockChildrens = client.getChildren().forPath("/ShardLockDemo");
						boolean isLock = false;
						try{
							if(selfIdentity.startsWith("R"))
								isLock = canGetLock(lockChildrens,0,selfIdentity.substring(0, selfIdentity.length() - 1),true);
							else if(selfIdentity.startsWith("W"))
								isLock = canGetLock(lockChildrens,1,selfIdentity.substring(0, selfIdentity.length() - 1),true);
						}catch(Exception e){
							e.printStackTrace();
						}
						logger.info("收到锁释放监听后，重新尝试获取锁，结果为:"+isLock);
						if(isLock){
							//获得锁
							logger.info("获得锁，解除因为获取不到锁的阻塞");
							shardLocklatch.countDown();
						}
					}
				} else if (event.getType().equals(
						PathChildrenCacheEvent.Type.CHILD_UPDATED)) {

				}
			}
		});
	}
}
