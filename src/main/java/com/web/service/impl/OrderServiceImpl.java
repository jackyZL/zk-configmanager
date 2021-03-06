package com.web.service.impl;

import com.web.service.OrderRepository;
import com.web.service.OrderService;
import com.web.service.ProductRepository;
import com.web.service.pojo.Order;
import com.web.service.pojo.Product;
import com.web.util.LockUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;

@Service("orderService")
public class OrderServiceImpl implements OrderService {
	@Autowired
	private OrderRepository orderMapper;

	@Autowired
	private ProductRepository productMapper;

	public OrderRepository getOrderMapper() {
		return orderMapper;
	}

	public void setOrderMapper(OrderRepository orderMapper) {
		this.orderMapper = orderMapper;
	}

	@Transactional
	public boolean doOrder(Order o) {
		LockUtil.init("localhost:2181,localhost:2182");
		LockUtil.getExclusiveLock();
		//获取当前的产品库存数量
		Product nowp = productMapper.selectProductById(o.getProductId());
		if(nowp.getSize()>=o.getPnum()){
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			orderMapper.saveOrder(o);
			HashMap<String,Integer> hm = new HashMap<String,Integer>();
			hm.put("nums", o.getPnum());
			hm.put("id",nowp.getId());
			productMapper.reduceNum(hm);
			System.out.println("库存充足，购买成功");
		}else{
			System.out.println("库存不足，购买失败");
			return false;
		}
		LockUtil.unlockForExclusive();
		return true;
	}




}
