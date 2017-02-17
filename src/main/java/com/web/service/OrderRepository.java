package com.web.service;

import com.web.service.pojo.Order;

public interface OrderRepository {
	void saveOrder(Order order);
}
