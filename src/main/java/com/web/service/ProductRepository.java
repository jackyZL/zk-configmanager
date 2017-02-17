package com.web.service;

import com.web.service.pojo.Product;

import java.util.HashMap;

public interface ProductRepository {
	Product selectProductById(Long id);
	void reduceNum(HashMap<String, Integer> hm);
}
