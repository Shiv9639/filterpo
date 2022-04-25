package com.lcl.scs.r9333.lpv.po.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.ArticleDCNodes;
import com.lcl.scs.r9333.lpv.po.repository.LpvArticleNodesRepository;
import com.lcl.scs.r9333.lpv.po.service.LPVArticleNodeService;

@Service
public class  LPVArticleNodeServiceImpl implements LPVArticleNodeService{
	@Autowired
	LpvArticleNodesRepository lpvArticleNodeRepo;
	@Override
	public ArticleDCNodes createNode(ArticleDCNodes articleNode) {
		
		return lpvArticleNodeRepo.insert(articleNode);
	}
	@Override
	public ArticleDCNodes findByarticleIdAndDC(String customerItemName,String DC) {
		
		return lpvArticleNodeRepo.findByarticleIdAndDC(customerItemName,DC);
	}
	
}
