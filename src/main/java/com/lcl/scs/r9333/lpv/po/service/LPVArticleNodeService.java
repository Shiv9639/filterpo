package com.lcl.scs.r9333.lpv.po.service;

import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.ArticleDCNodes;

@Service
public interface LPVArticleNodeService {
	 public ArticleDCNodes createNode(ArticleDCNodes articleNode);
	public ArticleDCNodes findByarticleIdAndDC(String customerItemName, String DC);
}
