package com.lcl.scs.r9333.lpv.po.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.lcl.scs.r9333.lpv.po.model.ArticleDCNodes;


@Repository
public interface LpvArticleNodesRepository extends MongoRepository<ArticleDCNodes, String>{

	ArticleDCNodes findByarticleIdAndDC(String customerItemName,String DC);

}
