/* Computation of Spearman Correlation for Pagerank, Views, Posts, Comments in MongoDB*/

//Set Collection
tag = "mod";
dbColl = db.getCollection("pageRank_2"+tag); dbColl_ = db.getCollection("pageRank_"+tag);

//Set Global Pagerank, Posts and Comments
db.allAuthor.find({views:{$exists:true}}).forEach(function(a){dbColl.update({_id:a._id},{$set:{v:a.views}})});
dbColl_.find().forEach(function(a){dbColl.update({_id:a._id},{$set:{c:a.value.comment,p:a.value.post}})});
db.pageRank_2all.find().forEach(function(a){dbColl.update({_id:a._id},{$set:{prG:a.value.pr}})});
dbColl.ensureIndex({"value.pr":-1}); dbColl.ensureIndex({v:-1}); dbColl.ensureIndex({c:-1}); dbColl.ensureIndex({p:-1}); dbColl.ensureIndex({prG:-1});

//Compute order for Global Pagerank
cont=1; dbColl.find({v:{$exists:true}}).sort({"value.pr":-1}).forEach(function(a){a.prV=cont++;dbColl.save(a);});
cont=1; dbColl.find({v:{$exists:true}}).sort({v:-1}).forEach(function(a){a.oV=cont++;dbColl.save(a);});
cont=1; dbColl.find({v:{$exists:true}}).sort({prG:-1}).forEach(function(a){a.prgV=cont++;dbColl.save(a);});

//Compute order for Tag Pagerank
cont=1; dbColl.find().sort({prG:-1}).forEach(function(a){a.prgO=cont++;dbColl.save(a);});
cont=1; dbColl.find().sort({"value.pr":-1}).forEach(function(a){a.prO=cont++;dbColl.save(a);});
cont=1; dbColl.find().sort({p:-1}).forEach(function(a){a.pO=cont++;dbColl.save(a);});
cont=1; dbColl.find().sort({c:-1}).forEach(function(a){a.cO=cont++;dbColl.save(a);});

//Compute distance square
sumV=0; sumVg=0; dbColl.find({v:{$exists:true}}).forEach(function(a){sumV+=Math.pow(a.oV-a.prV,2);sumVg+=Math.pow(a.oV-a.prgV,2);});
sumC=0; sumP=0; dbColl.find().forEach(function(a){sumC+=Math.pow(a.cO-a.prO,2);sumP+=Math.pow(a.pO-a.prO,2);});
sumCg=0; sumPg=0; dbColl.find().forEach(function(a){sumCg+=Math.pow(a.cO-a.prgO,2); sumPg+=Math.pow(a.pO-a.prgO,2);});
sumPp=0; dbColl.find().forEach(function(a){sumPp+=Math.pow(a.prO-a.prgO,2);});
countV = dbColl.count({v:{$exists:true}}); count = dbColl.count();

//Compute final Spearman
p=1-((6*sumVg)/(countV*(Math.pow(count,2)-1)));
p=1-((6*sumCg)/(count*(Math.pow(count,2)-1)));
p=1-((6*sumPg)/(count*(Math.pow(count,2)-1)));
p=1-((6*sumV)/(countV*(Math.pow(countV,2)-1)));
p=1-((6*sumC)/(count*(Math.pow(count,2)-1)));
p=1-((6*sumP)/(count*(Math.pow(count,2)-1)));
p=1-((6*sumPp)/(count*(Math.pow(count,2)-1)));

print('Done');