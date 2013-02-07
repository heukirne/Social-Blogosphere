var authorStats = db.posts.aggregate(
		{ $project : { authorID:1, commentSize:1 }}, 
		{ $group :
				{ _id : "$authorID", 
				  posts : {$sum:1}, 
				  comments :{$sum : "$commentSize"} }
				}
		);

var authorTags = db.posts.aggregate(
    { $project : {
	authorID : 1,
	tags : 1,
    }},
    { $unwind : "$tags" },
    { $group : {
	_id : { tags : "$tags", authorID : "$authorID" },
	size : { $sum : 1 },
    }});


var mapTags = function() {
					   idAuthor = this.authorID;
                       this.tags.forEach(function(t){ emit( {authorID: idAuthor, tag: t} , 1); });
                   };

var reduce = function(key, values) { return Array.sum(values); };

db.posts.mapReduce( mapTags, reduce, { replace: "authorTags" });

db.authorTags.find().sort({value:-1}).forEach(function(d){
			a = db.authorStats.findOne({_id:d._id.authorID});
			if (a.tag == null) a.tag = [d._id.tag];
			else a.tag.push(d._id.tag);

			if (a.tag.length > 5) a=a;
			else  db.authorStats.save(a);
		});




SELECT profileID,idade,sexo,tempoBlog,redessocias,autoria,atualizacao,atividade,escolaridade,posts,comment,tags INTO OUTFILE 'answerComp.csv'
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  FROM answers;




  