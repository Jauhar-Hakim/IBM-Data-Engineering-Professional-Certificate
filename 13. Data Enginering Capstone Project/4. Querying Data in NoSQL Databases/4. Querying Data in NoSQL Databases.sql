wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/nosql/catalog.json

use catalog;

db.createCollection("electronics");

mongoimport -u root -p MTQwNDMtbWlraWtr --authenticationDatabase admin --db catalog --collection electronics --file catalog.json

show dbs;

show collections;

db.electronics.createIndex({ "type": 1 });

db.electronics.find({"type":"laptop"}).count();

db.electronics.find({"type":"smart phone", "screen size": {$eq : 6}}).count();

db.electronics.aggregate([
    {$match: {"type":"smart phone"}},
    {"$group":{
        "average screen size":{"$avg":"$screen size"}
    }}
]);

mongoexport -u root -p MTQwNDMtbWlraWtr --authenticationDatabase admin --db catalog --collection electronics --out electronics.csv --type=csv --fields _id,type,model