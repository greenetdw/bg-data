
use hadoop
db.persons.drop()
db.persons.insert({name:"zhangsan",age:15,sex:"M"})
db.persons.insert({name:"lisi",sex:"M"})
db.persons.insert({name:"wangwu",age:18})
db.persons.insert({name:"xiaoer",age:18,sex:"M"})
db.persons.insert({name:"tom",age:17})
db.persons.insert({name:"Helen",age:15,sex:"F"})
db.persons.insert({name:"Judy",age:15,sex:"F"})
db.persons.find().toArray()
