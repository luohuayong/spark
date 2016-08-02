import ConfigParser

cf = ConfigParser.ConfigParser()
cf.read("app.conf")
db_host = cf.get("database","host")
db_dbname = cf.get("database","dbname")
db_user = cf.get("database","user")
db_pwd = cf.get("database","pwd")

print "db_host = %s" % db_host
print "db_dbname = %s" % db_dbname
