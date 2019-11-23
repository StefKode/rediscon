from rediscon import RedisCon

red = RedisCon(host="dax", log_enabled=True, trx_log=True)
print("RedisCon Version: %s" % red.getVersion())
red.setConMonInterval(5)
red.subscribeToList(["Otto"])
red.connect()

print("Start..")
red.set("Otto", 2)
for key, val in red.subscribedChanges():
    print(key,val)
