# Copyright: Stefan Koch, 2019
# License: GPLv3
#
# Library to manage redis connection.
# It simplifies the usage of redis pubsub, does automatic re-connect and wait-to-connect
# see the test file to learn how to use it.
#

import redis
from threading import Thread
import time


class RedisCon():
    version = "1.4"

    def __init__(self, name="not-set", host=None, port=6379, db=0, log_enabled=False, trx_log=False):
        self.name = name
        self.host = host
        self.port = port
        self.db = db
        self.log_enabled = log_enabled
        self.trx_log = trx_log
        self._reset_values()

    def getVersion(self):
        return self.version

    def _reset_values(self):
        self.redis = None
        self.run = True
        self.connected = False
        self.con_thread = None
        self.monitor_interval = 30
        self.discon_cb = None
        self.pubsub = None
        self.log_i = 0
        self.subscriber_list = []

    def setConMonInterval(self, secs):
        self.monitor_interval = secs

    def connect(self):
        if self.connected:
            print("error: redis is already connected")
            return

        self.log("issue connector")
        self._start_connector()
        self.log("wait for connection...")
        while True:
            if self.connected:
                self.log("ok, connected")
                return
            time.sleep(1)

    def subscribeToList(self, sublist):
        self.log("subscribe")
        if self.connected:
            print("error: redis must not be connected")
            return

        if not self.pubsub is None:
            print("error: pubsub is already intialized")
            return

        self.subscriber_list = sublist
        self.log("subscribed to %d keys" % len(self.subscriber_list))

    def get(self, key):
        while True:
            try:
                val = self.redis.get(key)
                self.trxlog("r", key, val)
                return val
            except:
                self.log("get: con abort, wait...")
                self.connected = False
                while self.run:
                    time.sleep(1)
                    if self.connected:
                        self.log("ok, re-connected")
                        break
 
    def set(self, key, value):
        while True:
            try:
                self.redis.set(key, value)
                self.trxlog("w", key, value)
                break
            except:
                self.log("set: con abort, wait...")
                self.connected = False
                while self.run:
                    time.sleep(1)
                    if self.connected:
                        self.log("ok, re-connected")
                        break

    def match(self, pattern):
        while True:
            try:
                return self.redis.scan_iter(pattern)
            except Exception as e:
                self.log("match: con abort, wait...")
                self.connected = False
                while self.run:
                    time.sleep(1)
                    if self.connected:
                        self.log("ok, re-connected")
                        break

    def subscribedChanges(self):
        while not self.connected:
            self.log("subscriber: wait for connection")
            time.sleep(1)

        if self.pubsub is None:
            print("error: no subscriptions intialized")
            return

        while self.run:
            try:
                for msg in self.pubsub.listen():
                    #self.log("subscriber: got data")

                    if msg is None:
                        self.log("subscriber: None")
                        time.sleep(1)
                        continue

                    if msg['type'] != 'pmessage':
                        self.log("subscriber: wrong message type")
                        continue

                    key = self._extract_key(msg['channel'])

                    if key is None:
                        self.log("subscriber: None key")
                        continue

                    try:
                        val = self.get(key)
                        yield key, val
                    except:
                        self.log("cannot read subscribed key")
            except:
                #con abort
                self.log("subscriber: con abort, wait...")
                self.connected = False
                while self.run:
                    time.sleep(1)
                    if self.connected:
                        self.log("ok, re-connected")
                        break
                continue

    def close(self):
        self.log("close redis...")
        self.run = False
        self.log("wait con thread")
        self.con_thread.join()
        self.log("ok, all closed")
        self._reset_values()

    def _extract_key(self, key):
        return key[15:]

    def _start_connector(self):
        self.con_thread = Thread(target=self._connector)
        self.run = True
        self.con_thread.start()

    def _init_subscriptions(self):
        if len(self.subscriber_list) > 0:
            self.pubsub = self.redis.pubsub()
            for item in self.subscriber_list:
                sub = '__keyspace@0__:%s' % item
                self.pubsub.psubscribe(sub)
                self.log("subscribe: %s" % sub)
        else:
            self.log("no init of subscriptions (empty)")
            self.pubsub = None

    def _connector(self):
        self.connected = False
        while self.run:
            #------ TRY Connect ------
            try:
                self.log("connect..")
                self.redis = redis.StrictRedis(host=self.host, port=self.port, db=self.db, decode_responses=True)
                self.redis.ping()
                self._init_subscriptions()
                self.connected = True
                self.log("connected")
            except:
                self.log("connection error")
                time.sleep(1)
                continue

            #------ MON Connection State ------
            while self.run and self.connected:
                try:
                    #self.log("probe con")
                    self.redis.ping()
                    time.sleep(self.monitor_interval)
                except:
                    self.log("lost connection")
                    break

            self.connected = False
            time.sleep(self.monitor_interval)

    def log(self, what):
        if self.log_enabled:
            print("[%3d] rediscon(%s): %s" %(self.log_i, self.name, str(what)), flush=True)
            self.log_i += 1

    def trxlog(self, op, key, val):
        if not self.trx_log:
            return
        if op == "w":
            what = "WRITE"
        else:
            what = "READ "
        print("[%3d] rediscon(%s): %s [%s] = %s" %(self.log_i, self.name, what, str(key), str(val)), flush=True)
        self.log_i += 1

