# !/usr/bin/env python
# --coding:utf-8--
#我的蘑菇邀请码-谢谢支持 http://www.mogumiao.com?invitationCode=5B245DC3-5231-4955-B1DA-7715137C35DF
# 目前 只是 简单能运行 ，还没优化 - 我的使用方法是 ：将ippool控制 和ip调用分离 ，所以 还需要 一个 ip调用接口
# 其实我写的这个更像 一个 夹在 项目和 ip代理供应Api的缓冲层，粗略 计算 是 14(h) * 30(次) * 3(个ip) 一天不到2000个ip够用
import requests
import psycopg2
import time
import uuid
from fake_useragent import UserAgent
import threading
# 异步库
import aiohttp
import asyncio

param = {
    "url":"自我测试的url网址 可以考虑将这一环去掉，不过为了稳定性还是可以靠虑的",
    "conn": None,
    "cur":None,
    "ipGetUrl": 'ipAgent来源',
    "hasConnected":False,
    "lens":0
}


header = {
    "HOST": param["url"],
    "Referer": param["url"],  # ???
    "User-Agent":"",
}



ua = UserAgent()
# re 代表 上一次刷新 没有获取有效ip，马上重新获取-可以做一些保护机制 否则 ，ip接口一旦失效，这里仍不断获取 ～
def testIpConnected(re):
    param["begin"]= time.time()
    if re :
        pass
    else:
        # 我用的数据库 你也可以写成 server，放在server的内存里,主要麻烦的就是 异步 + 锁,让数据库替我先解决吧～
        param["conn"] = psycopg2.connect(database='scrapy', user='mlcb', password='123456',
                                         host='localhost', port='5432')
    param["lens"] = 0
    param["hasConnected"] = False
    print("begin Reflash")
    # 这个是 请求头 和ip代理一起返回 ，可以 共同 保护不被发现
    param["User-Agent"] = ua.random
    # 根据
    h = time.strftime("%H:%M:%S", time.localtime())
    if h >= "08:59:00" and h < "23:00:00":
        param["ipGetUrl"] = '工作时间 多维护几个ip'
    else:
        param["ipGetUrl"] = '休息时间 少维护几个'

    # 做一些准备
    param["cur"] = param["conn"].cursor()
    # 这里本来还有一层 ，因为 目前的ip固定5min失效 对于 > 5 但不固定失效时间，可以在这里再加一层缓存 检查不必要每次清空
    #   selectSql = 'select ip,port from ippool order by random()'
    #   param["cur"].execute(selectSql)
    #   rows = param["cur"].fetchall()
    #   在检测期间 先置所有 ip 为 no-Actived - 也就是说 会有那么几秒的 无法联通 ，
    #       可以考虑优化，就是 取一半 进行检测神么的 然后将 检测频率加倍
    #   做 ip数量检测 太少了 先去 取一点
    #   if len(rows) < 3:
    try:
        newIps = requests.get(param["ipGetUrl"])
    except  Exception  as e:
            #获取newIP失败
        pass
    else:
            # 可能 获取失败 对失败情况的处理...
            # ...
        if newIps.status_code != 200:
            # get it over again and Alert Woring！
            pass
            # .获取成功. json
        else:
            newIps = list(eval(newIps.text)["msg"])
            print("newIps: " + str(newIps))
    # 给 ip 一定准备时间
    time.sleep(10)
    # 开始异步检测每一个 ip 连通性
    tasks = []
    for newIp in newIps:
        tasks.append(test("http://" + str(newIp["ip"]) + ":" + str(newIp["port"])))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    param["end"] = time.time()
    delayTime = round(param["end"] - param["begin"])
    print("delayTime " + str(delayTime))
    # 结尾要检测 本次插入是否 维护了足够多的 ip进入 数据库
    if param["lens"] <= 0:
        testIpConnected(True)
    else:
        param["conn"].close()
        timer = threading.Timer(120.0 - 8, testIpConnected, [False])
        timer.start()
    # 也许还会有 批量insert的事，delete 不知咋弄
    pass


# 异步
async def test(proxy):
    async with aiohttp.ClientSession() as session:
        print(proxy + "   " + str(time.time()) + "   B")
        try:
            async with session.get(param["url"], headers=header,allow_redirects=False,proxy=proxy,timeout=3) as resp:
                status = resp.status
                if status == 200:
                    print(proxy + "   " + str(time.time()) + "  E")
                    # 插入
                    date = insert_Ip(proxy)
                    if not param["hasConnected"]:
                        print("new Date: " + date)
                        param["hasConnected"] = True
                        deleteAll(date)

        except Exception  as e:
            #超时 删掉-- 目前不用删因为 采用的是 重插入 策略，即 在第一条新的可用ip插入后 就将旧数据已经清空了
            print(proxy + "   " + str(time.time()) + "  超时   " )
            print(e)

def insert_Ip(proxy):
    item = {}
    item["ip"] = proxy[7:proxy.find(':',7)]
    item["port"] = proxy[proxy.find(':',7)+1:]
    item["id"] = str(uuid.uuid1())
    item["enterdate"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    try:
        inster_sql = 'insert into ippool (ip,port,id,enterdate) values(%s,%s,%s,%s)'
    except Exception as e:
        return None
    else:
        param["cur"].execute(inster_sql,
                         (item['ip'], item['port'], item['id'], item['enterdate']))
        # 等到最后一起提交?
        param["conn"].commit()
        param["lens"] = 1 + int(param["lens"])
        print("insert " + proxy)
        return item["enterdate"]
    # logger.info('pipline commit insert: %s' % inster_sql)


def deleteAll(date):
    try:
        delete_sql = "delete from ippool where enterdate::VARCHAR < '" + date +"'"
        param["cur"].execute(delete_sql)
        # 等到最后一起提交?
        param["conn"].commit()
        print("delete all!")
    except Exception  as e:
        print("delete Error")
        print(e)



# if __name__ == '__main__':
testIpConnected(False)
