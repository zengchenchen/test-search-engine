# -*- coding: utf-8 -*-
from werobot import WeRoBot
from SE import Search
import sys

token = '518888888'
encoding_aes_key = '24HXKErdtkTRKZF9ehWI9Ls12NbiaDci2shk9oMKERK'
app_id = 'wx7aa7413ec6be90e7'
robot = WeRoBot(token=token, encoding_aes_key=encoding_aes_key, app_id=app_id)

name = sys.argv[1] if len(sys.argv) > 1 else 'ms'
wiki_SE = Search('/home/ubuntu/db_files/%s_wiki.db' % name)


@robot.subscribe
def hello(message):
    return 'Welcome :) new friend! Here is a simple search engine build all by zengChenChen. You can tell me directly what you wanna search!'


@robot.text
def echo_text(message):
    if message.content == '【收到不支持的消息类型，暂无法显示】':
        return 'Could U just type English text to me? :)'
    res = wiki_SE.query(message.content)
    return res


# @robot.handler
# def echo(message):
#     return 'Could U speak English? :)'

# robot.config['HOST'] = '0.0.0.0'
robot.config['PORT'] = 6666

robot.run(server='gunicorn')
