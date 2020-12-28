# coding:utf-8
import os
import re
import sys
import requests
from lxml import etree
from optparse import OptionParser
reload(sys)
sys.setdefaultencoding('utf-8')
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
def option_parser():
    usage = "usage: %prog [options] arg1"

    parser = OptionParser(usage=usage)

    parser.add_option("--application_name", dest="application_name", action="store", type="string", help="")
    parser.add_option("--active_batches", dest="active_batches", action="store", type="string", help="")

    return parser

if __name__ == '__main__':

    reload(sys)
    sys.setdefaultencoding('utf-8')

    optParser = option_parser()
    options, args = optParser.parse_args(sys.argv[1:])

    if (options.application_name is None or options.active_batches is None):
        print "请指定完整参数--application_name --active_batches"
        exit(1)
    active_batche = 0
    record = ""
    schedul = ""
    resourcemanager_url = "resourcemanager_url" # 例：http://localhost:8088/cluster/scheduler
    resourcemanager_url_html = requests.get(resourcemanager_url).content.decode('utf-8')
    html = etree.HTML(resourcemanager_url_html)
    application_content = html.xpath('//*[@id="apps"]/script')
    for content in application_content:
        application_text_list = content.text.split("=", 1)[1].split("],")
        for application_text in application_text_list:
            application_text = application_text.replace("[", "").replace("]", "").split(",")
            application_name = application_text[2].replace("\"", "")
            application_id = re.findall(">(.*)<", str(application_text[0]))[0]
            if (application_name == options.application_name):
                streaming_url = "http://localhost:8088/proxy/%s/streaming/" % application_id
                streaming_html = requests.get(streaming_url).content.decode('utf-8')
                streaming_html = etree.HTML(streaming_html)
                streaming_content_list = streaming_html.xpath('//*[@id="active"]')
                # 清洗active_batche
                for content in streaming_content_list:
                    active_batches = content.text
                    active_batche = int(re.findall("\((.*)\)", active_batches)[0])
                streaming_records_list = streaming_html.xpath('//*[@id="active-batches-table"]/tbody/*/td[2]')
                # 清洗record
                for records in streaming_records_list:
                    record = records.text
                streaming_scheduling_delay_list = streaming_html.xpath('//*[@id="active-batches-table"]/tbody/*/td[3]')
                # 清洗Scheduling Delay
                for scheduling in streaming_scheduling_delay_list:
                    schedul = scheduling.text
                print active_batche
                if (active_batche > int(options.active_batches)):
                    content = "任务%s延迟了,积压批次:%d,Records:%s,Scheduling Delay:%s" % (application_name, active_batche, record, schedul)
                    print content
                    # TODO 加上公司内部IM接口通知地址，时刻关注，推荐用飞书


