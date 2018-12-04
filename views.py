# -*- coding: utf8 -*-
from django.shortcuts import render
from django.http import HttpResponse
from django.http import JsonResponse
from django.shortcuts import render_to_response
import datetime
from broker import db_test
from broker import db_exe
# Create your views here.
logpath = "C:\\Python34\\mysite\\DGW\\"
logtxtpath = logpath+"dgw_log.txt"
def write_log_txt(object):
    #logList = read_log_txt()
    f = open(logtxtpath,"a")

    f.write("\n["+str(datetime.datetime.now())+"]"+str(object))
    f.close()
def broker_index(request):
    #Client to broker ...start
    #broker now all use http post
    #?Id=demo0898&pwd=8387&version= Ver7.82&area=Taipei&ip=10.0.7.82
    if 'type' in request.GET:
        type = request.GET['type'].strip()
        #db_test.dosomething()
        #response = HttpResponse("Here's the text of the Web page... and id=" + id)
        if type == "login":
            id = request.GET['id'].strip()
            pwd = request.GET['pwd'].strip()
            version = request.GET['version'].strip()
            area = request.GET['area'].strip()
            ip = request.GET['ip'].strip()
            dic_result = db_exe.login(id,pwd,version,area,ip)
            OverseasMarketList = db_test.read_config_list(6)

            if db_test.key_exist_in_object('errorcode',dic_result):
                return JsonResponse({
                  "success":False,
                  "errorcode":dic_result['errorcode'],
                  "error_msg":dic_result['error_msg']
                })
            else:
                RightList = [{"Market":"TW",
                            "View" :True,
                            "Stock":dic_result['TW_stock_account_list'],
                            "Future":dic_result['TW_futures_account_list'],
                            "VIPIP":dic_result['login_vip_ip'],
                            "VIPPort":dic_result['login_vip_port']
                        }]
                if dic_result['account_competence'] in ['3','4',3,4]:
                    for item in OverseasMarketList:
                        RightList.append({"Market":item,
                                "View":True,
                                "Stock":dic_result['stock_account_list'],
                                "Future":dic_result['futures_account_list'],
                                "VIPIP":dic_result['HK_login_vip_ip'],
                                "VIPPort":dic_result['HK_login_vip_port']
                            })
                return JsonResponse({
                  "success":True,
                  "errorcode":"",
                  "error_msg":"",
                  "cert":dic_result['login_cert'],
                  "version":"1.0",
                  "order_server_ip":dic_result['order_server_ip'],
                  "expire_date":dic_result['expire_date'],
                  "Privilege" : "0xffef",
                  "Right":RightList
                })
        elif type == "slogin":
            id = 'slogin'
            pwd = '0000'
            version = request.GET['version'].strip()
            area = request.GET['area'].strip()
            ip = request.GET['ip'].strip()
            dic_result = db_exe.login(id,pwd,version,area,ip)
            RightList = [{"Market":"TW",
                            "View" :True,
                            "Stock":"",
                            "Future":"",
                            "VIPIP":dic_result['login_vip_ip'],
                            "VIPPort":dic_result['login_vip_port']
                        }]
            return JsonResponse({
                  "success":True,
                  "errorcode":"",
                  "error_msg":"",
                  "cert":dic_result['login_cert'],
                  "version":"1.0",
                  "expire_date":dic_result['expire_date'],
                  "Privilege" : "0xffef",
                  "Right":RightList
            })
        elif type == "logout":
            cert = request.GET['cert'].strip()
            db_exe.logout(cert)
            RightList=[]
            return JsonResponse({
                  "success":True,
                  "errorcode":"",
                  "error_msg":"",
                  "cert":"",
                  "version":"1.0",
                  "Right":RightList
                  #,
                  #"string(unknown)": "1fef",
                  #"version": "Ver1.2",
                  #"address": "http://eod.apex.com.tw/get.php:80"
                })
        elif type == "cert":
            cert = request.GET['cert'].strip()
            if db_exe.query_cert(cert) == "":
                return JsonResponse({
                    "success":False,
                    "errorcode":"C0001",
                    "error_msg":"資料庫無此認證碼",
                    "cert":""+cert
                })
            else:
                db_exe.check_cert(cert)
                return JsonResponse({
                    "success":True,
                    "errorcode":"",
                    "error_msg":"",
                    "cert":""+db_exe.query_cert(cert)
                })
    #response = JsonResponse(data, encoder=MyJSONEncoder)
    #Client to broker ...end
    else:
        results = db_exe.broker_index_results()
        currlink_result = db_exe.broker_index_currlink_result()
        vip_list = []
        for row in results:
            ip = row[1]
            version = row[2]
            area = row[3]
            for r in currlink_result:
                if row[1] == r[0]:
                    current_link = r[1]
            max_link = row[4]
            pro = row[5]
            act = row[6]
            last_update = row[7]
            vip_list.append({ 'ip':ip, 'version':version, 'area':area, 'current_link':current_link, 'max_link':max_link, 'pro':pro, 'act':act, 'last_update':last_update})
        path = request.path
        host = request.get_host()
        return render_to_response('broker_index.html',locals())
def broker_list(request):#/broker_list/ for broker_mod_list
    dic = {}
    write_file_TF = False
    if 'broker_list_update' in request.POST:
        txb_sn = request.POST['sn'].strip()
        txb_ip = request.POST['ip'].strip()
        txb_ver = request.POST['ver'].strip()
        txb_area = request.POST['area'].strip()
        txb_max_link = request.POST['max_link'].strip()
        txb_pro = request.POST['pro'].strip()
        txb_act = request.POST['act'].strip()
        dic.update({'txb_sn':txb_sn})
        dic.update({'txb_ip':txb_ip})
        if len(txb_ver)>0:
            write_file_TF = True
            dic.update({'txb_ver':txb_ver})
        if len(txb_area)>0:
            write_file_TF = True
            dic.update({'txb_area':txb_area})
        if txb_max_link != "--":
            write_file_TF = True
            dic.update({'txb_max_link':txb_max_link})
        if txb_pro != "--":
            write_file_TF = True
            dic.update({'txb_pro':txb_pro})
        if txb_act != "--":
            write_file_TF = True
            dic.update({'txb_act':txb_act})
    results = db_exe.mod_list(dic,write_file_TF)
    vip_list = []
    for row in results:
        sn = row[0]
        ip = row[1]
        version = row[2]
        area = row[3]
        max_link = row[4]
        pro = row[5]
        act = row[6]
        last_update = row[7]
        vip_list.append({ 'sn':sn, 'ip':ip, 'version':version, 'area':area, 'max_link':max_link, 'pro':pro, 'act':act, 'last_update':last_update})
    VIPServerList = db_test.read_config_list(2)
    VersionList = db_test.read_config_list(3)
    AreaList = db_test.read_config_list(4)
    max_link = db_test.read_config_list(5)[0].split(';')
    max_link_list = []
    for number in range(int(max_link[0]), int(max_link[1])+int(max_link[2]), int(max_link[2])):
        max_link_list.append(str(number))
    pro_list = ['S','D']
    act_list = ['T','F']
    path = request.path
    host = request.get_host()
    return render_to_response('broker_mod_list.html',locals())

def broker_set_config(request):#/broker_set/ for broker_set_config
    dic = {}
    write_file_TF = False
    if 'ok' in request.POST:
        txb_ip = request.POST['ip'].strip()
        txb_ver = request.POST['ver'].strip()
        txb_area = request.POST['area'].strip()
        og_txb_ip = request.POST['og_ip'].strip()
        og_txb_ver = request.POST['og_ver'].strip()
        og_txb_area = request.POST['og_area'].strip()
        if len(og_txb_ip)>0:
            write_file_TF = True
            dic.update({'txb_ip':txb_ip})
            dic.update({'og_txb_ip':og_txb_ip})
        if len(og_txb_ver)>0:
            write_file_TF = True
            dic.update({'txb_ver':txb_ver})
            dic.update({'og_txb_ver':og_txb_ver})
        if len(og_txb_area)>0:
            write_file_TF = True
            dic.update({'txb_area':txb_area})
            dic.update({'og_txb_area':og_txb_area})
    db_exe.set_config(dic,write_file_TF)
    results = db_exe.query_sn_ip_from_relation()
    VIPServerList = []
    for row in results:
        ip = row[1]
        VIPServerList.append({'ip':ip})
    VersionList = db_test.read_config_list(3)
    AreaList = db_test.read_config_list(4)
    path = request.path
    host = request.get_host()
    return render_to_response('broker_set_config.html',locals())

def add_vip(request):#/broker_set_add/
    dic = {}
    write_file_TF = False
    if 'add' in request.POST:
        txb_ip = request.POST['ip'].strip()
        txb_ver = request.POST['ver'].strip()
        txb_area = request.POST['area'].strip()
        if len(txb_ip)>0:
            write_file_TF = True
            dic.update({'txb_ip':txb_ip})
        if len(txb_ver)>0:
            write_file_TF = True
            dic.update({'txb_ver':txb_ver})
        if len(txb_area)>0:
            write_file_TF = True
            dic.update({'txb_area':txb_area})
        db_exe.add_vip(dic,write_file_TF)
    elif 'del' in request.POST:
        og_txb_ip = request.POST['og_ip'].strip()
        og_txb_ver = request.POST['og_ver'].strip()
        og_txb_area = request.POST['og_area'].strip()
        if len(og_txb_ip)>0:
            write_file_TF = True
            dic.update({'og_txb_ip':og_txb_ip})
        if len(og_txb_ver)>0:
            write_file_TF = True
            dic.update({'og_txb_ver':og_txb_ver})
        if len(og_txb_area)>0:
            write_file_TF = True
            dic.update({'og_txb_area':og_txb_area})
        db_exe.del_vip(dic,write_file_TF)
    results = db_exe.query_sn_ip_from_relation()
    VIPServerList = []
    for row in results:
        ip = row[1]
        VIPServerList.append({'ip':ip})
    VersionList = db_test.read_config_list(3)
    AreaList = db_test.read_config_list(4)
    path = request.path
    host = request.get_host()
    return render_to_response('broker_set_config.html',locals())
def main(request):
    og_str = request.get_full_path()
    write_log_txt('] og_str1='+og_str)
    return render_to_response('html/main.html',locals())
def menu_1(request):
    og_str = request.get_full_path()    #'/main/html/menu_1/sub_1.html'
    """
    f = open("C:\\Python34\\mysite\\templates\\"+og_str.replace("/main/",''),"r",encoding="utf8")
    og_lines = f.readlines()
    f.close()
    """
    write_log_txt(' og_str2='+og_str)
    #return render_to_response(og_lines,locals())
    return render_to_response(og_str.replace("/main/",''),locals())