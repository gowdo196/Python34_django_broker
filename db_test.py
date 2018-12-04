# -*- coding: utf8 -*-
import sys, os
import time
import datetime
import threading
import logging, logging.handlers
import psycopg2
import psycopg2.extras
#import configparser
import os
import fileinput
import socket

inipath = "C:\\Python34\\mysite\\broker\\Config.ini"
temppath = "C:\\Python34\\mysite\\broker\\Temp.ini"
logtxtpath = "C:\\Python34\\mysite\\broker\\log.txt"
# ================= DB operation function start =================
def DB_first_connect():
    lines = read_config()
    DB_IP_ = lines[1][9:]
    DB_Port_ = lines[2][11:]
    DB_Name_ = lines[3][11:]
    DB_Pwd_ = lines[4][10:]
    DB_DB_ = lines[5][9:]
    DB_str_ = "host="+DB_IP_+" port="+DB_Port_+ " user="+DB_Name_ + " dbname="+DB_DB_ + " password="+DB_Pwd_
    return DB_str_
def HK_DB_first_connect():
    lines = read_config()
    DB_IP_ = lines[6][12:]
    DB_Port_ = lines[7][14:]
    DB_Name_ = lines[8][14:]
    DB_Pwd_ = lines[9][13:]
    DB_DB_ = lines[10][12:]
    DB_str_ = "host="+DB_IP_+" port="+DB_Port_+ " user="+DB_Name_ + " dbname="+DB_DB_ + " password="+DB_Pwd_
    return DB_str_
def DB_select_connect(sqlcmd):#return list of tuple
    DB_str_ = DB_first_connect()
    try:
        DB_conn_ = psycopg2.connect(DB_str_)
    except:
        #print "Can't connect to Working DB"
        sys.exit(0)
        
    DB_conn_.autocommit = True  
    DB_conn_.set_client_encoding('BIG5')
    DB_cursor_ = DB_conn_.cursor()
    DB_cursor_.execute("insert into public.tb_broker_log(log_level,msg,error_msg) VALUES (\'DEBUG\',\'DB_select_connect\',\'"+sqlcmd.replace('\'','')+"\')")
    DB_cursor_.execute(sqlcmd)

    results = DB_cursor_.fetchall()
    DB_conn_.close()
    return results
def DB_update_connect(sqlcmd):#no return
    DB_str_ = DB_first_connect()
    try:
        DB_conn_ = psycopg2.connect(DB_str_)
    except:
        #print "Can't connect to Working DB"
        sys.exit(0)
        
    DB_conn_.autocommit = True  
    DB_conn_.set_client_encoding('BIG5')
    DB_cursor_ = DB_conn_.cursor()
    DB_cursor_.execute("insert into dfh.tb_broker_log(log_level,msg,error_msg) VALUES (\'DEBUG\',\'DB_update_connect\',\'"+sqlcmd.replace("'","")+"\')")
    DB_cursor_.execute(sqlcmd)
    DB_conn_.close()
def HK_DB_select_connect(sqlcmd):#return list of tuple
    HK_DB_str = HK_DB_first_connect()
    try:
        DB_conn_ = psycopg2.connect(HK_DB_str)
    except:
        #print "Can't connect to Working DB"
        sys.exit(0)
        
    DB_conn_.autocommit = True  
    DB_conn_.set_client_encoding('UTF8')
    DB_cursor_ = DB_conn_.cursor()
    #DB_cursor_.execute("insert into public.tb_broker_log(log_level,msg,error_msg) VALUES (\'DEBUG\',\'DB_select_connect\',\'"+sqlcmd.replace('\'','')+"\')")
    DB_cursor_.execute(sqlcmd)

    results = DB_cursor_.fetchall()
    DB_conn_.close()
    return results
# ================= DB operation function end =================
# ================= File operation function start =================
def read_config():#open Config.ini
    f = open(inipath,"r",encoding = 'utf8')
    og_lines = f.readlines()
    lines = [i.split('\n',1)[0] for i in og_lines]
    #print(lines)
    f.close
    return lines
def read_temp():#open Temp.ini
    f = open(temppath,"r",encoding = 'utf8')
    og_lines = f.readlines()
    lines = [i.split('\n',1)[0] for i in og_lines]
    #print(lines)
    f.close
    return lines
def read_information_from_temp(int_):# int_ -> 0: tb_vip_status, 1: tb_vip_relation, 2: tb_vip_certification
    lines = read_temp()

    tb_vip_status_list = []
    tb_vip_relation_list = []
    tb_vip_certification_list = []

    tb_vip_status_Start = lines.index("[tb_vip_status]")
    tb_vip_relation_Start = lines.index("[tb_vip_relation]")
    tb_vip_certification_Start = lines.index("[tb_vip_certification]")

    for r in range(tb_vip_status_Start+1,tb_vip_relation_Start-1):
        tb_vip_status_list.append(tuple(lines[r].split('_:_')))
    for r in range(tb_vip_relation_Start+1,tb_vip_certification_Start-1):
        tb_vip_relation_list.append(tuple(lines[r].split('_:_')))
    for r in range(tb_vip_certification_Start+1,len(lines)):
        tb_vip_certification_list.append(tuple(lines[r].split('_:_')))
    if int_ == 0:
        return tb_vip_status_list
    if int_ == 1:
        return tb_vip_relation_list
    if int_ == 2:
        return tb_vip_certification_list

def read_config_list(int_):# int_ -> 0 : DBconfigList, 1 : OrderServerList, 2 : VIPServerList, 3 : VersionList, 4 : AreaList, 5 : MaxLinkList, 6 : OverseasMarketList, 7 : Error_msg_List, 8 : Active_Mode
    lines = read_config()

    DBconfigList = []#資料庫設定
    OrderServerList = []#下單server ip位置
    VIPServerList = []#VIP
    VersionList = []#版本
    AreaList = []#地區
    MaxLinkList = []#連線數 min : max : 區間值
    OverseasMarketList = []#海外市場
    Error_msg_List = []#錯誤訊息中文對照
    Active_Mode_List = []#20160504 新增切換模式設定,方便測試N版,正式版之間的轉換 

    DBconfigStart = lines.index("[DBconfig]")
    OrderServerStart = lines.index("[OrderServer]")
    ServerStart = lines.index("[VIPServerList]")
    VersionStart = lines.index("[1Version]")
    AreaStart = lines.index("[2Area]")
    MaxLinkStart = lines.index("[MaxLinkList]")
    OverseasMarketStart = lines.index("[OverseasMarketList]")
    Error_msg_Start = lines.index("[Error_msg_List]")
    Active_Mode_Start = lines.index("[Active_Mode]")

    for r in range(DBconfigStart+1,OrderServerStart-1):
        DBconfigList.append(lines[r])
    for r in range(OrderServerStart+1,ServerStart-1):
        OrderServerList.append(lines[r])
    for r in range(ServerStart+1,VersionStart-1):
        VIPServerList.append(lines[r])
    for r in range(VersionStart+1,AreaStart-1):
        VersionList.append(lines[r])
    for r in range(AreaStart+1,MaxLinkStart-1):
        AreaList.append(lines[r])
    for r in range(MaxLinkStart+1,OverseasMarketStart-1):
        MaxLinkList.append(lines[r])
    for r in range(OverseasMarketStart+1,Error_msg_Start-1):
        OverseasMarketList.append(lines[r])
    for r in range(Error_msg_Start+1,Active_Mode_Start-1):
        Error_msg_List.append(lines[r])
    for r in range(Active_Mode_Start+1,len(lines)):
        Active_Mode_List.append(lines[r])

    #print(line.strip())
    if int_ == 0:
        return DBconfigList
    if int_ == 1:
        return OrderServerList
    if int_ == 2:
        return VIPServerList
    if int_ == 3:
        return VersionList
    if int_ == 4:
        return AreaList
    if int_ == 5:
        return MaxLinkList
    if int_ == 6:
        return OverseasMarketList
    if int_ == 7:
        return Error_msg_List
    if int_ == 8:
        return Active_Mode_List

def key_exist_in_object(key_,object):
    if key_ in object:
        return object[key_]
    else:
        return ''
def list_re_append(list_,size_):
    result = []
    for r in list_:
        result.append(r[0:size_])
    return result
def write_temp(object,int_,mode_):# int_ -> 0 : Overwrite status (curr_link,Thread), 1 : Overwrite relation, 2 : Overwrite certification
    tb_vip_status_list = read_information_from_temp(0)
    tb_vip_relation_list = read_information_from_temp(1)
    tb_vip_certification_list = read_information_from_temp(2)

    tb_vip_status_list_result = []
    tb_vip_relation_list_result = []
    tb_vip_certification_list_result = []
    
    f = open(temppath,"w",encoding = 'utf8')
    # ==== for login logout oper ====
    login_vip_ip = key_exist_in_object('login_vip_ip',object)
    login_account = key_exist_in_object('login_account',object)
    login_pwd = key_exist_in_object('login_pwd',object)
    curr_link = key_exist_in_object('curr_link',object)
    login_cert = key_exist_in_object('login_cert',object)

    # ==== for web oper ====
    txb_sn = key_exist_in_object('txb_sn',object)
    txb_ip = key_exist_in_object('txb_ip',object)
    txb_ver = key_exist_in_object('txb_ver',object)
    txb_area = key_exist_in_object('txb_area',object)
    txb_max_link = key_exist_in_object('txb_max_link',object)
    txb_pro = key_exist_in_object('txb_pro',object)
    txb_act = key_exist_in_object('txb_act',object)

    og_txb_ip = key_exist_in_object('og_txb_ip',object)
    og_txb_ver = key_exist_in_object('og_txb_ver',object)
    og_txb_area = key_exist_in_object('og_txb_area',object)
    if mode_ == 'add':
        sn = 0
        # must use append because _:_ will be grow up _:__:__:_
        tb_vip_status_list_result = list_re_append(tb_vip_status_list,7)
        tb_vip_relation_list_result = list_re_append(tb_vip_relation_list,8)
        tb_vip_certification_list_result = list_re_append(tb_vip_certification_list,6)
        for r in tb_vip_relation_list:
            sn = int(r[0])#find last sn
        if txb_ip != "":
            update_row = []
            update_row.append(txb_ip)
            update_row.append('0')#curr_link
            update_row.append('100')#max_link
            update_row.append('D')#pro
            update_row.append('F')#act
            update_row.append('0')#Thread
            update_row.append(str(datetime.datetime.now()))
            tb_vip_status_list_result.append(update_row)
            update_row = []
            update_row.append(sn + 1)
            update_row.append(txb_ip)
            update_row.append('')#version
            update_row.append('')#area
            update_row.append('100')#max_link
            update_row.append('D')#pro
            update_row.append('F')#act
            update_row.append(str(datetime.datetime.now()))
            tb_vip_relation_list_result.append(update_row)
    elif mode_ == 'set':
        if int_ == 0:
            for r in tb_vip_status_list:
                update_row = []
                if r[0] == og_txb_ip:
                    update_row.append(txb_ip)
                else:
                    update_row.append(r[0])
                update_row.append(r[1])#curr_link
                update_row.append(r[2])#max_link
                update_row.append(r[3])#pro
                update_row.append(r[4])#act
                update_row.append(r[5])#Thread
                update_row.append(str(datetime.datetime.now()))
                # must use append because _:_ will be grow up _:__:__:_
                tb_vip_status_list_result.append(update_row)
            tb_vip_relation_list_result = list_re_append(tb_vip_relation_list,8)
            tb_vip_certification_list_result = list_re_append(tb_vip_certification_list,6)
        if int_ == 1:
            for r in tb_vip_relation_list:
                update_row = []
                update_row.append(r[0])
                if r[1] == og_txb_ip:
                    update_row.append(txb_ip)
                else:
                    update_row.append(r[1])
                if r[2] == og_txb_ver:
                    update_row.append(txb_ver)
                else:
                    update_row.append(r[2])
                if r[3] == og_txb_area:
                    update_row.append(txb_area)
                else:
                    update_row.append(r[3])
                update_row.append(r[4])#pro
                update_row.append(r[5])#act
                update_row.append(r[6])#Thread
                update_row.append(str(datetime.datetime.now()))
                tb_vip_relation_list_result.append(update_row)
            tb_vip_status_list_result = list_re_append(tb_vip_status_list,7)
            tb_vip_certification_list_result = list_re_append(tb_vip_certification_list,6)
        if int_ == 2:
            for r in tb_vip_certification_list:
                update_row = []
                update_row.append(r[0])#ip
                update_row.append(r[1])#account
                update_row.append(r[2])#pwd
                update_row.append(r[3])#cert
                if r[3] == login_cert:
                    update_row.append('0')
                else:
                    update_row.append(r[4])#certcheck
                update_row.append(str(datetime.datetime.now()))
                tb_vip_certification_list_result.append(update_row)
            tb_vip_status_list_result = list_re_append(tb_vip_status_list,7)
            tb_vip_relation_list_result = list_re_append(tb_vip_relation_list,8)
    elif mode_ == 'del':
        for r in tb_vip_status_list:
            update_row = []
            if r[0] != og_txb_ip:
                update_row.append(r[0])#ip
                update_row.append(r[1])#curr_link
                update_row.append(r[2])#max_link
                update_row.append(r[3])#pro
                update_row.append(r[4])#act
                update_row.append(r[5])#Thread
                update_row.append(str(datetime.datetime.now()))
                # must use append because _:_ will be grow up _:__:__:_
                tb_vip_status_list_result.append(update_row)
        for r in tb_vip_relation_list:
            if r[1] != og_txb_ip:
                update_row = []
                update_row.append(r[0])#sn
                update_row.append(r[1])#ip
                if r[2] == og_txb_ver:
                    update_row.append('')
                else:
                    update_row.append(r[2])#ver
                if r[3] == og_txb_area:
                    update_row.append('')
                else:
                    update_row.append(r[3])#area
                update_row.append(r[4])#pro
                update_row.append(r[5])#act
                update_row.append(r[6])#Thread
                update_row.append(str(datetime.datetime.now()))
                tb_vip_relation_list_result.append(update_row)
        tb_vip_certification_list_result = list_re_append(tb_vip_certification_list,6)
    elif mode_ == 'update_relation':
        if int_ == 0:
            for r in tb_vip_status_list:
                update_row = []
                if r[0] == txb_ip:
                    update_row.append(txb_ip)
                    if len(str(curr_link))>0 and int(curr_link) > 0:
                        update_row.append(curr_link)
                    else:
                        update_row.append(r[1])
                    if len(txb_max_link) > 0:
                        update_row.append(txb_max_link)
                    else:
                        update_row.append(r[2])
                    if len(txb_pro) > 0:
                        update_row.append(txb_pro)
                    else:
                        update_row.append(r[3])
                    if len(txb_act) > 0:
                        update_row.append(txb_act)
                    else:
                        update_row.append(r[4])
                    update_row.append(r[5])#Thread
                    update_row.append(str(datetime.datetime.now()))
                else:
                    # must use append because _:_ will be grow up _:__:__:_
                    update_row.append(r[0])
                    update_row.append(r[1])
                    update_row.append(r[2])
                    update_row.append(r[3])
                    update_row.append(r[4])
                    update_row.append(r[5])
                    update_row.append(r[6])
                tb_vip_status_list_result.append(update_row)
            tb_vip_relation_list_result = list_re_append(tb_vip_relation_list,8)
        if int_ == 1:
            for r in tb_vip_relation_list:
                update_row = []
                if r[0] == str(txb_sn):
                    update_row.append(txb_sn)
                    update_row.append(txb_ip)
                    if len(txb_ver) > 0:
                        update_row.append(txb_ver)
                    else:
                        update_row.append(r[2])
                    if len(txb_area) > 0:
                        update_row.append(txb_area)
                    else:
                        update_row.append(r[3])
                    if len(txb_max_link) > 0:
                        update_row.append(txb_max_link)
                    else:
                        update_row.append(r[4])
                    if len(txb_pro):
                        update_row.append(txb_pro)
                    else:
                        update_row.append(r[5])
                    if len(txb_act):
                        update_row.append(txb_act)
                    else:
                        update_row.append(r[6])
                    update_row.append(str(datetime.datetime.now()))
                else:
                    # must use append because _:_ will be grow up _:__:__:__:_
                    update_row.append(r[0])
                    update_row.append(r[1])
                    update_row.append(r[2])
                    update_row.append(r[3])
                    update_row.append(r[4])
                    update_row.append(r[5])
                    update_row.append(r[6])
                    update_row.append(r[7])
                tb_vip_relation_list_result.append(update_row)
            tb_vip_status_list_result = list_re_append(tb_vip_status_list,7)
        tb_vip_certification_list_result = list_re_append(tb_vip_certification_list,6)
    elif mode_ == 'add_cert':
        tb_vip_status_list_result = list_re_append(tb_vip_status_list,7)
        tb_vip_relation_list_result = list_re_append(tb_vip_relation_list,8)
        tb_vip_certification_list_result = list_re_append(tb_vip_certification_list,6)
        if login_vip_ip != "" and login_account != "" and login_cert != "":
            update_row = []
            update_row.append(login_vip_ip)
            update_row.append(login_account)
            update_row.append(login_pwd)
            update_row.append(login_cert)
            update_row.append('2')
            update_row.append(str(datetime.datetime.now()))
            tb_vip_certification_list_result.append(update_row)
    elif mode_ == 'del_cert':#20151224
        for r in tb_vip_certification_list:
            update_row = []
            if r[3] != login_cert:
                update_row.append(r[0])#login_vip_ip
                update_row.append(r[1])#login_account
                update_row.append(r[2])#login_pwd
                update_row.append(r[3])#login_cert
                update_row.append(r[4])#check_cert
                update_row.append(str(datetime.datetime.now()))
                # must use append because _:_ will be grow up _:__:__:_
                tb_vip_certification_list_result.append(update_row)
        tb_vip_status_list_result = list_re_append(tb_vip_status_list,7)
        tb_vip_relation_list_result = list_re_append(tb_vip_relation_list,8)
    elif mode_ == 'check_cert':#20160108
        for r in tb_vip_certification_list:
            update_row = []
            update_row.append(r[0])#login_vip_ip
            update_row.append(r[1])#login_account
            update_row.append(r[2])#login_pwd
            update_row.append(r[3])#login_cert
            if r[3] == login_cert:
                temp = int(r[4])+1
                update_row.append(str(temp))#check_cert
            else:
                update_row.append(r[4])#check_cert
            update_row.append(str(datetime.datetime.now()))
            tb_vip_certification_list_result.append(update_row)
        tb_vip_status_list_result = list_re_append(tb_vip_status_list,7)
        tb_vip_relation_list_result = list_re_append(tb_vip_relation_list,8)
    #====================test code====================
    """
    sqlcmd = 'select sn,ip,version,area,max_link,pro,act,last_update from dfh.tb_vip_relation ORDER BY sn;'
    tb_vip_relation_list_result = DB_select_connect(sqlcmd)
    sqlcmd = 'select ip,curr_link,max_link,pro,act,thread,last_update from dfh.tb_vip_status;'
    tb_vip_status_list_result = DB_select_connect(sqlcmd)
    """
    f.write("[tb_vip_status]\n")
    for r in tb_vip_status_list_result:
        for rr in r:
            f.write(str(rr)+"_:_")
        f.write("\n")
    f.write("\n")
    f.write("[tb_vip_relation]\n")
    for r in tb_vip_relation_list_result:
        for rr in r:
            f.write(str(rr)+"_:_")
        f.write("\n")
    f.write("\n")
    f.write("[tb_vip_certification]\n")
    for r in tb_vip_certification_list_result:
        for rr in r:
            f.write(str(rr)+"_:_")
        f.write("\n")
    f.close()

def write_config(object):
    DBconfigList = read_config_list(0)
    OrderServerList = read_config_list(1)
    VIPServerList = read_config_list(2)
    VersionList = read_config_list(3)
    AreaList = read_config_list(4)
    MaxLinkList = read_config_list(5)
    OverseasMarketList = read_config_list(6)
    Error_msg_List = read_config_list(7)
    Active_Mode_List = read_config_list(8)

    f = open(inipath,"w",encoding = 'utf8')

    tf_ip = 'txb_ip' in object
    tf_Version = 'txb_ver' in object
    tf_Area = 'txb_area' in object
    tf_og_ip = 'og_txb_ip' in object
    tf_og_Version = 'og_txb_ver' in object
    tf_og_Area = 'og_txb_area' in object
    # I don't know why fail, fuck
    if tf_ip:
        VIPServerList.append(object['txb_ip'])
    if tf_Version:    
        VersionList.append(object['txb_ver'])
    if tf_Area:
        AreaList.append(object['txb_area'])
    if tf_og_ip:
        VIPServerList.remove(object['og_txb_ip'])
    if tf_og_Version:
        VersionList.remove(object['og_txb_ver'])
    if tf_og_Area:
        AreaList.remove(object['og_txb_area'])
    f.write("[DBconfig]\n")
    for r in DBconfigList:
        f.write(r+"\n")

    f.write("\n")
    f.write("[OrderServer]\n")
    for r in OrderServerList:
        f.write(r+"\n")

    f.write("\n")
    f.write("[VIPServerList]\n")
    for r in VIPServerList:
        f.write(r+"\n")

    f.write("\n")
    f.write("[1Version]\n")
    for r in VersionList:
        f.write(r+"\n")

    f.write("\n")
    f.write("[2Area]\n")
    for r in AreaList:
        f.write(r+"\n")

    f.write("\n")
    f.write("[MaxLinkList]\n")
    for r in MaxLinkList:
        f.write(r+"\n")

    f.write("\n")
    f.write("[OverseasMarketList]\n")
    for r in OverseasMarketList:
        f.write(r+"\n")

    f.write("\n")
    f.write("[Error_msg_List]\n")
    for r in Error_msg_List:
        f.write(r+"\n")

    f.write("\n")
    f.write("[Active_Mode]\n")
    for r in Active_Mode_List:
        f.write(r+"\n")

    f.close()
def write_log_txt(object):
    #logList = read_log_txt()
    f = open(logtxtpath,"a")

    f.write(object['log_level']+","+object['function_name']+","+object['debug_string']+"\n")
    f.close()
# ================= File operation function end =================
# ================= socket operation start =================
def call_for_ddsc_gateway():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('203.69.245.142',9112))
        #sock.connect(('192.168.12.206',8080))
    except:
        print("ERROR")
    length = 71
    seq = 1
    """
    futures_list = [{'id':'A123456789','futures':'7777774'},
    {'id':'A123456798','futures':'5555558'},
    {'id':'M123456789','futures':'4444440'}]
    security_list = [{'id':'A123456789','security':'8888884'},
    {'id':'A123456798','security':'7777776'},
    {'id':'M123456789','security':'8889993'}]
    """
    cust_name_list = []
    for r in futures_list:
        # for big Endian
        length1 = int(length / 256)
        length2 = length % 256

        now_time_str = str(datetime.datetime.now())[11:13]+str(datetime.datetime.now())[14:16]+str(datetime.datetime.now())[17:19]+str(datetime.datetime.now())[20:23]
    
        head_start = chr(0x11)
        head = now_time_str+str(seq).rjust(10, '0')+'000'+'APE'+'       '+'               '
        seq = seq + 1
        body_length1 = chr(int(hex(length1),16))
        body_length2 = chr(int(hex(length2),16))
        body = str(r['id'])+'       '+'       '+'0'+'A'+'12345678            '+'192.168.210.75           '
        end = chr(0x0a)

        sock.send((head_start+head+body_length1+body_length2+body+end).encode('ascii'))
        #sock.send((head_start+hex_ord(head)+length_1+length_2+hex_ord(body)+end).replace('0x','').encode('ascii'))

        data = sock.recv(1024)

    return data#byte string
# ================= socket operation end =================
def RyanOP_UTF_8(s):
    s = s.encode('utf8')
    u = s.decode('utf8')
    return u

"""
def config_test():
    #fail in python 3.5
    #configparser can't work...20151124
    config = configparser.ConfigParser()
    config.read('Config.ini')
    section_a_Value = config.get('Section_A', 'Key_ABC')#GET "Value_ABC"
    section_b_Value = config.get('Section_B', 'Alarm') #Get "Some thing here"
    
def DB_first_connect():
    DB_IP_ = "192.168.12.21"
    DB_Port_ = "5432"
    DB_Name_ = "postgres"
    DB_Pwd_ = "0000"
    DB_DB_ = "dcn_online"
    
    config = configparser.ConfigParser()
    config.read("C:\Python35\mysite\broker\Config.ini")
    """